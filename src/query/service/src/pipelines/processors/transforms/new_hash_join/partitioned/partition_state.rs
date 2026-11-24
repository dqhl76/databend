// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use concurrent_queue::ConcurrentQueue;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use parquet::file::metadata::RowGroupMetaData;

use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinFactory;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::hash_join_table::NUM_PARTITIONS;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

pub const BYTES_PER_MIB: usize = 1024 * 1024;

#[derive(Debug)]
pub struct SpillMetadata {
    pub path: String,
    pub row_groups: Vec<RowGroupMetaData>,
}

/// Build-stage collection mode of one hash join recursion level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildStageMode {
    /// Optimistic default: `Stage::Build` squashes blocks directly into `basic.chunks` without
    /// evaluating keys, hashing, or scattering (identical to an unpartitioned in-memory join).
    Unpartitioned,
    /// Activated on `check_spill()` during `Stage::Build`, or when `Stage::BuildFinal` switches
    /// to partitioned execution. Build rows are radix-partitioned into `NUM_PARTITIONS` slots.
    Partitioned,
}

/// State machine for `Stage::BuildFinal`.
///
/// ```text
/// Deciding ──(Unpartitioned & Single buckets fit)──▶ Single ──(done, no spill)──▶ Stage::Probe
///    │                                                 │
///    │                                          (check_spill)
///    │                                                 ▼
///    │                                          AbortingSingle ──(barrier: drop table, queue chunks)──┐
///    │                                                                                                 ▼
///    ├──(Unpartitioned & Single buckets don't fit)──────────────────────────────────────────────▶ Scattering
///    │                                                                                                 │
///    │                                                                                            (barrier)
///    │                                                                                                 ▼
///    └──(Stage::Build already Partitioned)──────────────────────────────────────────────────▶ PartitionedBuild
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildFinalMode {
    Deciding,
    Single,
    AbortingSingle,
    Scattering,
    PartitionedBuild,
}

/// Lifecycle of a single partition (16-way radix partition or `GlobalShuffle` lane):
///
/// ```text
/// Collecting ──▶ (Building) ──▶ InMemory  (built into hash table and probed in memory)
///     │               │
///     └───────────────┴───────▶ Spilled   (untouched build chunks written to Parquet, restored in ProbeFinal)
/// ```
///
/// Only `Collecting` partitions (and `Building` pre-partitioned lanes whose `preserve_build_chunks`
/// flag kept intact `build_schema` chunks) ever spill. Once `InMemory`, a partition stays in memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStatus {
    Collecting,
    /// Pre-partitioned `GlobalShuffle` mode only: one thread exclusively owns this lane's
    /// `Buckets::Single` build and may abort directly to `Spilled` on `check_spill()`.
    Building,
    InMemory,
    Spilled,
}

pub struct PartitionSlot {
    pub status: PartitionStatus,
    pub build_rows: usize,
    pub build_bytes: usize,
    /// Indexes of this partition's chunks in the level's shared `BasicHashJoinState`.
    pub chunks: Vec<usize>,
    pub build_spills: Vec<SpillMetadata>,
    pub probe_spills: Vec<SpillMetadata>,
}

impl PartitionSlot {
    fn new() -> Self {
        PartitionSlot {
            status: PartitionStatus::Collecting,
            build_rows: 0,
            build_bytes: 0,
            chunks: Vec::new(),
            build_spills: Vec::new(),
            probe_spills: Vec::new(),
        }
    }
}

pub struct PartitionedInner {
    pub build_mode: BuildStageMode,
    pub build_final_mode: BuildFinalMode,
    /// Monotonic counter incremented once per `RestoreBuildFinalCheck` barrier in recursive
    /// sub-joins so all threads agree on whether the sub-join re-enters `BuildFinal`.
    pub build_final_round: usize,
    pub last_round_reentered: bool,

    pub partitions: Vec<PartitionSlot>,
    /// Partition whose chunks all threads are currently inserting into the hash table. Building
    /// one partition at a time lets live `check_spill()` decide before each next partition.
    pub active_build_pid: Option<usize>,
    /// Whether the resident partitions released their memory before restoring spilled ones.
    pub residents_released: bool,
    /// Currently restoring spilled partition id, if any.
    pub restore_partition: Option<usize>,
    pub restore_build_queue: VecDeque<SpillMetadata>,
    pub restore_probe_queue: VecDeque<SpillMetadata>,
    pub restore_sub_state: Option<Arc<PartitionedJoinState>>,
    pub restore_finished: bool,
    /// Runtime filter packets collected across all processors at the end of `Stage::Build`.
    pub rf_packets: Vec<JoinRuntimeFilterPacket>,
}

impl PartitionedInner {
    fn new(num_partitions: usize) -> Self {
        PartitionedInner {
            build_mode: BuildStageMode::Unpartitioned,
            build_final_mode: BuildFinalMode::Deciding,
            build_final_round: 0,
            last_round_reentered: false,
            partitions: (0..num_partitions).map(|_| PartitionSlot::new()).collect(),
            active_build_pid: None,
            residents_released: false,
            restore_partition: None,
            restore_build_queue: VecDeque::new(),
            restore_probe_queue: VecDeque::new(),
            restore_sub_state: None,
            restore_finished: false,
            rf_packets: Vec::new(),
        }
    }
}

/// Shared state across all processors of one hash join recursion level.
///
/// Two level-0 topologies share the same state machine:
/// - **Default (`pre_partitioned == false`)**: starts optimistically in
///   `BuildStageMode::Unpartitioned` (`basic.chunks` + `Buckets::Single`) and falls back on real
///   `check_spill()` to 16-way radix partitioning over `Buckets::Partitioned`.
/// - **Distributed `GlobalShuffle` (`pre_partitioned == true`)**: build and probe already arrive
///   co-partitioned across `T` disjoint lanes `lanes[0..T]`. Each lane builds its own independent
///   `1/T`-sized `Buckets::Single` hash table (or spills directly without any level-0 re-scatter),
///   and spilled lanes restore in full `T`-thread parallel at level 1.
pub struct PartitionedJoinState {
    pub ctx: Arc<QueryContext>,
    pub factory: Arc<HashJoinFactory>,
    pub level: usize,
    pub max_level: usize,
    /// False when spilling/partitioning is disallowed (`CacheScan`, empty keys, or `max_level`).
    pub allow_spill: bool,
    /// True when level-0 inputs arrive pre-partitioned into `lanes.len()` disjoint `GlobalShuffle`
    /// lanes (1-to-1 paired between build and probe ports).
    pub pre_partitioned: bool,

    /// Build chunks and hash table of every partition kept in memory (non-pre-partitioned mode).
    pub basic: Arc<BasicHashJoinState>,
    /// Independent per-lane states when `pre_partitioned == true` (empty otherwise).
    pub lanes: Vec<Arc<BasicHashJoinState>>,
    /// Unallocated remaining estimated build bytes of currently `Building` pre-partitioned lanes,
    /// preventing `T` threads from concurrently overshooting memory limits without double-counting.
    pub in_flight_unallocated_bytes: AtomicUsize,
    /// Empty block with `build_projection` schema: in `PartitionedBuild` (`preserve_build_chunks`
    /// is false) resident chunks are projected in-place, so spilled chunk slots are replaced by
    /// this placeholder to keep chunk indexes aligned.
    placeholder: DataBlock,
    build_input_rows: AtomicUsize,
    /// Build rows routed to the 16 partitions (spilled or resident): sizes `Buckets::Partitioned`.
    partition_rows: AtomicUsize,

    /// Lock-free mirror of `inner.build_mode == BuildStageMode::Partitioned`.
    partitioned_flag: AtomicBool,
    /// Lock-free mirror of `inner.build_final_mode == BuildFinalMode::AbortingSingle`.
    aborting_single: AtomicBool,
    /// Whether any partition at this level has ever spilled to disk.
    ever_spilled: AtomicBool,
    /// Fast-path flag indicating all partitions at this level are already `Spilled`.
    all_spilled: AtomicBool,

    /// Untouched `build_schema` chunks moved out of `basic.chunks` when switching from
    /// `Unpartitioned` (or aborted `Single`) to `Partitioned`, to be scattered in parallel.
    pub transition_queue: ConcurrentQueue<DataBlock>,
    /// Untouched `build_schema` chunks of partition `p`, taken when `p` flips to `Spilled`
    /// and waiting to be written to Parquet.
    pub pending_spill_blocks: Vec<ConcurrentQueue<DataBlock>>,

    inner: Mutex<PartitionedInner>,
}

impl PartitionedJoinState {
    pub fn create(
        ctx: Arc<QueryContext>,
        level: usize,
        factory: Arc<HashJoinFactory>,
        allow_spill: bool,
        shared_build_chunks: bool,
        pre_partitioned_lanes: Option<usize>,
    ) -> Result<Arc<Self>> {
        // On SSE4.2, fast_hash (_mm_crc32_u64) only sets the low 32 bits.
        #[cfg(target_feature = "sse4.2")]
        const HASH_JOIN_SPILL_MAX_LEVEL: usize = 7;
        #[cfg(not(target_feature = "sse4.2"))]
        const HASH_JOIN_SPILL_MAX_LEVEL: usize = 15;

        let settings = ctx.get_settings();
        let max_spill_level = settings.get_max_hash_join_spill_level()? as usize;
        let max_level = max_spill_level.min(HASH_JOIN_SPILL_MAX_LEVEL);

        let desc = factory.desc();
        let build_fields = desc.build_schema.fields();
        let placeholder = DataBlock::new(
            desc.build_projection
                .iter()
                .map(|index| {
                    let data_type = build_fields[*index].data_type();
                    BlockEntry::from(ColumnBuilder::with_capacity(data_type, 0).build())
                })
                .collect(),
            0,
        );

        let (pre_partitioned, num_partitions, lanes) = match pre_partitioned_lanes {
            Some(num_lanes) => (
                true,
                num_lanes,
                (0..num_lanes)
                    .map(|_| Arc::new(BasicHashJoinState::create()))
                    .collect(),
            ),
            None => (false, NUM_PARTITIONS, Vec::new()),
        };

        Ok(Arc::new(PartitionedJoinState {
            ctx,
            factory,
            level,
            max_level,
            allow_spill,
            pre_partitioned,
            basic: Arc::new(BasicHashJoinState::with_shared_build_chunks(
                shared_build_chunks,
            )),
            lanes,
            in_flight_unallocated_bytes: AtomicUsize::new(0),
            placeholder,
            build_input_rows: AtomicUsize::new(0),
            partition_rows: AtomicUsize::new(0),
            partitioned_flag: AtomicBool::new(false),
            aborting_single: AtomicBool::new(false),
            ever_spilled: AtomicBool::new(false),
            all_spilled: AtomicBool::new(false),
            transition_queue: ConcurrentQueue::unbounded(),
            pending_spill_blocks: (0..num_partitions)
                .map(|_| ConcurrentQueue::unbounded())
                .collect(),
            inner: Mutex::new(PartitionedInner::new(num_partitions)),
        }))
    }

    pub fn lock(&self) -> MutexGuard<'_, PartitionedInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn num_partitions(&self) -> usize {
        self.pending_spill_blocks.len()
    }

    pub fn lane(&self, lane_id: usize) -> &Arc<BasicHashJoinState> {
        if self.pre_partitioned {
            &self.lanes[lane_id]
        } else {
            &self.basic
        }
    }

    pub fn has_resident_build_rows(&self) -> bool {
        if self.pre_partitioned {
            self.lanes.iter().any(|lane| *lane.build_rows.deref() > 0)
        } else {
            *self.basic.build_rows.deref() > 0
        }
    }

    pub fn clear_residents(&self) {
        self.basic.clear();
        for lane in &self.lanes {
            lane.clear();
        }
    }

    pub fn can_spill(&self) -> bool {
        self.allow_spill && self.level < self.max_level
    }

    pub fn shift_bits(&self) -> usize {
        self.level * 4
    }

    pub fn is_partitioned(&self) -> bool {
        self.partitioned_flag.load(Ordering::Acquire)
    }

    pub fn set_partitioned(&self) {
        self.partitioned_flag.store(true, Ordering::Release);
    }

    pub fn is_aborting_single(&self) -> bool {
        self.aborting_single.load(Ordering::Acquire)
    }

    pub fn set_aborting_single(&self, aborting: bool) {
        self.aborting_single.store(aborting, Ordering::Release);
    }

    pub fn has_spilled(&self) -> bool {
        self.ever_spilled.load(Ordering::Acquire)
    }

    pub fn set_spilled(&self) {
        self.ever_spilled.store(true, Ordering::Release);
    }

    pub fn are_all_spilled(&self) -> bool {
        self.all_spilled.load(Ordering::Acquire)
    }

    pub fn add_build_input_rows(&self, rows: usize) {
        self.build_input_rows.fetch_add(rows, Ordering::AcqRel);
    }

    pub fn build_input_rows(&self) -> usize {
        self.build_input_rows.load(Ordering::Acquire)
    }

    pub fn add_partition_rows(&self, rows: usize) {
        self.partition_rows.fetch_add(rows, Ordering::AcqRel);
    }

    pub fn partition_rows(&self) -> usize {
        self.partition_rows.load(Ordering::Acquire)
    }

    /// Move all untouched `build_schema` chunks out of `basic` into `transition_queue`, dropping
    /// any half-built `Single` hash table/arenas/scan_map, and switch `build_mode` to
    /// `Partitioned`. Caller must hold `inner` lock.
    pub fn transition_to_partitioned_locked(&self, inner: &mut PartitionedInner) {
        let chunks = self.basic.take_all_chunks_for_transition();
        for chunk in chunks {
            if !chunk.is_empty() {
                self.transition_queue
                    .push(chunk)
                    .expect("push unbounded transition queue failed");
            }
        }
        inner.build_mode = BuildStageMode::Partitioned;
        self.set_partitioned();
    }

    /// Transition partition `pid` from `Collecting` to `Spilled` while `inner` is locked, moving
    /// all of its untouched build chunks to `pending_spill_blocks[pid]`.
    pub fn mark_partition_spilled(&self, inner: &mut PartitionedInner, pid: usize) {
        let slot = &mut inner.partitions[pid];
        if slot.status != PartitionStatus::Collecting {
            return;
        }
        slot.status = PartitionStatus::Spilled;
        slot.build_rows = 0;
        slot.build_bytes = 0;
        let blocks = if self.pre_partitioned {
            self.lanes[pid].take_all_chunks_for_transition()
        } else {
            let chunks = std::mem::take(&mut slot.chunks);
            self.basic.take_unbuilt_chunks(&chunks, &self.placeholder)
        };
        for block in blocks {
            if !block.is_empty() {
                self.pending_spill_blocks[pid]
                    .push(block)
                    .expect("push unbounded concurrent queue failed");
            }
        }
        self.set_spilled();

        if inner
            .partitions
            .iter()
            .all(|s| s.status == PartitionStatus::Spilled)
        {
            self.all_spilled.store(true, Ordering::Release);
        }
    }

    /// Transition an aborted `Building` pre-partitioned lane to `Spilled` while `inner` is locked,
    /// queuing its intact `build_schema` chunks to `pending_spill_blocks[pid]`.
    pub fn mark_building_lane_spilled(
        &self,
        inner: &mut PartitionedInner,
        pid: usize,
        chunks: Vec<DataBlock>,
    ) {
        debug_assert!(self.pre_partitioned);
        let slot = &mut inner.partitions[pid];
        debug_assert_eq!(slot.status, PartitionStatus::Building);
        slot.status = PartitionStatus::Spilled;
        slot.build_rows = 0;
        slot.build_bytes = 0;
        for block in chunks {
            if !block.is_empty() {
                self.pending_spill_blocks[pid]
                    .push(block)
                    .expect("push unbounded concurrent queue failed");
            }
        }
        self.set_spilled();

        if inner
            .partitions
            .iter()
            .all(|s| s.status == PartitionStatus::Spilled)
        {
            self.all_spilled.store(true, Ordering::Release);
        }
    }

    pub fn has_any_pending_spill_blocks(&self) -> bool {
        self.pending_spill_blocks.iter().any(|q| !q.is_empty())
    }
}
