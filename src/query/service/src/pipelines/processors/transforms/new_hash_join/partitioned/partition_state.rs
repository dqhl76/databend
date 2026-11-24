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

/// Lifecycle of a single radix partition at one recursion level:
///
/// ```text
/// Collecting ──▶ InMemory  (built into the hash table and probed in memory)
///     │
///     └────────▶ Spilled   (untouched build chunks written to Parquet, restored in ProbeFinal)
/// ```
///
/// Only `Collecting` partitions, whose chunks are untouched `build_schema` blocks, ever spill.
/// Once a partition becomes `InMemory` it stays in memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStatus {
    Collecting,
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

    pub partitions: [PartitionSlot; NUM_PARTITIONS],
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
    fn new() -> Self {
        PartitionedInner {
            build_mode: BuildStageMode::Unpartitioned,
            build_final_mode: BuildFinalMode::Deciding,
            build_final_round: 0,
            last_round_reentered: false,
            partitions: std::array::from_fn(|_| PartitionSlot::new()),
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
/// Execution starts optimistically in `BuildStageMode::Unpartitioned` (squashing blocks straight
/// into `basic.chunks` with zero scatter/hash overhead) and builds a single contiguous
/// `Buckets::Single` hash table in `Stage::BuildFinal` (zero probe indirection).
///
/// If real `check_spill()` triggers during `Stage::Build`, or `Buckets::Single` does not fit in
/// remaining memory, or `check_spill()` triggers mid-`Stage::BuildFinal` (including from
/// concurrent external queries), the join transitions cleanly to 16-way radix partitioning over
/// `Buckets::Partitioned`, building resident partitions one-by-one and spilling untouched
/// `Collecting` partitions through one multiplexed Parquet writer per thread.
pub struct PartitionedJoinState {
    pub ctx: Arc<QueryContext>,
    pub factory: Arc<HashJoinFactory>,
    pub level: usize,
    pub max_level: usize,
    /// False when spilling/partitioning is disallowed (`CacheScan`, empty keys, or `max_level`).
    pub allow_spill: bool,

    /// Build chunks and hash table of every partition kept in memory.
    pub basic: Arc<BasicHashJoinState>,
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
    /// Fast-path flag indicating all 16 partitions are already `Spilled`.
    all_spilled: AtomicBool,

    /// Untouched `build_schema` chunks moved out of `basic.chunks` when switching from
    /// `Unpartitioned` (or aborted `Single`) to `Partitioned`, to be scattered in parallel.
    pub transition_queue: ConcurrentQueue<DataBlock>,
    /// Untouched `build_schema` chunks of partition `p`, taken from `basic` when `p` flips
    /// `Collecting -> Spilled` and waiting to be written to Parquet.
    pub pending_spill_blocks: [ConcurrentQueue<DataBlock>; NUM_PARTITIONS],

    inner: Mutex<PartitionedInner>,
}

impl PartitionedJoinState {
    pub fn create(
        ctx: Arc<QueryContext>,
        level: usize,
        factory: Arc<HashJoinFactory>,
        allow_spill: bool,
        shared_build_chunks: bool,
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

        Ok(Arc::new(PartitionedJoinState {
            ctx,
            factory,
            level,
            max_level,
            allow_spill,
            basic: Arc::new(BasicHashJoinState::with_shared_build_chunks(
                shared_build_chunks,
            )),
            placeholder,
            build_input_rows: AtomicUsize::new(0),
            partition_rows: AtomicUsize::new(0),
            partitioned_flag: AtomicBool::new(false),
            aborting_single: AtomicBool::new(false),
            ever_spilled: AtomicBool::new(false),
            all_spilled: AtomicBool::new(false),
            transition_queue: ConcurrentQueue::unbounded(),
            pending_spill_blocks: std::array::from_fn(|_| ConcurrentQueue::unbounded()),
            inner: Mutex::new(PartitionedInner::new()),
        }))
    }

    pub fn lock(&self) -> MutexGuard<'_, PartitionedInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
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
        let chunks = std::mem::take(&mut slot.chunks);
        for block in self.basic.take_unbuilt_chunks(&chunks, &self.placeholder) {
            self.pending_spill_blocks[pid]
                .push(block)
                .expect("push unbounded concurrent queue failed");
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
