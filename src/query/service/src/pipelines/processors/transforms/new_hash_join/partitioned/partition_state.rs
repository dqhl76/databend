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
use databend_common_expression::DataBlock;
use parquet::file::metadata::RowGroupMetaData;

use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinFactory;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::hash_join_table::hash_bits;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

pub const NUM_PARTITIONS: usize = 16;
pub const BYTES_PER_MIB: usize = 1024 * 1024;

#[derive(Debug)]
pub struct SpillMetadata {
    pub path: String,
    pub row_groups: Vec<RowGroupMetaData>,
}

/// Lifecycle of a single radix partition at one recursion level:
///
/// ```text
/// Collecting ──▶ InMemory  (resident partition, hash table built and probed in memory)
///     │
///     └────────▶ Spilled   (untouched build chunks written to Parquet, restored in ProbeFinal)
/// ```
///
/// Only `Collecting` partitions (whose chunks are untouched `build_schema` blocks) ever spill.
/// Once a partition transitions to `InMemory` it never transitions back to `Spilled`.
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
    pub basic_state: Arc<BasicHashJoinState>,
    pub build_spills: Vec<SpillMetadata>,
    pub probe_spills: Vec<SpillMetadata>,
}

impl PartitionSlot {
    fn new() -> Self {
        PartitionSlot {
            status: PartitionStatus::Collecting,
            build_rows: 0,
            build_bytes: 0,
            basic_state: Arc::new(BasicHashJoinState::create()),
            build_spills: Vec::new(),
            probe_spills: Vec::new(),
        }
    }
}

pub struct PartitionedInner {
    pub partitions: [PartitionSlot; NUM_PARTITIONS],
    /// Partition currently having its build chunks stolen and inserted by all threads in parallel.
    /// Building one partition at a time keeps memory-pressure checks at 1/16 granularity so real
    /// `check_spill()` reacts promptly to both local growth and concurrent queries.
    pub active_build_pid: Option<usize>,
    /// Whether all resident `InMemory` partitions have released their hash tables before restore.
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
/// When `can_spill()` is true, build rows are radix-partitioned into `NUM_PARTITIONS` independent
/// partitions so both `Stage::Build` and `Stage::BuildFinal` can respond to real `check_spill()`
/// memory pressure (including pressure from concurrent queries) by spilling untouched `Collecting`
/// partitions without ever tearing down a half-built hash table.
///
/// When `can_spill()` is false (`CacheScan`, empty join keys, or `level >= max_level`), build
/// chunks stay in `flat_basic` on the non-partitioned flat path.
pub struct PartitionedJoinState {
    pub ctx: Arc<QueryContext>,
    pub factory: Arc<HashJoinFactory>,
    pub level: usize,
    pub max_level: usize,
    /// False when spilling/partitioning is disallowed (`CacheScan`, empty keys, or `max_level`).
    pub allow_spill: bool,

    /// Build state used when `!can_spill()` (e.g. `TransformCacheScan`, cross join, `max_level`).
    pub flat_basic: Arc<BasicHashJoinState>,
    build_input_rows: AtomicUsize,

    /// Whether any partition at this level has ever spilled to disk.
    ever_spilled: AtomicBool,
    /// Fast-path flag indicating all 16 partitions are already `Spilled`.
    all_spilled: AtomicBool,

    /// Per-partition untouched `build_schema` chunks moved out of `partitions[p].basic_state` when
    /// partition `p` flips `Collecting -> Spilled`.
    pub pending_spill_blocks: [ConcurrentQueue<DataBlock>; NUM_PARTITIONS],

    inner: Mutex<PartitionedInner>,
}

impl PartitionedJoinState {
    pub fn create(
        ctx: Arc<QueryContext>,
        level: usize,
        factory: Arc<HashJoinFactory>,
        allow_spill: bool,
    ) -> Result<Arc<Self>> {
        // On SSE4.2, fast_hash (_mm_crc32_u64) only sets the low 32 bits.
        #[cfg(target_feature = "sse4.2")]
        const HASH_JOIN_SPILL_MAX_LEVEL: usize = 7;
        #[cfg(not(target_feature = "sse4.2"))]
        const HASH_JOIN_SPILL_MAX_LEVEL: usize = 15;

        let settings = ctx.get_settings();
        let max_spill_level = settings.get_max_hash_join_spill_level()? as usize;
        let max_level = max_spill_level.min(HASH_JOIN_SPILL_MAX_LEVEL);

        Ok(Arc::new(PartitionedJoinState {
            ctx,
            factory,
            level,
            max_level,
            allow_spill,
            flat_basic: Arc::new(BasicHashJoinState::create()),
            build_input_rows: AtomicUsize::new(0),
            ever_spilled: AtomicBool::new(false),
            all_spilled: AtomicBool::new(false),
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
        for block in slot.basic_state.take_chunks_and_clear() {
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

/// Partition id of a row at recursion level `shift_bits / 4`.
///
/// A restored (or resident) partition is joined by an in-memory hash table that hashes the same keys
/// with the same hash: the bucket index is the top bits (`hash >> hash_shift`) and the
/// early-filtering tag is the low 4 bits (`hash & 0b1111`). Taking partition bits directly from
/// either end would leave them constant inside a partition and reduce reachable buckets or tags.
///
/// Each level takes the parity of the tag nibble and its own nibble `[4 + shift_bits, 8 + shift_bits)`.
/// Over GF(2) these parities are independent of each other and of the tag bits, keeping bucket
/// indices and tags uniformly spread inside every partition at every level.
#[inline(always)]
pub fn get_partition_id(hash: u64, shift_bits: usize) -> u64 {
    debug_assert!(shift_bits + 8 <= hash_bits() as usize);
    ((hash >> (4 + shift_bits)) ^ hash) & 0b1111
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::NUM_PARTITIONS;
    use super::get_partition_id;
    use crate::pipelines::processors::transforms::hash_join_table::HashJoinHashTable;

    fn hash(key: u64, width: u32) -> u64 {
        let mut h = key;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        h >> (64 - width)
    }

    fn partition_coverage(width: u32, levels: usize, build_rows: usize) -> (f64, usize) {
        let capacity = HashJoinHashTable::<u64>::with_build_row_num(build_rows)
            .pointers
            .len();
        let hash_shift = width - capacity.trailing_zeros();

        let mut buckets = HashSet::new();
        let mut tags = HashSet::new();
        let mut sampled = 0;
        let mut key = 0;
        while sampled < capacity * 8 {
            key += 1;
            let h = hash(key, width);
            if (0..levels).all(|level| get_partition_id(h, level * 4) == 5) {
                buckets.insert(h >> hash_shift);
                tags.insert(h & 0b1111);
                sampled += 1;
            }
        }
        (buckets.len() as f64 / capacity as f64, tags.len())
    }

    #[test]
    fn test_partition_keeps_all_buckets_reachable() {
        for width in [32, 64] {
            for levels in [1, 2] {
                let (bucket_coverage, tags) = partition_coverage(width, levels, 1 << 10);
                assert!(
                    bucket_coverage > 0.99,
                    "width {width}, levels {levels}: only {bucket_coverage} of buckets reachable"
                );
                assert_eq!(tags, NUM_PARTITIONS, "width {width}, levels {levels}");
            }
        }
    }

    #[test]
    fn test_partition_is_balanced() {
        for width in [32, 64] {
            for shift_bits in [0, 4, 24] {
                let mut counts = [0usize; NUM_PARTITIONS];
                for key in 0..(1 << 16) {
                    counts[get_partition_id(hash(key, width), shift_bits) as usize] += 1;
                }
                let expected = (1 << 16) / NUM_PARTITIONS;
                for count in counts {
                    assert!(
                        count.abs_diff(expected) < expected / 10,
                        "width {width}, shift {shift_bits}: {counts:?}"
                    );
                }
            }
        }
    }
}
