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
use databend_common_expression::HashMethodKind;
use databend_common_sql::plans::JoinType;
use ethnum::U256;
use parquet::file::metadata::RowGroupMetaData;

use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinFactory;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::hash_join_table::RawEntry;
use crate::pipelines::processors::transforms::hash_join_table::StringRawEntry;
use crate::pipelines::processors::transforms::hash_join_table::hash_bits;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

pub const NUM_PARTITIONS: usize = 16;
pub const BYTES_PER_MIB: usize = 1024 * 1024;

/// Process-wide estimated bytes of in-flight flat hash table builds.
///
/// Checked when multiple joins enter `Stage::BuildFinal` concurrently so later joins account for
/// hash tables already admitted into memory before mutating their own build chunks.
static GLOBAL_IN_FLIGHT_FLAT_BUILD_BYTES: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
pub struct SpillMetadata {
    pub path: String,
    pub row_groups: Vec<RowGroupMetaData>,
}

/// Lifecycle of a single radix partition at one recursion level:
///
/// ```text
/// Collecting ──▶ Building ──▶ InMemory  (resident partition, probed in memory)
///     │
///     └────────▶ Spilled                (written to Parquet, restored after probing)
/// ```
///
/// Once a partition enters `Building` it never transitions to `Spilled`; once it enters
/// `Spilled` its untouched `build_schema` chunks are moved to `pending_spill_blocks`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStatus {
    Collecting,
    Building,
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
    /// Whether upfront admission check in `Flat` mode has already decided `Flat` vs `Partitioned`.
    pub flat_final_build_checked: bool,
    /// True during Round 1 of `Stage::BuildFinal` when a flat join switched to `Partitioned` at
    /// the start of `BuildFinal` and all processors are scattering untouched chunks into the 16
    /// partitions before the stage barrier.
    pub scattering_in_build_final: bool,
    /// Whether the deterministic `BuildFinal` resident/spilled partition plan has been computed.
    pub build_final_planned: bool,
    /// Estimated build bytes of partitions currently in `PartitionStatus::Building`.
    pub in_flight_build_bytes: usize,
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
            flat_final_build_checked: false,
            scattering_in_build_final: false,
            build_final_planned: false,
            in_flight_build_bytes: 0,
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

/// Shared state across all processors of one hash join level.
///
/// A join stays on the flat single-partition path (`partitioned == false`, storing blocks in
/// `flat_basic`) whenever it fits in memory — incurring zero partitioning overhead, zero extra
/// key hashing, and zero bucket power-of-2 inflation on non-spilling queries. It switches to
/// 16-way radix partitioned mode (`partitioned == true`) only when memory pressure triggers
/// during `Stage::Build` or when the upfront `Stage::BuildFinal` admission check determines the
/// flat hash table would exceed remaining memory.
pub struct PartitionedJoinState {
    pub ctx: Arc<QueryContext>,
    pub factory: Arc<HashJoinFactory>,
    pub level: usize,
    pub max_level: usize,
    /// False when spilling/partitioning is disallowed (e.g. `CacheScan` reads `flat_basic.chunks`,
    /// empty join keys, or `level >= max_level`).
    pub allow_spill: bool,

    /// Build state for the flat single-partition fast path (and for `TransformCacheScan`).
    pub flat_basic: Arc<BasicHashJoinState>,
    flat_bytes: AtomicUsize,
    flat_reserved_bytes: AtomicUsize,
    build_input_rows: AtomicUsize,

    /// Whether this level has switched from flat mode to 16-way radix partitioned mode.
    partitioned: AtomicBool,
    /// Whether any partition at this level has ever spilled to disk.
    ever_spilled: AtomicBool,
    /// Fast-path flag indicating all 16 partitions are already `Spilled`.
    all_spilled: AtomicBool,

    /// Untouched `build_schema` chunks moved from `flat_basic` when switching to partitioned mode.
    pub transition_queue: ConcurrentQueue<DataBlock>,
    /// Per-partition untouched `build_schema` chunks moved out of `partitions[p].basic_state` when
    /// partition `p` flips `Collecting -> Spilled`.
    pub pending_spill_blocks: [ConcurrentQueue<DataBlock>; NUM_PARTITIONS],

    inner: Mutex<PartitionedInner>,
}

impl Drop for PartitionedJoinState {
    fn drop(&mut self) {
        self.release_flat_build_reservation();
    }
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
            flat_bytes: AtomicUsize::new(0),
            flat_reserved_bytes: AtomicUsize::new(0),
            build_input_rows: AtomicUsize::new(0),
            partitioned: AtomicBool::new(false),
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
        self.partitioned.load(Ordering::Acquire)
    }

    pub fn set_partitioned(&self) {
        self.partitioned.store(true, Ordering::Release);
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

    pub fn add_flat_bytes(&self, bytes: usize) -> usize {
        self.flat_bytes.fetch_add(bytes, Ordering::AcqRel) + bytes
    }

    pub fn flat_bytes(&self) -> usize {
        self.flat_bytes.load(Ordering::Acquire)
    }

    pub fn global_in_flight_flat_build_bytes() -> usize {
        GLOBAL_IN_FLIGHT_FLAT_BUILD_BYTES.load(Ordering::Acquire)
    }

    pub fn reserve_flat_build(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        if self
            .flat_reserved_bytes
            .compare_exchange(0, bytes, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            GLOBAL_IN_FLIGHT_FLAT_BUILD_BYTES.fetch_add(bytes, Ordering::AcqRel);
        }
    }

    pub fn release_flat_build_reservation(&self) {
        let bytes = self.flat_reserved_bytes.swap(0, Ordering::AcqRel);
        if bytes > 0 {
            GLOBAL_IN_FLIGHT_FLAT_BUILD_BYTES.fetch_sub(bytes, Ordering::AcqRel);
        }
    }

    pub fn move_flat_chunks_to_transition(&self) {
        for block in self.flat_basic.take_chunks_and_clear() {
            self.transition_queue
                .push(block)
                .expect("push unbounded concurrent queue failed");
        }
        self.flat_bytes.store(0, Ordering::Release);
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

/// Estimate the net memory growth of building an in-memory hash table for `rows` build rows whose
/// raw chunks occupy `chunks_bytes`.
pub fn estimate_hash_table_bytes(
    rows: usize,
    chunks_bytes: usize,
    method: &HashMethodKind,
    join_type: JoinType,
    has_build_projection: bool,
) -> usize {
    if rows == 0 {
        return 0;
    }

    let bucket_capacity = std::cmp::max(rows.saturating_mul(2).next_power_of_two(), 1 << 10);
    let bucket_bytes = bucket_capacity.saturating_mul(std::mem::size_of::<u64>());

    let entry_size = match method {
        HashMethodKind::KeysU8(_) => std::mem::size_of::<RawEntry<u8>>(),
        HashMethodKind::KeysU16(_) => std::mem::size_of::<RawEntry<u16>>(),
        HashMethodKind::KeysU32(_) => std::mem::size_of::<RawEntry<u32>>(),
        HashMethodKind::KeysU64(_) => std::mem::size_of::<RawEntry<u64>>(),
        HashMethodKind::KeysU128(_) => std::mem::size_of::<RawEntry<u128>>(),
        HashMethodKind::KeysU256(_) => std::mem::size_of::<RawEntry<U256>>(),
        HashMethodKind::Serializer(_) | HashMethodKind::SingleBinary(_) => {
            std::mem::size_of::<StringRawEntry>()
        }
    };
    let entry_bytes = rows.saturating_mul(entry_size);

    let scan_map_bytes = match join_type {
        JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => rows,
        _ => 0,
    };

    // Binary/serializer hash tables copy encoded key bytes into the arena. When `build_projection`
    // is empty, the original key columns are dropped as each chunk is inserted, so net key-byte
    // growth is bounded near zero; otherwise `chunks_bytes` is a safe upper bound.
    let binary_key_bytes = match method {
        HashMethodKind::Serializer(_) | HashMethodKind::SingleBinary(_) if has_build_projection => {
            chunks_bytes
        }
        _ => 0,
    };

    bucket_bytes
        .saturating_add(entry_bytes)
        .saturating_add(scan_map_bytes)
        .saturating_add(binary_key_bytes)
}

/// Estimate the memory needed during `Stage::Probe` when `spilled_count > 0` partitions are
/// spilled and `resident_count` partitions stay in memory across `max_threads` processors.
///
/// Accounts for per-thread `probe_partition_stream` buffers, active `probe_writers` Parquet
/// buffers, and per-resident `PerformanceContext` allocations so `BuildFinal` does not fill the
/// entire budget with resident hash tables and then overshoot during `Stage::Probe`.
pub fn estimate_probe_headroom(
    max_threads: usize,
    max_block_size: usize,
    probe_cols: usize,
    resident_count: usize,
    spilled_count: usize,
) -> usize {
    if spilled_count == 0 {
        return 0;
    }

    let threads = max_threads.max(1);
    let cols = probe_cols.max(1);
    let per_part_stream = max_block_size
        .saturating_mul(cols)
        .saturating_mul(4)
        .min(512 * 1024);
    let stream_bytes = threads
        .saturating_mul(NUM_PARTITIONS)
        .saturating_mul(per_part_stream);
    let writer_bytes = threads
        .saturating_mul(spilled_count)
        .saturating_mul(4 * BYTES_PER_MIB);
    let resident_ctx_bytes = threads
        .saturating_mul(resident_count)
        .saturating_mul(2 * BYTES_PER_MIB);

    stream_bytes
        .saturating_add(writer_bytes)
        .saturating_add(resident_ctx_bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use databend_common_expression::HashMethodFixedKeys;
    use databend_common_expression::HashMethodKind;
    use databend_common_expression::HashMethodSerializer;
    use databend_common_expression::HashMethodSingleBinary;
    use databend_common_sql::plans::JoinType;
    use ethnum::U256;

    use super::NUM_PARTITIONS;
    use super::estimate_hash_table_bytes;
    use super::estimate_probe_headroom;
    use super::get_partition_id;
    use crate::pipelines::processors::transforms::hash_join_table::HashJoinHashTable;
    use crate::pipelines::processors::transforms::hash_join_table::RawEntry;
    use crate::pipelines::processors::transforms::hash_join_table::StringRawEntry;

    #[test]
    fn test_estimate_hash_table_bytes() {
        let u64_method = HashMethodKind::KeysU64(HashMethodFixedKeys::<u64>::default());

        assert_eq!(
            estimate_hash_table_bytes(0, 0, &u64_method, JoinType::Inner, false),
            0
        );

        let rows: usize = 10_000;
        let expected_buckets = (rows * 2).next_power_of_two() * std::mem::size_of::<u64>();
        let expected_u64 = expected_buckets + rows * std::mem::size_of::<RawEntry<u64>>();
        assert_eq!(
            estimate_hash_table_bytes(rows, 80_000, &u64_method, JoinType::Inner, false),
            expected_u64
        );

        // Right join includes 1 byte per row for `scan_map`.
        assert_eq!(
            estimate_hash_table_bytes(rows, 80_000, &u64_method, JoinType::Right, false),
            expected_u64 + rows
        );

        // Wider fixed key uses larger RawEntry.
        let u256_method = HashMethodKind::KeysU256(HashMethodFixedKeys::<U256>::default());
        assert_eq!(
            estimate_hash_table_bytes(rows, 80_000, &u256_method, JoinType::Right, false),
            expected_buckets + rows * std::mem::size_of::<RawEntry<U256>>() + rows
        );

        // Binary/serializer keys add `chunks_bytes` only when `has_build_projection` is true.
        let ser_method = HashMethodKind::Serializer(HashMethodSerializer::default());
        let bin_method = HashMethodKind::SingleBinary(HashMethodSingleBinary::default());
        let expected_str_base = expected_buckets + rows * std::mem::size_of::<StringRawEntry>();
        assert_eq!(
            estimate_hash_table_bytes(rows, 200_000, &ser_method, JoinType::Inner, false),
            expected_str_base
        );
        assert_eq!(
            estimate_hash_table_bytes(rows, 200_000, &bin_method, JoinType::Inner, true),
            expected_str_base + 200_000
        );
    }

    #[test]
    fn test_estimate_probe_headroom() {
        assert_eq!(estimate_probe_headroom(8, 65536, 2, 16, 0), 0);
        let h = estimate_probe_headroom(8, 65536, 1, 4, 12);
        // 32 MiB stream + 384 MiB writers + 64 MiB resident ctx = 480 MiB.
        assert_eq!(h, 480 * 1024 * 1024);
    }

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
