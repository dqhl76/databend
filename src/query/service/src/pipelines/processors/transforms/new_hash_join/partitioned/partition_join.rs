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
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::ProgressValues;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::ColumnRef;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::arrow::and_validities;
use databend_common_expression::hash_util::hash_by_method;
use databend_common_pipeline::core::check_interrupt;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::traits::Location;
use databend_common_sql::plans::JoinType;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ReadSettings;
use ethnum::U256;
use log::info;

use super::partition_state::BYTES_PER_MIB;
use super::partition_state::BuildFinalMode;
use super::partition_state::BuildStageMode;
use super::partition_state::PartitionSlot;
use super::partition_state::PartitionStatus;
use super::partition_state::PartitionedInner;
use super::partition_state::PartitionedJoinState;
use super::partition_state::SpillMetadata;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::get_hashes;
use crate::pipelines::processors::transforms::hash_join_table::Buckets;
use crate::pipelines::processors::transforms::hash_join_table::NUM_PARTITIONS;
use crate::pipelines::processors::transforms::hash_join_table::RawEntry;
use crate::pipelines::processors::transforms::hash_join_table::StringRawEntry;
use crate::pipelines::processors::transforms::hash_join_table::partition_id;
use crate::pipelines::processors::transforms::merge_join_runtime_filter_packets;
use crate::pipelines::processors::transforms::new_hash_join::common::SquashBlocks;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::memory::create_hash_table;
use crate::pipelines::processors::transforms::new_hash_join::memory::hash_table_buckets;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextSpillProgress;
use crate::spillers::Layout;
use crate::spillers::SpillAdapter;
use crate::spillers::SpillTarget;
use crate::spillers::SpillsBufferPool;
use crate::spillers::SpillsDataReader;
use crate::spillers::SpillsDataWriter;

/// Per-partition memory cap for `build_partition_stream` once partitioned mode activates.
/// Across 8 threads × 16 partitions this bounds build-stream buffers to at most ~64 MiB while
/// keeping Parquet row groups large enough for sequential I/O.
const BUILD_STREAM_MAX_BLOCK_BYTES: usize = 512 * 1024;
/// Per-partition memory cap for `probe_partition_stream` during `Stage::Probe` spilling.
/// Across 8 threads × 16 partitions this bounds probe-stream buffers to at most ~32 MiB.
const PROBE_STREAM_MAX_BLOCK_BYTES: usize = 256 * 1024;
const PROBE_STREAM_MAX_BLOCK_ROWS: usize = 8192;
/// Per-thread writer memory pool cap (2 × 4 MiB chunks for double-buffered async I/O).
const MAX_MULTIPLEXED_WRITER_POOL_BYTES: usize = 8 * BYTES_PER_MIB;
/// Target uncompressed bytes per Parquet row group before flushing the active partition in
/// `MultiplexedSpillWriter`, bounding `ArrowWriter` internal buffer memory while coalescing
/// consecutive blocks of the same partition (e.g. `GlobalShuffle` lanes).
const MULTIPLEXED_WRITER_ROW_GROUP_BYTES: usize = 4 * BYTES_PER_MIB;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProbeFinalStage {
    FlushProbeBuffers,
    FlushResidentFinal,
    RestoreBuild,
    RestoreBuildFinal,
    RestoreBuildFinalCheck,
    RestoreProbe,
    RestoreProbeFinal,
    Done,
}

/// One Parquet `SpillsDataWriter` per thread, multiplexed across all partitions at a level via
/// `flush_row_groups()`. Reduces peak concurrent writers from `threads × partitions` down to
/// `threads`, cutting writer pool memory while allowing `SpillsDataReader` to fetch each
/// partition's row groups independently by byte range.
struct MultiplexedSpillWriter {
    path: String,
    writer: SpillsDataWriter,
    next_row_group: usize,
    active_pid: Option<usize>,
    active_bytes: usize,
    partition_ranges: Vec<Vec<Range<usize>>>,
}

impl MultiplexedSpillWriter {
    fn create(prefix: &str, writer_pool_bytes: usize, num_partitions: usize) -> Result<Self> {
        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let operator = data_operator.spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let path = format!("{}/{}", prefix, GlobalUniq::unique());
        let writer = buffer_pool.writer(operator, path.clone(), writer_pool_bytes, target)?;
        Ok(MultiplexedSpillWriter {
            path,
            writer,
            next_row_group: 0,
            active_pid: None,
            active_bytes: 0,
            partition_ranges: (0..num_partitions).map(|_| Vec::new()).collect(),
        })
    }

    fn flush_active(&mut self) -> Result<()> {
        let Some(pid) = self.active_pid.take() else {
            return Ok(());
        };
        self.active_bytes = 0;
        let row_group_count = self.writer.flush_row_groups()?;
        if row_group_count > self.next_row_group {
            self.partition_ranges[pid].push(self.next_row_group..row_group_count);
            self.next_row_group = row_group_count;
        }
        Ok(())
    }

    fn write(&mut self, pid: usize, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }
        if self.active_pid.is_some_and(|prev| prev != pid) {
            self.flush_active()?;
        }
        let block_bytes = block.memory_size();
        self.active_pid = Some(pid);
        self.writer.write(block)?;
        self.active_bytes = self.active_bytes.saturating_add(block_bytes);
        if self.active_bytes >= MULTIPLEXED_WRITER_ROW_GROUP_BYTES {
            self.flush_active()?;
        }
        Ok(())
    }

    fn close(mut self, ctx: &Arc<QueryContext>) -> Result<Vec<(usize, SpillMetadata)>> {
        let (written, row_groups) = self.writer.close()?;
        if let Some(pid) = self.active_pid.take()
            && row_groups.len() > self.next_row_group
        {
            self.partition_ranges[pid].push(self.next_row_group..row_groups.len());
            self.next_row_group = row_groups.len();
        }
        ctx.add_spill_file(
            Location::Remote(self.path.clone()),
            Layout::Parquet,
            written,
        );
        record_join_spill_progress(ctx, written, &row_groups);

        let mut out = Vec::new();
        for (pid, ranges) in self.partition_ranges.into_iter().enumerate() {
            let mut part_rgs = Vec::new();
            for range in ranges {
                part_rgs.extend_from_slice(&row_groups[range]);
            }
            if !part_rgs.is_empty() {
                out.push((pid, SpillMetadata {
                    path: self.path.clone(),
                    row_groups: part_rgs,
                }));
            }
        }
        Ok(out)
    }
}

/// Exclusive in-flight build of one pre-partitioned `GlobalShuffle` lane, with RAII release of
/// `in_flight_unallocated_bytes` so cancellations/errors never leak reserved budget.
struct ActiveLaneBuild {
    pid: usize,
    /// Temporary `Join` when work-stealing a lane `pid != lane_id`; `None` uses `local.memory_join`.
    stolen_join: Option<Box<dyn Join>>,
    per_row_bytes: usize,
    reserved_bytes: usize,
    state: Arc<PartitionedJoinState>,
}

impl ActiveLaneBuild {
    fn release(&mut self, bytes: usize) {
        let delta = bytes.min(self.reserved_bytes);
        if delta > 0 {
            self.reserved_bytes -= delta;
            self.state
                .in_flight_unallocated_bytes
                .fetch_sub(delta, Ordering::AcqRel);
        }
    }
}

impl Drop for ActiveLaneBuild {
    fn drop(&mut self) {
        if self.reserved_bytes > 0 {
            self.state
                .in_flight_unallocated_bytes
                .fetch_sub(self.reserved_bytes, Ordering::AcqRel);
            self.reserved_bytes = 0;
        }
    }
}

struct PartitionedLocal {
    /// Used in `BuildStageMode::Unpartitioned` and pre-partitioned `Stage::Build`.
    squash_block: SquashBlocks,
    /// The in-memory join over `state.lane(lane_id)` (`basic` or this processor's `GlobalShuffle` lane).
    memory_join: Option<Box<dyn Join>>,
    /// Currently building pre-partitioned lane owned exclusively by this processor.
    active_lane: Option<ActiveLaneBuild>,
    /// Single multiplexed Parquet writer per thread for build spills.
    build_writer: Option<MultiplexedSpillWriter>,
    /// Single multiplexed Parquet writer per thread for probe spills.
    probe_writer: Option<MultiplexedSpillWriter>,
    /// Tracks whether this processor has already performed the one-time local buffer flush when
    /// partition `pid` transitioned from `Collecting` to `Spilled`, so already-spilled partitions
    /// continue batching full blocks in `build_partition_stream` instead of flushing tiny chunks.
    flushed_on_spill: Vec<bool>,
    /// Lazily allocated only if non-pre-partitioned partitioned mode activates; dropped at end of
    /// `Build` / `Scattering`.
    build_partition_stream: Option<BlockPartitionStream>,
    /// Buffers the probe rows of spilled partitions (non-pre-partitioned mode only).
    probe_partition_stream: Option<BlockPartitionStream>,
    /// Which partitions spilled, read once after `BuildFinal`, when it can no longer change.
    spilled_partitions: Option<Vec<bool>>,
    /// Cached spilled flag for `self.lane_id` in pre-partitioned mode.
    lane_spilled: Option<bool>,
    probe_final_stage: ProbeFinalStage,
    restore_sub_join: Option<Box<PartitionedHashJoin>>,
    restore_build_final_round: usize,
    num_partitions: usize,
    block_rows: usize,
    block_bytes: usize,
    num_threads: usize,
    location_prefix: String,
    writer_pool_bytes: usize,
    read_settings: ReadSettings,
}

impl PartitionedLocal {
    fn create(ctx: &Arc<QueryContext>, num_partitions: usize) -> Result<Self> {
        let settings = ctx.get_settings();
        let block_rows = settings.get_max_block_size()? as usize;
        let block_bytes = settings.get_max_block_bytes()? as usize;
        let num_threads = (settings.get_max_threads()? as usize).max(1);
        let location_prefix = ctx.query_id_spill_prefix();
        let writer_pool_bytes = settings
            .get_spill_writer_memory_pool_size_mb()?
            .saturating_mul(BYTES_PER_MIB)
            .min(MAX_MULTIPLEXED_WRITER_POOL_BYTES);

        Ok(PartitionedLocal {
            squash_block: SquashBlocks::new(block_rows, block_bytes),
            memory_join: None,
            active_lane: None,
            build_writer: None,
            probe_writer: None,
            flushed_on_spill: vec![false; num_partitions],
            build_partition_stream: None,
            probe_partition_stream: None,
            spilled_partitions: None,
            lane_spilled: None,
            probe_final_stage: ProbeFinalStage::FlushProbeBuffers,
            restore_sub_join: None,
            restore_build_final_round: 0,
            num_partitions,
            block_rows,
            block_bytes,
            num_threads,
            location_prefix,
            writer_pool_bytes,
            read_settings: ReadSettings::from_settings(&settings)?,
        })
    }

    fn build_stream(&mut self) -> &mut BlockPartitionStream {
        self.build_partition_stream.get_or_insert_with(|| {
            BlockPartitionStream::create(
                self.block_rows,
                self.block_bytes.min(BUILD_STREAM_MAX_BLOCK_BYTES),
                NUM_PARTITIONS,
            )
        })
    }

    fn probe_stream(&mut self) -> &mut BlockPartitionStream {
        self.probe_partition_stream.get_or_insert_with(|| {
            BlockPartitionStream::create(
                self.block_rows.min(PROBE_STREAM_MAX_BLOCK_ROWS),
                self.block_bytes.min(PROBE_STREAM_MAX_BLOCK_BYTES),
                NUM_PARTITIONS,
            )
        })
    }

    fn probe_spill_reserve_bytes(&self, spilled_or_pending: usize) -> usize {
        let probe_stream_cap =
            spilled_or_pending.saturating_mul(self.block_bytes.min(PROBE_STREAM_MAX_BLOCK_BYTES));
        let per_thread = self
            .writer_pool_bytes
            .saturating_add(probe_stream_cap)
            .saturating_add(2 * BYTES_PER_MIB);
        self.num_threads.saturating_mul(per_thread)
    }

    /// Pre-partitioned `Stage::Probe` needs no `probe_partition_stream`, and only threads owning
    /// spilled lanes open a `probe_writer`.
    fn pre_partitioned_probe_spill_reserve_bytes(&self, spilled_or_pending: usize) -> usize {
        spilled_or_pending.saturating_mul(self.writer_pool_bytes.saturating_add(BYTES_PER_MIB))
    }

    fn write_build_spill(&mut self, pid: usize, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }
        if self.build_writer.is_none() {
            self.build_writer = Some(MultiplexedSpillWriter::create(
                &self.location_prefix,
                self.writer_pool_bytes,
                self.num_partitions,
            )?);
        }
        self.build_writer.as_mut().unwrap().write(pid, block)
    }

    fn write_probe_spill(&mut self, pid: usize, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }
        if self.probe_writer.is_none() {
            self.probe_writer = Some(MultiplexedSpillWriter::create(
                &self.location_prefix,
                self.writer_pool_bytes,
                self.num_partitions,
            )?);
        }
        self.probe_writer.as_mut().unwrap().write(pid, block)
    }

    fn close_build_writer(&mut self, state: &PartitionedJoinState) -> Result<()> {
        if let Some(writer) = self.build_writer.take() {
            let metadata = writer.close(&state.ctx)?;
            if !metadata.is_empty() {
                let mut inner = state.lock();
                for (pid, meta) in metadata {
                    inner.partitions[pid].build_spills.push(meta);
                }
            }
        }
        Ok(())
    }

    fn close_probe_writer(&mut self, state: &PartitionedJoinState) -> Result<()> {
        if let Some(writer) = self.probe_writer.take() {
            let metadata = writer.close(&state.ctx)?;
            if !metadata.is_empty() {
                let mut inner = state.lock();
                for (pid, meta) in metadata {
                    inner.partitions[pid].probe_spills.push(meta);
                }
            }
        }
        Ok(())
    }
}

enum JoinLocal {
    /// Used only when spilling/partitioning is disallowed (`CacheScan`, empty keys, or `max_level`).
    Flat(Box<dyn Join>),
    /// Unified optimistic-Single + 16-way radix-partitioned spillable join.
    Partitioned(Box<PartitionedLocal>),
}

/// Unified hash join with zero-overhead in-memory execution (`Unpartitioned` build +
/// `Buckets::Single` probe) and seamless fallback to 16-way radix partitioning under real
/// `check_spill()` memory pressure during both `Stage::Build` and `Stage::BuildFinal`.
pub struct PartitionedHashJoin {
    local: JoinLocal,
    state: Arc<PartitionedJoinState>,
    lane_id: usize,
    memory_settings: MemorySettings,
    function_ctx: FunctionContext,
    hash_method_kind: HashMethodKind,
    desc: Arc<HashJoinDesc>,
    join_type: JoinType,
    /// Whether the join outputs unmatched build rows (so build rows with NULL keys must be kept).
    keeps_null_key_build_rows: bool,
    /// Direct `ColumnRef` build key offsets that are nullable and not NULL-equal, used for O(1)
    /// NULL-key pre-filtering in `Stage::Build` without invoking `Evaluator`.
    nullable_colref_keys: Vec<usize>,
    is_binary_hash_method: bool,
    inlist_threshold: usize,
    bloom_threshold: usize,
    min_max_threshold: usize,
}

unsafe impl Send for PartitionedHashJoin {}
unsafe impl Sync for PartitionedHashJoin {}

impl PartitionedHashJoin {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<QueryContext>,
        function_ctx: FunctionContext,
        hash_method_kind: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        memory_settings: MemorySettings,
        state: Arc<PartitionedJoinState>,
        flat_join: Option<Box<dyn Join>>,
        join_type: JoinType,
        lane_id: usize,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let inlist_threshold = settings.get_inlist_runtime_filter_threshold()? as usize;
        let bloom_threshold = settings.get_bloom_runtime_filter_threshold()? as usize;
        let min_max_threshold = settings.get_min_max_runtime_filter_threshold()? as usize;

        let num_partitions = state.num_partitions();
        let local = match flat_join {
            Some(join) => JoinLocal::Flat(join),
            None => {
                JoinLocal::Partitioned(Box::new(PartitionedLocal::create(&ctx, num_partitions)?))
            }
        };

        let keeps_null_key_build_rows = matches!(
            join_type,
            JoinType::Right | JoinType::RightSemi | JoinType::RightAnti
        );

        let mut nullable_colref_keys = Vec::new();
        if !keeps_null_key_build_rows {
            for (expr, &is_null_equal) in desc.build_keys.iter().zip(&desc.is_null_equal) {
                if !is_null_equal
                    && let Expr::ColumnRef(ColumnRef { id, .. }) = expr
                    && desc
                        .build_schema
                        .field(*id)
                        .data_type()
                        .is_nullable_or_null()
                {
                    nullable_colref_keys.push(*id);
                }
            }
        }

        let is_binary_hash_method = matches!(
            hash_method_kind,
            HashMethodKind::SingleBinary(_) | HashMethodKind::Serializer(_)
        );

        Ok(PartitionedHashJoin {
            local,
            state,
            lane_id,
            memory_settings,
            function_ctx,
            hash_method_kind,
            desc,
            join_type,
            keeps_null_key_build_rows,
            nullable_colref_keys,
            is_binary_hash_method,
            inlist_threshold,
            bloom_threshold,
            min_max_threshold,
        })
    }

    fn has_right_final_probe(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::Right | JoinType::RightSemi | JoinType::RightAnti
        )
    }

    fn partition_needs_restore(&self, has_build: bool, has_probe: bool) -> bool {
        match self.join_type {
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => has_build && has_probe,
            JoinType::Left | JoinType::LeftAnti => has_probe,
            JoinType::Right | JoinType::RightAnti => has_build,
            _ => has_build || has_probe,
        }
    }

    /// O(1) fast-path check to drop NULL-key build rows for `!keeps_null_key_build_rows` joins
    /// before squashing/storing. When no column has nulls (`null_count() == 0`), returns `block`
    /// untouched without calling `Evaluator` or allocating.
    fn filter_null_key_rows_fast(&self, block: DataBlock) -> Result<DataBlock> {
        if self.nullable_colref_keys.is_empty() || block.is_empty() {
            return Ok(block);
        }

        let mut valids: Option<Bitmap> = None;
        for &col_id in &self.nullable_colref_keys {
            match block.get_by_offset(col_id) {
                BlockEntry::Const(scalar, _, _) => {
                    if scalar.is_null() {
                        return Ok(DataBlock::empty());
                    }
                }
                BlockEntry::Column(col) => {
                    let (is_all_null, col_valids) = col.validity();
                    if is_all_null {
                        return Ok(DataBlock::empty());
                    }
                    if let Some(bitmap) = col_valids
                        && bitmap.null_count() > 0
                    {
                        valids = and_validities(valids, Some(bitmap.clone()));
                        if let Some(v) = valids.as_ref()
                            && v.true_count() == 0
                        {
                            return Ok(DataBlock::empty());
                        }
                    }
                }
            }
        }

        match valids {
            None => Ok(block),
            Some(bitmap) if bitmap.true_count() == bitmap.len() => Ok(block),
            Some(bitmap) => block.filter_with_bitmap(&bitmap),
        }
    }

    fn push_unpartitioned_chunk(&mut self, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }
        let already_partitioned = {
            let inner = self.state.lock();
            if inner.build_mode == BuildStageMode::Unpartitioned {
                self.state.basic.push_chunk(block);
                return Ok(());
            }
            true
        };
        if already_partitioned {
            self.partition_build_block(block)?;
        }
        Ok(())
    }

    fn transition_build_to_partitioned(&mut self) -> Result<()> {
        {
            let mut inner = self.state.lock();
            if inner.build_mode == BuildStageMode::Unpartitioned {
                info!(
                    "HashJoin switching Stage::Build to partitioned mode due to check_spill, level: {}, build_rows: {}",
                    self.state.level,
                    *self.state.basic.build_rows.deref()
                );
                self.state.transition_to_partitioned_locked(&mut inner);
            }
        }
        self.flush_local_squash_into_partitions()?;
        self.drain_transition_queue_on_build()?;
        self.spill_partitions_on_build_pressure()
    }

    fn flush_local_squash_into_partitions(&mut self) -> Result<()> {
        let flushed = match &mut self.local {
            JoinLocal::Partitioned(local) => local.squash_block.finalize()?,
            JoinLocal::Flat(_) => return Ok(()),
        };
        if let Some(block) = flushed {
            self.partition_build_block(block)?;
        }
        Ok(())
    }

    fn drain_transition_queue_on_build(&mut self) -> Result<()> {
        while let Ok(chunk) = self.state.transition_queue.pop() {
            check_interrupt()?;
            self.partition_build_block(chunk)?;
            self.spill_partitions_on_build_pressure()?;
        }
        Ok(())
    }

    fn partition_build_block(&mut self, mut block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        let keys = self.desc.build_key(&block, &self.function_ctx)?;
        let mut keys = DataBlock::new(keys, block.num_rows());
        if !self.keeps_null_key_build_rows
            && let Some(valids) = self.desc.build_valids_by_keys(&keys)?
            && valids.true_count() != valids.len()
        {
            block = block.filter_with_bitmap(&valids)?;
            keys = keys.filter_with_bitmap(&valids)?;
        }
        if block.is_empty() {
            return Ok(());
        }

        self.desc.remove_keys_nullable(&mut keys);
        let mut hashes = Vec::with_capacity(block.num_rows());
        hash_by_method(
            &self.hash_method_kind,
            ProjectedBlock::from(keys.columns()),
            block.num_rows(),
            &mut hashes,
        )?;
        let shift_bits = self.state.shift_bits();
        for hash in hashes.iter_mut() {
            *hash = partition_id(*hash, shift_bits);
        }

        let ready = match &mut self.local {
            JoinLocal::Partitioned(local) => local.build_stream().partition(hashes, block, true),
            JoinLocal::Flat(_) => unreachable!("partition_build_block called in Flat mode"),
        };

        for (pid, ready_block) in ready {
            check_interrupt()?;
            self.route_build_block(pid, ready_block)?;
        }
        Ok(())
    }

    fn route_build_block(&mut self, pid: usize, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }
        self.state.add_partition_rows(block.num_rows());

        // Fast path when all 16 partitions are already Spilled (e.g. BUILD-SPILL): avoid locking.
        if self.state.are_all_spilled() {
            if let JoinLocal::Partitioned(local) = &mut self.local {
                local.write_build_spill(pid, block)?;
            }
            return Ok(());
        }

        let spilled = {
            let mut inner = self.state.lock();
            let slot = &mut inner.partitions[pid];
            match slot.status {
                PartitionStatus::Collecting => {
                    slot.build_rows += block.num_rows();
                    slot.build_bytes += block.memory_size();
                    let chunk_index = self.state.basic.push_unqueued_chunk(block);
                    slot.chunks.push(chunk_index);
                    return Ok(());
                }
                PartitionStatus::Spilled => true,
                PartitionStatus::Building | PartitionStatus::InMemory => {
                    unreachable!("route_build_block called after build stage")
                }
            }
        };

        if spilled && let JoinLocal::Partitioned(local) = &mut self.local {
            local.write_build_spill(pid, block)?;
        }
        Ok(())
    }

    /// Route one squashed build block directly to this processor's pre-partitioned `GlobalShuffle`
    /// lane without evaluating join keys, hashing, or scattering.
    fn route_pre_partitioned_build_block(&mut self, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        let pid = self.lane_id;
        if self.state.are_all_spilled() {
            if let JoinLocal::Partitioned(local) = &mut self.local {
                local.write_build_spill(pid, block)?;
            }
            return Ok(());
        }

        let spilled = {
            let mut inner = self.state.lock();
            let slot = &mut inner.partitions[pid];
            match slot.status {
                PartitionStatus::Collecting => {
                    slot.build_rows += block.num_rows();
                    slot.build_bytes += block.memory_size();
                    self.state.lanes[pid].push_chunk(block);
                    return Ok(());
                }
                PartitionStatus::Spilled => true,
                PartitionStatus::Building | PartitionStatus::InMemory => {
                    unreachable!("route_pre_partitioned_build_block called after build stage")
                }
            }
        };

        if spilled && let JoinLocal::Partitioned(local) = &mut self.local {
            local.write_build_spill(pid, block)?;
        }
        Ok(())
    }

    fn flush_local_squash_if_lane_spilled(&mut self) -> Result<()> {
        let is_spilled = {
            let inner = self.state.lock();
            inner.partitions[self.lane_id].status == PartitionStatus::Spilled
        };
        if !is_spilled {
            return Ok(());
        }
        if let JoinLocal::Partitioned(local) = &mut self.local
            && let Some(block) = local.squash_block.finalize()?
        {
            local.write_build_spill(self.lane_id, block)?;
        }
        Ok(())
    }

    /// Under real `check_spill()` pressure during pre-partitioned `Stage::Build`, spill the
    /// largest `Collecting` `GlobalShuffle` lanes one by one directly to Parquet (zero scatter).
    fn spill_lanes_on_build_pressure(&mut self) -> Result<()> {
        if !self.state.can_spill() {
            return Ok(());
        }

        if self.state.has_spilled() {
            self.flush_local_squash_if_lane_spilled()?;
        }
        self.drain_pending_spill_blocks()?;

        while self.memory_settings.check_spill() {
            check_interrupt()?;

            let victim = {
                let mut inner = self.state.lock();
                let largest_nonempty = inner
                    .partitions
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| s.status == PartitionStatus::Collecting && s.build_rows > 0)
                    .max_by_key(|(_, s)| (s.build_bytes, s.build_rows))
                    .map(|(pid, s)| (pid, s.build_rows, s.build_bytes));

                if let Some((pid, rows, bytes)) = largest_nonempty {
                    info!(
                        "HashJoin spilling pre-partitioned lane {} during build collect, level: {}, rows: {}, bytes: {}",
                        pid, self.state.level, rows, bytes
                    );
                    self.state.mark_partition_spilled(&mut inner, pid);
                    Some(pid)
                } else {
                    let mut flipped_count = 0usize;
                    let num_lanes = inner.partitions.len();
                    for pid in 0..num_lanes {
                        if inner.partitions[pid].status == PartitionStatus::Collecting {
                            self.state.mark_partition_spilled(&mut inner, pid);
                            flipped_count += 1;
                        }
                    }
                    if flipped_count == 0 {
                        break;
                    }
                    info!(
                        "HashJoin marked remaining {} collecting pre-partitioned lanes spilled during build collect, level: {}",
                        flipped_count, self.state.level
                    );
                    None
                }
            };

            if victim.is_none() || victim == Some(self.lane_id) {
                self.flush_local_squash_if_lane_spilled()?;
            }
            self.drain_pending_spill_blocks()?;

            if victim.is_none() {
                break;
            }
        }

        Ok(())
    }

    fn add_block_pre_partitioned(&mut self, data: Option<DataBlock>) -> Result<()> {
        match data {
            Some(block) => {
                let block = self.filter_null_key_rows_fast(block)?;
                if block.is_empty() {
                    return Ok(());
                }

                let squashed = match &mut self.local {
                    JoinLocal::Partitioned(local) => local.squash_block.add_block(block)?,
                    JoinLocal::Flat(_) => unreachable!(),
                };
                if let Some(squashed) = squashed {
                    self.route_pre_partitioned_build_block(squashed)?;
                }
                if self.state.can_spill()
                    && (self.state.has_spilled() || self.memory_settings.check_spill())
                {
                    self.spill_lanes_on_build_pressure()?;
                }
            }
            None => {
                let flushed = match &mut self.local {
                    JoinLocal::Partitioned(local) => local.squash_block.finalize()?,
                    JoinLocal::Flat(_) => unreachable!(),
                };
                if let Some(block) = flushed {
                    self.route_pre_partitioned_build_block(block)?;
                }
                if self.state.can_spill()
                    && (self.state.has_spilled() || self.memory_settings.check_spill())
                {
                    self.spill_lanes_on_build_pressure()?;
                }
                if let JoinLocal::Partitioned(local) = &mut self.local {
                    local.close_build_writer(&self.state)?;
                }
            }
        }
        Ok(())
    }

    fn per_row_build_overhead_bytes(&self) -> usize {
        let entry_size = hash_entry_size(&self.hash_method_kind);
        let scan_map_per_row = usize::from(self.has_right_final_probe());
        let binary_prefix = if self.is_binary_hash_method { 8 } else { 0 };
        entry_size + scan_map_per_row + binary_prefix
    }

    fn pre_partitioned_lane_cost(&self, rows: usize) -> (usize, usize, usize) {
        let buckets_bytes = Buckets::capacity(rows) * std::mem::size_of::<u64>();
        let per_row_bytes = self.per_row_build_overhead_bytes();
        let total = buckets_bytes.saturating_add(rows.saturating_mul(per_row_bytes));
        (buckets_bytes, per_row_bytes, total)
    }

    fn should_spill_next_pre_partitioned_lane(
        &self,
        inner: &PartitionedInner,
        pid: usize,
        lane_cost: usize,
    ) -> bool {
        if !self.state.can_spill() {
            return false;
        }
        if self.memory_settings.check_spill() {
            return true;
        }
        let Some(remain) = self.memory_settings.check_spill_remain() else {
            return false;
        };
        if remain <= 0 {
            return true;
        }

        let in_flight = self
            .state
            .in_flight_unallocated_bytes
            .load(Ordering::Acquire);
        let remaining_collecting_cost: usize = inner
            .partitions
            .iter()
            .filter(|s| s.status == PartitionStatus::Collecting)
            .map(|s| self.pre_partitioned_lane_cost(s.build_rows).2)
            .sum();

        let will_have_spilled = self.state.has_spilled()
            || (in_flight.saturating_add(remaining_collecting_cost) as isize > remain);

        let probe_reserve = if will_have_spilled {
            let spilled_or_pending = inner
                .partitions
                .iter()
                .enumerate()
                .filter(|&(p, s)| {
                    s.status == PartitionStatus::Spilled
                        || (s.status == PartitionStatus::Collecting && p != pid)
                })
                .count();
            match &self.local {
                JoinLocal::Partitioned(local) => {
                    local.pre_partitioned_probe_spill_reserve_bytes(spilled_or_pending)
                }
                JoinLocal::Flat(_) => 0,
            }
        } else {
            0
        };

        in_flight
            .saturating_add(lane_cost)
            .saturating_add(probe_reserve) as isize
            > remain
    }

    fn abort_active_pre_partitioned_lane(&mut self) {
        let JoinLocal::Partitioned(local) = &mut self.local else {
            return;
        };
        let Some(active) = local.active_lane.take() else {
            return;
        };
        let pid = active.pid;
        drop(active);
        if pid == self.lane_id {
            local.memory_join = None;
        }
        let lane_rows = *self.state.lanes[pid].build_rows.deref();
        let chunks = self.state.lanes[pid].take_all_chunks_for_transition();
        let mut inner = self.state.lock();
        info!(
            "HashJoin aborting pre-partitioned lane {} BuildFinal due to check_spill, level: {}, rows: {}",
            pid, self.state.level, lane_rows
        );
        self.state
            .mark_building_lane_spilled(&mut inner, pid, chunks);
    }

    /// Build each `GlobalShuffle` lane into its own cache-hot `1/T`-sized `Buckets::Single` hash
    /// table. Threads prefer their own `lane_id` first, track in-flight unallocated bytes to
    /// respect tight memory budgets without double-counting, and can abort a `Building` lane
    /// straight to Parquet on live `check_spill()` without barrier re-entry or scattering.
    fn final_build_pre_partitioned(&mut self) -> Result<Option<ProgressValues>> {
        loop {
            check_interrupt()?;

            let has_active = matches!(
                &self.local,
                JoinLocal::Partitioned(local) if local.active_lane.is_some()
            );

            if has_active {
                let step_res = {
                    let JoinLocal::Partitioned(local) = &mut self.local else {
                        unreachable!();
                    };
                    let active = local.active_lane.as_mut().unwrap();
                    let progress = match active.stolen_join.as_mut() {
                        Some(join) => join.final_build()?,
                        None => {
                            if local.memory_join.is_none() {
                                local.memory_join = Some(self.state.factory.create_memory_join(
                                    self.join_type,
                                    self.state.lane(self.lane_id).clone(),
                                )?);
                            }
                            local.memory_join.as_mut().unwrap().final_build()?
                        }
                    };
                    if let Some(p) = &progress {
                        active.release(p.rows.saturating_mul(active.per_row_bytes));
                    }
                    progress
                };

                match step_res {
                    Some(progress) => {
                        if self.state.can_spill() && self.memory_settings.check_spill() {
                            self.abort_active_pre_partitioned_lane();
                        }
                        return Ok(Some(progress));
                    }
                    None => {
                        let pid = {
                            let JoinLocal::Partitioned(local) = &mut self.local else {
                                unreachable!();
                            };
                            let active = local.active_lane.take().unwrap();
                            let pid = active.pid;
                            drop(active);
                            pid
                        };
                        self.state.lanes[pid].finalize_chunks(&self.desc.build_projection);
                        let mut inner = self.state.lock();
                        inner.partitions[pid].status = PartitionStatus::InMemory;
                        continue;
                    }
                }
            }

            if self.state.has_any_pending_spill_blocks() {
                let drained = self.drain_pending_spill_blocks()?;
                if drained.rows > 0 {
                    return Ok(Some(drained));
                }
            }

            enum LaneStep {
                Claim {
                    pid: usize,
                    buckets_bytes: usize,
                    per_row_bytes: usize,
                    lane_cost: usize,
                },
                DrainSpills,
                Done,
            }

            let step = {
                let mut inner = self.state.lock();

                // Empty lanes cost no build work; initialize empty `Buckets::Single` so `Probe`
                // never sees `HashJoinHashTable::Null`.
                for (pid, slot) in inner.partitions.iter_mut().enumerate() {
                    if slot.status == PartitionStatus::Collecting && slot.build_rows == 0 {
                        slot.status = PartitionStatus::InMemory;
                        let lane = &self.state.lanes[pid];
                        if matches!(lane.hash_table.deref(), HashJoinHashTable::Null) {
                            let buckets = Buckets::with_build_row_num(0);
                            *lane.hash_table.as_mut() =
                                create_hash_table(&self.desc, &self.hash_method_kind, buckets);
                        }
                        lane.finalize_chunks(&self.desc.build_projection);
                    }
                }

                // Prefer own lane first for L2/L3 cache locality, otherwise smallest remaining lane.
                let next_lane =
                    if inner.partitions[self.lane_id].status == PartitionStatus::Collecting {
                        Some(self.lane_id)
                    } else {
                        inner
                            .partitions
                            .iter()
                            .enumerate()
                            .filter(|(_, s)| s.status == PartitionStatus::Collecting)
                            .min_by_key(|&(pid, s)| (s.build_bytes, s.build_rows, pid))
                            .map(|(pid, _)| pid)
                    };

                match next_lane {
                    None => LaneStep::Done,
                    Some(pid) => {
                        let rows = inner.partitions[pid].build_rows;
                        let (buckets_bytes, per_row_bytes, lane_cost) =
                            self.pre_partitioned_lane_cost(rows);
                        if self.should_spill_next_pre_partitioned_lane(&inner, pid, lane_cost) {
                            let num_lanes = inner.partitions.len();
                            let mut newly_spilled = 0usize;
                            for p in 0..num_lanes {
                                if inner.partitions[p].status == PartitionStatus::Collecting {
                                    self.state.mark_partition_spilled(&mut inner, p);
                                    newly_spilled += 1;
                                }
                            }
                            let total_spilled = inner
                                .partitions
                                .iter()
                                .filter(|s| s.status == PartitionStatus::Spilled)
                                .count();
                            info!(
                                "HashJoin pre-partitioned build final spill triggered, level: {}, lanes: {}, resident_or_building: {}, spilled: {} (newly_spilled: {})",
                                self.state.level,
                                num_lanes,
                                num_lanes - total_spilled,
                                total_spilled,
                                newly_spilled
                            );
                            LaneStep::DrainSpills
                        } else {
                            inner.partitions[pid].status = PartitionStatus::Building;
                            self.state
                                .in_flight_unallocated_bytes
                                .fetch_add(lane_cost, Ordering::AcqRel);
                            LaneStep::Claim {
                                pid,
                                buckets_bytes,
                                per_row_bytes,
                                lane_cost,
                            }
                        }
                    }
                }
            };

            match step {
                LaneStep::Claim {
                    pid,
                    buckets_bytes,
                    per_row_bytes,
                    lane_cost,
                } => {
                    let mut active = ActiveLaneBuild {
                        pid,
                        stolen_join: None,
                        per_row_bytes,
                        reserved_bytes: lane_cost,
                        state: self.state.clone(),
                    };
                    let lane = &self.state.lanes[pid];
                    lane.preserve_build_chunks
                        .store(self.state.can_spill(), Ordering::Release);
                    let buckets = Buckets::with_build_row_num(*lane.build_rows.deref());
                    *lane.hash_table.as_mut() =
                        create_hash_table(&self.desc, &self.hash_method_kind, buckets);
                    active.release(buckets_bytes);

                    if pid != self.lane_id {
                        active.stolen_join = Some(
                            self.state
                                .factory
                                .create_memory_join(self.join_type, lane.clone())?,
                        );
                    }

                    if let JoinLocal::Partitioned(local) = &mut self.local {
                        local.active_lane = Some(active);
                    }

                    if self.state.can_spill() && self.memory_settings.check_spill() {
                        self.abort_active_pre_partitioned_lane();
                    }
                }
                LaneStep::DrainSpills => {
                    let drained = self.drain_pending_spill_blocks()?;
                    if drained.rows > 0 {
                        return Ok(Some(drained));
                    }
                }
                LaneStep::Done => {
                    let drained = self.drain_pending_spill_blocks()?;
                    if drained.rows > 0 {
                        return Ok(Some(drained));
                    }
                    if let JoinLocal::Partitioned(local) = &mut self.local {
                        local.close_build_writer(&self.state)?;
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn flush_and_drop_build_partition_stream(&mut self) -> Result<()> {
        let ready = match &mut self.local {
            JoinLocal::Partitioned(local) => {
                let Some(mut stream) = local.build_partition_stream.take() else {
                    return Ok(());
                };
                let ids = stream.partition_ids();
                let mut ready = Vec::with_capacity(ids.len());
                for pid in ids {
                    if let Some(block) = stream.finalize_partition(pid) {
                        ready.push((pid, block));
                    }
                }
                ready
            }
            JoinLocal::Flat(_) => return Ok(()),
        };

        for (pid, block) in ready {
            check_interrupt()?;
            self.route_build_block(pid, block)?;
        }
        Ok(())
    }

    fn drain_pending_spill_blocks(&mut self) -> Result<ProgressValues> {
        let mut progress = ProgressValues::default();
        let JoinLocal::Partitioned(local) = &mut self.local else {
            return Ok(progress);
        };

        for (pid, queue) in self.state.pending_spill_blocks.iter().enumerate() {
            while let Ok(block) = queue.pop() {
                check_interrupt()?;
                progress.rows += block.num_rows();
                progress.bytes += block.memory_size();
                local.write_build_spill(pid, block)?;
            }
        }
        Ok(progress)
    }

    /// Under real `check_spill()` pressure during `Stage::Build` or `Scattering`, spill the
    /// largest `Collecting` partitions one by one until pressure eases (or all 16 are `Spilled`).
    fn spill_partitions_on_build_pressure(&mut self) -> Result<()> {
        if !self.state.can_spill() {
            return Ok(());
        }

        if self.state.are_all_spilled()
            && !self.state.has_any_pending_spill_blocks()
            && matches!(
                &self.local,
                JoinLocal::Partitioned(local) if local.flushed_on_spill.iter().all(|&f| f)
            )
        {
            return Ok(());
        }

        // First flush this thread's local buffer once for any partition already marked Spilled by
        // another processor.
        if self.state.has_spilled()
            && let JoinLocal::Partitioned(local) = &mut self.local
        {
            let need_flush: [bool; NUM_PARTITIONS] = {
                let inner = self.state.lock();
                std::array::from_fn(|pid| {
                    inner.partitions[pid].status == PartitionStatus::Spilled
                        && !local.flushed_on_spill[pid]
                })
            };
            for (pid, should_flush) in need_flush.into_iter().enumerate() {
                if should_flush {
                    local.flushed_on_spill[pid] = true;
                    if let Some(stream) = local.build_partition_stream.as_mut()
                        && let Some(block) = stream.finalize_partition(pid)
                    {
                        local.write_build_spill(pid, block)?;
                    }
                }
            }
        }

        self.drain_pending_spill_blocks()?;

        while self.memory_settings.check_spill() {
            check_interrupt()?;

            let victim = {
                let mut inner = self.state.lock();
                let largest_nonempty = inner
                    .partitions
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| s.status == PartitionStatus::Collecting && s.build_rows > 0)
                    .max_by_key(|(_, s)| (s.build_bytes, s.build_rows))
                    .map(|(pid, s)| (pid, s.build_rows, s.build_bytes));

                if let Some((pid, rows, bytes)) = largest_nonempty {
                    info!(
                        "HashJoin spilling partition {} during build collect, level: {}, rows: {}, bytes: {}",
                        pid, self.state.level, rows, bytes
                    );
                    self.state.mark_partition_spilled(&mut inner, pid);
                    Some(pid)
                } else {
                    let mut flipped_count = 0usize;
                    for pid in 0..NUM_PARTITIONS {
                        if inner.partitions[pid].status == PartitionStatus::Collecting {
                            self.state.mark_partition_spilled(&mut inner, pid);
                            flipped_count += 1;
                        }
                    }
                    if flipped_count == 0 {
                        break;
                    }
                    info!(
                        "HashJoin marked remaining {} collecting partitions spilled during build collect, level: {}",
                        flipped_count, self.state.level
                    );
                    None
                }
            };

            if let JoinLocal::Partitioned(local) = &mut self.local {
                match victim {
                    Some(pid) => {
                        local.flushed_on_spill[pid] = true;
                        if let Some(stream) = local.build_partition_stream.as_mut()
                            && let Some(block) = stream.finalize_partition(pid)
                        {
                            local.write_build_spill(pid, block)?;
                        }
                    }
                    None => {
                        for pid in 0..NUM_PARTITIONS {
                            if !local.flushed_on_spill[pid] {
                                local.flushed_on_spill[pid] = true;
                                if let Some(stream) = local.build_partition_stream.as_mut()
                                    && let Some(block) = stream.finalize_partition(pid)
                                {
                                    local.write_build_spill(pid, block)?;
                                }
                            }
                        }
                    }
                }
            }

            self.drain_pending_spill_blocks()?;

            if victim.is_none() {
                break;
            }
        }

        Ok(())
    }

    /// Decide the initial `BuildFinalMode` once upon entering `Stage::BuildFinal`.
    fn ensure_build_final_mode(&self) -> BuildFinalMode {
        let mut inner = self.state.lock();
        if inner.build_final_mode != BuildFinalMode::Deciding {
            return inner.build_final_mode;
        }

        if inner.build_mode == BuildStageMode::Partitioned {
            self.state
                .basic
                .preserve_build_chunks
                .store(false, Ordering::Release);
            inner.build_final_mode = BuildFinalMode::PartitionedBuild;
            return BuildFinalMode::PartitionedBuild;
        }

        // `Stage::Build` stayed Unpartitioned. Guard only against the exact single contiguous
        // `Buckets::Single` allocation exceeding remaining memory; if it fits, start optimistic
        // `Single` build while checking live `check_spill()` after every chunk.
        let rows = *self.state.basic.build_rows.deref();
        let buckets_bytes = Buckets::capacity(rows) * std::mem::size_of::<u64>();
        let must_partition = rows > 0
            && self.state.can_spill()
            && (self.memory_settings.check_spill()
                || self
                    .memory_settings
                    .check_spill_remain()
                    .is_some_and(|remain| remain < buckets_bytes as isize));

        if must_partition {
            info!(
                "HashJoin switching to partitioned BuildFinal before Single buckets ({} MiB), level: {}, rows: {}",
                buckets_bytes / BYTES_PER_MIB,
                self.state.level,
                rows
            );
            self.state.transition_to_partitioned_locked(&mut inner);
            inner.build_final_mode = BuildFinalMode::Scattering;
            BuildFinalMode::Scattering
        } else {
            self.state
                .basic
                .preserve_build_chunks
                .store(self.state.can_spill(), Ordering::Release);
            let buckets = Buckets::with_build_row_num(rows);
            *self.state.basic.hash_table.as_mut() =
                create_hash_table(&self.desc, &self.hash_method_kind, buckets);
            inner.build_final_mode = BuildFinalMode::Single;
            BuildFinalMode::Single
        }
    }

    fn final_build_unified(&mut self) -> Result<Option<ProgressValues>> {
        let mode = self.ensure_build_final_mode();
        match mode {
            BuildFinalMode::Deciding => unreachable!("ensure_build_final_mode resolved Deciding"),
            BuildFinalMode::Single => self.final_build_single(),
            BuildFinalMode::AbortingSingle => Ok(None),
            BuildFinalMode::Scattering => self.final_build_scattering(),
            BuildFinalMode::PartitionedBuild => self.final_build_partitioned(),
        }
    }

    /// Optimistic `Buckets::Single` build with `preserve_build_chunks = true`. Checks real
    /// `check_spill()` after every chunk inserted so both local arena growth and external
    /// concurrent query pressure can abort cleanly before the `Stage::BuildFinal` barrier.
    fn final_build_single(&mut self) -> Result<Option<ProgressValues>> {
        if self.state.is_aborting_single() {
            return Ok(None);
        }

        let Some(progress) = self.memory_join()?.final_build()? else {
            return Ok(None);
        };

        if self.state.can_spill()
            && (self.state.is_aborting_single() || self.memory_settings.check_spill())
        {
            let mut inner = self.state.lock();
            if inner.build_final_mode == BuildFinalMode::Single {
                info!(
                    "HashJoin aborting Single BuildFinal due to check_spill, level: {}, build_rows: {}",
                    self.state.level,
                    *self.state.basic.build_rows.deref()
                );
                inner.build_final_mode = BuildFinalMode::AbortingSingle;
                self.state.set_aborting_single(true);
                self.state.basic.clear_build_queue();
            }
            return Ok(None);
        }

        Ok(Some(progress))
    }

    /// Parallel scatter of `transition_queue` into 16 partitions (entered either when `Single`
    /// buckets did not fit or after `AbortingSingle` dropped its partial hash table at the
    /// barrier).
    fn final_build_scattering(&mut self) -> Result<Option<ProgressValues>> {
        if let Ok(chunk) = self.state.transition_queue.pop() {
            check_interrupt()?;
            let progress = ProgressValues {
                rows: chunk.num_rows(),
                bytes: chunk.memory_size(),
            };
            self.partition_build_block(chunk)?;
            self.spill_partitions_on_build_pressure()?;
            return Ok(Some(progress));
        }

        self.flush_and_drop_build_partition_stream()?;
        self.spill_partitions_on_build_pressure()?;
        let drained = self.drain_pending_spill_blocks()?;
        if drained.rows > 0 {
            return Ok(Some(drained));
        }
        if let JoinLocal::Partitioned(local) = &mut self.local {
            local.close_build_writer(&self.state)?;
        }
        Ok(None)
    }

    fn should_spill_next_partition(
        &self,
        inner: &PartitionedInner,
        buckets: &Buckets,
        pid: usize,
    ) -> bool {
        if !self.state.can_spill() {
            return false;
        }
        if self.memory_settings.check_spill() {
            return true;
        }
        let Some(remain) = self.memory_settings.check_spill_remain() else {
            return false;
        };
        if remain <= 0 {
            return true;
        }

        let seg_bytes = buckets.segment_capacity() * std::mem::size_of::<u64>();
        let per_row_bytes = self.per_row_build_overhead_bytes();

        let partition_cost = |slot: &PartitionSlot| -> usize {
            seg_bytes.saturating_add(slot.build_rows.saturating_mul(per_row_bytes))
        };

        let next_cost = partition_cost(&inner.partitions[pid]);

        // Reserve probe spill overhead only when spilling has occurred or all remaining
        // `Collecting` partitions together exceed `remain`.
        let remaining_collecting_cost: usize = inner
            .partitions
            .iter()
            .filter(|s| s.status == PartitionStatus::Collecting)
            .map(partition_cost)
            .sum();

        let will_have_spilled =
            self.state.has_spilled() || (remaining_collecting_cost as isize > remain);

        let probe_reserve = if will_have_spilled {
            let spilled_or_pending = inner
                .partitions
                .iter()
                .filter(|s| s.status != PartitionStatus::InMemory)
                .count();
            match &self.local {
                JoinLocal::Partitioned(local) => {
                    local.probe_spill_reserve_bytes(spilled_or_pending)
                }
                JoinLocal::Flat(_) => 0,
            }
        } else {
            0
        };

        next_cost.saturating_add(probe_reserve) as isize > remain
    }

    /// Build resident partitions one at a time (with all threads collaborating on the active
    /// partition's chunks in parallel) into `Buckets::Partitioned`, checking live memory budget
    /// before starting each next `Collecting` partition.
    fn final_build_partitioned(&mut self) -> Result<Option<ProgressValues>> {
        loop {
            check_interrupt()?;

            enum BuildStep {
                BuildChunk(usize),
                DrainSpills,
                Done,
            }

            let step = {
                let mut inner = self.state.lock();

                if let Some(pid) = inner.active_build_pid {
                    BuildStep::BuildChunk(pid)
                } else if self.state.has_any_pending_spill_blocks() {
                    BuildStep::DrainSpills
                } else {
                    let buckets = self.init_partitioned_hash_table(&inner);
                    // Empty partitions cost no build work. Their probe rows still reach the hash
                    // table, so they get their (empty) bucket segment.
                    for (pid, slot) in inner.partitions.iter_mut().enumerate() {
                        if slot.status == PartitionStatus::Collecting && slot.build_rows == 0 {
                            slot.status = PartitionStatus::InMemory;
                            if let Some(buckets) = buckets {
                                buckets.allocate(pid);
                            }
                        }
                    }

                    let next_collecting = inner
                        .partitions
                        .iter()
                        .enumerate()
                        .filter(|(_, s)| s.status == PartitionStatus::Collecting)
                        .min_by_key(|&(pid, s)| (s.build_bytes, s.build_rows, pid))
                        .map(|(pid, _)| pid);

                    match next_collecting {
                        None => BuildStep::Done,
                        Some(pid) => {
                            let should_spill = match buckets {
                                Some(buckets) => {
                                    self.should_spill_next_partition(&inner, buckets, pid)
                                }
                                None => {
                                    self.state.can_spill() && self.memory_settings.check_spill()
                                }
                            };

                            if should_spill {
                                let mut newly_spilled = 0usize;
                                for p in 0..NUM_PARTITIONS {
                                    if inner.partitions[p].status == PartitionStatus::Collecting {
                                        self.state.mark_partition_spilled(&mut inner, p);
                                        newly_spilled += 1;
                                    }
                                }
                                let total_spilled = inner
                                    .partitions
                                    .iter()
                                    .filter(|s| s.status == PartitionStatus::Spilled)
                                    .count();
                                info!(
                                    "HashJoin build final spill triggered, level: {}, resident: {}, spilled: {} (newly_spilled: {})",
                                    self.state.level,
                                    NUM_PARTITIONS - total_spilled,
                                    total_spilled,
                                    newly_spilled
                                );
                                BuildStep::DrainSpills
                            } else {
                                let slot = &mut inner.partitions[pid];
                                slot.status = PartitionStatus::InMemory;
                                if let Some(buckets) = buckets {
                                    buckets.allocate(pid);
                                }
                                self.state.basic.queue_build_chunks(&slot.chunks);
                                inner.active_build_pid = Some(pid);
                                BuildStep::BuildChunk(pid)
                            }
                        }
                    }
                }
            };

            match step {
                BuildStep::BuildChunk(pid) => {
                    if let Some(progress) = self.memory_join()?.final_build()? {
                        return Ok(Some(progress));
                    }
                    // This partition's build queue is exhausted; allow claiming the next partition.
                    let mut inner = self.state.lock();
                    if inner.active_build_pid == Some(pid) {
                        inner.active_build_pid = None;
                    }
                }
                BuildStep::DrainSpills => {
                    let progress = self.drain_pending_spill_blocks()?;
                    if progress.rows > 0 {
                        return Ok(Some(progress));
                    }
                }
                BuildStep::Done => {
                    let drained = self.drain_pending_spill_blocks()?;
                    if drained.rows > 0 {
                        return Ok(Some(drained));
                    }
                    if let JoinLocal::Partitioned(local) = &mut self.local {
                        local.close_build_writer(&self.state)?;
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn finish_build_final_reenter_locked(&self, inner: &mut PartitionedInner) {
        match inner.build_final_mode {
            BuildFinalMode::AbortingSingle => {
                // Past the Stage::BuildFinal barrier: no thread accesses the aborted Single table.
                // Drop hash_table/arenas/scan_map and move intact build_schema chunks to
                // `transition_queue` for parallel scattering in the next BuildFinal round.
                self.state.transition_to_partitioned_locked(inner);
                self.state.set_aborting_single(false);
                inner.build_final_mode = BuildFinalMode::Scattering;
            }
            BuildFinalMode::Scattering => {
                inner.build_final_mode = BuildFinalMode::PartitionedBuild;
            }
            _ => {}
        }
    }

    /// Create the level's partitioned hash table on the first call, whenever at least one
    /// partition is `Collecting` (including empty ones, so `basic.hash_table` is never `Null` if
    /// any partition stays `InMemory`).
    fn init_partitioned_hash_table(&self, inner: &PartitionedInner) -> Option<&Buckets> {
        let basic = &self.state.basic;
        if matches!(basic.hash_table.deref(), HashJoinHashTable::Null) {
            if inner
                .partitions
                .iter()
                .all(|s| s.status != PartitionStatus::Collecting)
            {
                return None;
            }

            let buckets =
                Buckets::partitioned(self.state.partition_rows(), self.state.shift_bits());
            *basic.hash_table.as_mut() =
                create_hash_table(&self.desc, &self.hash_method_kind, buckets);
        }

        hash_table_buckets(basic.hash_table.deref())
    }

    fn memory_join(&mut self) -> Result<&mut dyn Join> {
        let JoinLocal::Partitioned(local) = &mut self.local else {
            unreachable!("memory_join called in Flat mode");
        };
        if local.memory_join.is_none() {
            local.memory_join = Some(
                self.state
                    .factory
                    .create_memory_join(self.join_type, self.state.lane(self.lane_id).clone())?,
            );
        }
        Ok(local.memory_join.as_mut().unwrap().as_mut())
    }

    fn is_lane_spilled(&mut self) -> bool {
        let JoinLocal::Partitioned(local) = &mut self.local else {
            unreachable!("is_lane_spilled called in Flat mode");
        };
        *local.lane_spilled.get_or_insert_with(|| {
            let inner = self.state.lock();
            inner.partitions[self.lane_id].status == PartitionStatus::Spilled
        })
    }

    /// Which partitions spilled at this level. Fixed once `BuildFinal` is over.
    fn spilled_partitions(&mut self) -> [bool; NUM_PARTITIONS] {
        let JoinLocal::Partitioned(local) = &mut self.local else {
            unreachable!("spilled_partitions called in Flat mode");
        };
        let vec = local.spilled_partitions.get_or_insert_with(|| {
            let inner = self.state.lock();
            inner
                .partitions
                .iter()
                .map(|s| s.status == PartitionStatus::Spilled)
                .collect()
        });
        std::array::from_fn(|pid| vec[pid])
    }

    /// Pre-partitioned `GlobalShuffle` `Stage::Probe`: each thread probes only its own lane's
    /// `Buckets::Single` (or writes directly to Parquet if that lane spilled), with zero hashing,
    /// zero filtering, and zero `probe_partition_stream`.
    fn probe_pre_partitioned(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() {
            return Ok(Box::new(EmptyJoinStream));
        }

        if !self.state.has_spilled() || !self.is_lane_spilled() {
            return self.memory_join()?.probe_block(data);
        }

        if let JoinLocal::Partitioned(local) = &mut self.local {
            local.write_probe_spill(self.lane_id, data)?;
        }
        Ok(Box::new(EmptyJoinStream))
    }

    /// Probe the resident hash table with resident rows of `data`, and buffer rows of spilled
    /// partitions to spill them. When no partition spilled, delegates straight to `memory_join`
    /// with zero overhead.
    fn probe_partitioned(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() {
            return Ok(Box::new(EmptyJoinStream));
        }

        if !self.state.has_spilled() {
            return self.memory_join()?.probe_block(data);
        }

        let spilled = self.spilled_partitions();
        let mut hashes = Vec::with_capacity(data.num_rows());
        get_hashes(
            &self.function_ctx,
            &data,
            &self.desc.probe_keys,
            &self.hash_method_kind,
            self.join_type,
            false,
            &self.desc.is_null_equal,
            &mut hashes,
        )?;
        let shift_bits = self.state.shift_bits();
        let mut spilled_ids = Vec::new();
        let resident = Bitmap::from_trusted_len_iter(hashes.iter().map(|hash| {
            let pid = partition_id(*hash, shift_bits);
            let is_spilled = spilled[pid as usize];
            if is_spilled {
                spilled_ids.push(pid);
            }
            !is_spilled
        }));

        let data = match resident.true_count() {
            0 => {
                self.spill_probe_rows(spilled_ids, data)?;
                return Ok(Box::new(EmptyJoinStream));
            }
            rows if rows == data.num_rows() => data,
            _ => {
                let spilled_data = data.clone().filter_with_bitmap(&!&resident)?;
                self.spill_probe_rows(spilled_ids, spilled_data)?;
                data.filter_with_bitmap(&resident)?
            }
        };
        self.memory_join()?.probe_block(data)
    }

    fn spill_probe_rows(&mut self, pids: Vec<u64>, data: DataBlock) -> Result<()> {
        let JoinLocal::Partitioned(local) = &mut self.local else {
            unreachable!("spill_probe_rows called in Flat mode");
        };
        for (pid, block) in local.probe_stream().partition(pids, data, true) {
            check_interrupt()?;
            local.write_probe_spill(pid, block)?;
        }
        Ok(())
    }

    fn final_probe_partitioned(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
        // Without spilled partitions every row was probed in memory: finish directly.
        if !self.state.has_spilled() {
            return self.memory_join()?.final_probe();
        }

        let stage = match &self.local {
            JoinLocal::Partitioned(local) => local.probe_final_stage,
            JoinLocal::Flat(_) => unreachable!(),
        };

        match stage {
            ProbeFinalStage::FlushProbeBuffers => {
                if let JoinLocal::Partitioned(local) = &mut self.local {
                    if let Some(mut stream) = local.probe_partition_stream.take() {
                        for pid in stream.partition_ids() {
                            check_interrupt()?;
                            if let Some(block) = stream.finalize_partition(pid) {
                                local.write_probe_spill(pid, block)?;
                            }
                        }
                    }
                    local.close_probe_writer(&self.state)?;
                }

                // The resident build rows that no probe row matched are output once all
                // processors have probed, after the barrier that ends this step.
                let next_stage =
                    if self.has_right_final_probe() && self.state.has_resident_build_rows() {
                        ProbeFinalStage::FlushResidentFinal
                    } else {
                        ProbeFinalStage::RestoreBuild
                    };
                if let JoinLocal::Partitioned(local) = &mut self.local {
                    local.probe_final_stage = next_stage;
                }
                Ok(Some(Box::new(EmptyJoinStream)))
            }
            ProbeFinalStage::FlushResidentFinal => {
                if let JoinLocal::Partitioned(local) = &mut self.local {
                    local.probe_final_stage = ProbeFinalStage::RestoreBuild;
                }
                if self.state.pre_partitioned
                    && (self.is_lane_spilled()
                        || *self.state.lanes[self.lane_id].build_rows.deref() == 0)
                {
                    return Ok(Some(Box::new(EmptyJoinStream)));
                }
                match self.memory_join()?.final_probe()? {
                    Some(stream) => Ok(Some(stream)),
                    None => Ok(Some(Box::new(EmptyJoinStream))),
                }
            }
            ProbeFinalStage::RestoreBuild => {
                // Past the barrier after FlushProbeBuffers / FlushResidentFinal, no processor will
                // ever read any resident partition again: free all resident memory before restoring.
                if let JoinLocal::Partitioned(local) = &mut self.local {
                    local.memory_join = None;
                }

                let sub_state = {
                    let mut inner = self.state.lock();
                    if !inner.residents_released {
                        self.state.clear_residents();
                        inner.residents_released = true;
                    }

                    if inner.restore_partition.is_none() && !inner.restore_finished {
                        let mut chosen = None;
                        for (pid, slot) in inner.partitions.iter_mut().enumerate() {
                            if slot.status != PartitionStatus::Spilled {
                                continue;
                            }
                            let has_build = !slot.build_spills.is_empty();
                            let has_probe = !slot.probe_spills.is_empty();
                            if !has_build && !has_probe {
                                continue;
                            }
                            if self.partition_needs_restore(has_build, has_probe) {
                                chosen = Some(pid);
                                break;
                            } else {
                                slot.build_spills.clear();
                                slot.probe_spills.clear();
                            }
                        }

                        if let Some(pid) = chosen {
                            let build_spills =
                                std::mem::take(&mut inner.partitions[pid].build_spills);
                            let probe_spills =
                                std::mem::take(&mut inner.partitions[pid].probe_spills);
                            inner.restore_partition = Some(pid);
                            inner.restore_build_queue = VecDeque::from(build_spills);
                            inner.restore_probe_queue = VecDeque::from(probe_spills);
                            inner.restore_sub_state = Some(
                                self.state
                                    .factory
                                    .create_level_state(self.state.level + 1, false)?,
                            );
                        } else {
                            inner.restore_finished = true;
                        }
                    }

                    if inner.restore_finished {
                        None
                    } else {
                        inner.restore_sub_state.clone()
                    }
                };

                let Some(sub_state) = sub_state else {
                    if let JoinLocal::Partitioned(local) = &mut self.local {
                        local.probe_final_stage = ProbeFinalStage::Done;
                    }
                    return Ok(None);
                };

                let mut sub_join = Box::new(self.state.factory.create_partitioned_join(
                    self.join_type,
                    sub_state,
                    0,
                )?);
                self.restore_build_data(&mut sub_join)?;

                if let JoinLocal::Partitioned(local) = &mut self.local {
                    local.restore_sub_join = Some(sub_join);
                    local.probe_final_stage = ProbeFinalStage::RestoreBuildFinal;
                }
                Ok(Some(Box::new(EmptyJoinStream)))
            }
            ProbeFinalStage::RestoreBuildFinal => {
                if let JoinLocal::Partitioned(local) = &mut self.local {
                    let sub_join = local
                        .restore_sub_join
                        .as_mut()
                        .expect("restore_sub_join must exist in RestoreBuildFinal");
                    let round = sub_join.state.lock().build_final_round;
                    local.restore_build_final_round = round;
                    while sub_join.final_build()?.is_some() {
                        check_interrupt()?;
                    }
                    local.probe_final_stage = ProbeFinalStage::RestoreBuildFinalCheck;
                }
                Ok(Some(Box::new(EmptyJoinStream)))
            }
            ProbeFinalStage::RestoreBuildFinalCheck => {
                // Past the ProbeFinal barrier after RestoreBuildFinal: all threads finished the
                // sub-join's BuildFinal round, so `sub_join`'s `build_final_mode` is frozen.
                // Exactly one thread advances the round counter and performs any re-entry
                // transition (`AbortingSingle -> Scattering` or `Scattering -> PartitionedBuild`);
                // all threads read `last_round_reentered` and stay in lockstep across the next
                // ProbeFinal barrier.
                if let JoinLocal::Partitioned(local) = &mut self.local {
                    let sub_join = local
                        .restore_sub_join
                        .as_mut()
                        .expect("restore_sub_join must exist in RestoreBuildFinalCheck");
                    let reenter = {
                        let mut sub_inner = sub_join.state.lock();
                        if sub_inner.build_final_round == local.restore_build_final_round {
                            let needs = matches!(
                                sub_inner.build_final_mode,
                                BuildFinalMode::AbortingSingle | BuildFinalMode::Scattering
                            );
                            if needs {
                                sub_join.finish_build_final_reenter_locked(&mut sub_inner);
                            }
                            sub_inner.last_round_reentered = needs;
                            sub_inner.build_final_round += 1;
                        }
                        sub_inner.last_round_reentered
                    };
                    local.probe_final_stage = if reenter {
                        ProbeFinalStage::RestoreBuildFinal
                    } else {
                        ProbeFinalStage::RestoreProbe
                    };
                }
                Ok(Some(Box::new(EmptyJoinStream)))
            }
            ProbeFinalStage::RestoreProbe => {
                if let JoinLocal::Partitioned(local) = &mut self.local {
                    local.probe_final_stage = ProbeFinalStage::RestoreProbeFinal;
                }
                Ok(Some(RestoreProbeStream::create(self)))
            }
            ProbeFinalStage::RestoreProbeFinal => {
                // Safety: `self.local` stays alive for the lifetime of the returned stream, and
                // callers always exhaust and drop the returned stream before calling `final_probe`
                // again.
                unsafe {
                    let join_ptr: *mut PartitionedHashJoin = self;
                    let sub_join = match &mut (*join_ptr).local {
                        JoinLocal::Partitioned(local) => local
                            .restore_sub_join
                            .as_mut()
                            .expect("restore_sub_join must exist in RestoreProbeFinal"),
                        JoinLocal::Flat(_) => unreachable!(),
                    };

                    if let Some(stream) = sub_join.final_probe()? {
                        return Ok(Some(stream));
                    }
                }

                if let JoinLocal::Partitioned(local) = &mut self.local {
                    local.restore_sub_join = None;
                    local.probe_final_stage = ProbeFinalStage::RestoreBuild;
                }
                {
                    let mut inner = self.state.lock();
                    inner.restore_partition = None;
                    inner.restore_sub_state = None;
                }
                Ok(Some(Box::new(EmptyJoinStream)))
            }
            ProbeFinalStage::Done => Ok(None),
        }
    }

    fn steal_restore_build_task(&self) -> Option<SpillMetadata> {
        let mut inner = self.state.lock();
        inner.restore_build_queue.pop_front()
    }

    fn steal_restore_probe_task(&self) -> Option<SpillMetadata> {
        let mut inner = self.state.lock();
        inner.restore_probe_queue.pop_front()
    }

    fn restore_build_data(&self, sub_join: &mut PartitionedHashJoin) -> Result<()> {
        let read_settings = match &self.local {
            JoinLocal::Partitioned(local) => local.read_settings,
            JoinLocal::Flat(_) => unreachable!(),
        };

        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let operator = data_operator.spill_operator();

        while let Some(data) = self.steal_restore_build_task() {
            let buffer_pool = SpillsBufferPool::instance();
            let mut reader = buffer_pool.reader(
                operator.clone(),
                data.path,
                self.desc.build_schema.clone(),
                data.row_groups,
                target,
                read_settings,
            )?;

            loop {
                check_interrupt()?;
                let Some(block) = reader.read()? else {
                    break;
                };
                sub_join.add_block(Some(block))?;
            }
        }

        sub_join.add_block(None)
    }
}

impl Join for PartitionedHashJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        if let Some(block) = data.as_ref() {
            self.state.add_build_input_rows(block.num_rows());
        }

        if let JoinLocal::Flat(flat_join) = &mut self.local {
            return flat_join.add_block(data);
        }

        if self.state.pre_partitioned {
            return self.add_block_pre_partitioned(data);
        }

        match data {
            Some(block) => {
                let block = self.filter_null_key_rows_fast(block)?;
                if block.is_empty() {
                    return Ok(());
                }

                if !self.state.is_partitioned() {
                    let squashed = match &mut self.local {
                        JoinLocal::Partitioned(local) => local.squash_block.add_block(block)?,
                        JoinLocal::Flat(_) => unreachable!(),
                    };
                    if let Some(squashed) = squashed {
                        self.push_unpartitioned_chunk(squashed)?;
                    }
                    if self.state.can_spill() && self.memory_settings.check_spill() {
                        self.transition_build_to_partitioned()?;
                    }
                } else {
                    self.flush_local_squash_into_partitions()?;
                    self.partition_build_block(block)?;
                    self.drain_transition_queue_on_build()?;
                    self.spill_partitions_on_build_pressure()?;
                }
            }
            None => {
                let flushed = match &mut self.local {
                    JoinLocal::Partitioned(local) => local.squash_block.finalize()?,
                    JoinLocal::Flat(_) => unreachable!(),
                };
                if let Some(block) = flushed {
                    if !self.state.is_partitioned() {
                        self.push_unpartitioned_chunk(block)?;
                    } else {
                        self.partition_build_block(block)?;
                    }
                }

                if !self.state.is_partitioned()
                    && self.state.can_spill()
                    && self.memory_settings.check_spill()
                {
                    self.transition_build_to_partitioned()?;
                }

                if self.state.is_partitioned() {
                    self.drain_transition_queue_on_build()?;
                    self.flush_and_drop_build_partition_stream()?;
                    self.spill_partitions_on_build_pressure()?;
                    if let JoinLocal::Partitioned(local) = &mut self.local {
                        local.close_build_writer(&self.state)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        match &mut self.local {
            JoinLocal::Flat(flat_join) => flat_join.final_build(),
            JoinLocal::Partitioned(_) if self.state.pre_partitioned => {
                self.final_build_pre_partitioned()
            }
            JoinLocal::Partitioned(_) => self.final_build_unified(),
        }
    }

    fn needs_build_final_reenter(&self) -> bool {
        if self.state.pre_partitioned {
            return false;
        }
        match &self.local {
            JoinLocal::Flat(_) => false,
            JoinLocal::Partitioned(_) => {
                let inner = self.state.lock();
                matches!(
                    inner.build_final_mode,
                    BuildFinalMode::AbortingSingle | BuildFinalMode::Scattering
                )
            }
        }
    }

    fn finish_build_final_reenter(&mut self) -> Result<()> {
        let mut inner = self.state.lock();
        self.finish_build_final_reenter_locked(&mut inner);
        Ok(())
    }

    fn add_runtime_filter_packet(&self, packet: JoinRuntimeFilterPacket) {
        let mut inner = self.state.lock();
        inner.rf_packets.push(packet);
    }

    fn build_runtime_filter(&self) -> Result<JoinRuntimeFilterPacket> {
        let build_rows = self.state.build_input_rows();
        let packets = {
            let mut inner = self.state.lock();
            std::mem::take(&mut inner.rf_packets)
        };

        if self.state.has_spilled() {
            return Ok(JoinRuntimeFilterPacket::disable_all(build_rows));
        }

        let mut packet = merge_join_runtime_filter_packets(
            packets,
            self.inlist_threshold,
            self.bloom_threshold,
            self.min_max_threshold,
        )?;
        packet.build_rows = build_rows;
        Ok(packet)
    }

    fn is_spill_happened(&self) -> bool {
        self.state.has_spilled()
    }

    fn can_skip_probe(&self) -> bool {
        matches!(
            self.join_type,
            JoinType::Inner
                | JoinType::LeftSemi
                | JoinType::Right
                | JoinType::RightSemi
                | JoinType::RightAnti
        )
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        match &mut self.local {
            JoinLocal::Flat(flat_join) => flat_join.probe_block(data),
            JoinLocal::Partitioned(_) if self.state.pre_partitioned => {
                self.probe_pre_partitioned(data)
            }
            JoinLocal::Partitioned(_) => self.probe_partitioned(data),
        }
    }

    fn final_probe(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
        match &mut self.local {
            JoinLocal::Flat(flat_join) => flat_join.final_probe(),
            JoinLocal::Partitioned(_) => self.final_probe_partitioned(),
        }
    }
}

struct RestoreProbeStream<'a> {
    join: &'a mut PartitionedHashJoin,
    spills_reader: Option<SpillsDataReader>,
    joined_stream: Option<Box<dyn JoinStream + 'a>>,
}

impl<'a> RestoreProbeStream<'a> {
    fn create(join: &'a mut PartitionedHashJoin) -> Box<dyn JoinStream + 'a> {
        Box::new(RestoreProbeStream {
            join,
            spills_reader: None,
            joined_stream: None,
        })
    }

    fn next_probe_block(&mut self) -> Result<Option<DataBlock>> {
        let read_settings = match &self.join.local {
            JoinLocal::Partitioned(local) => local.read_settings,
            JoinLocal::Flat(_) => unreachable!(),
        };

        loop {
            check_interrupt()?;
            if self.spills_reader.is_none() {
                while let Some(data) = self.join.steal_restore_probe_task() {
                    if data.row_groups.is_empty() {
                        continue;
                    }
                    let data_operator = DataOperator::instance();
                    let target = SpillTarget::from_storage_params(data_operator.spill_params());
                    let operator = data_operator.spill_operator();
                    let buffer_pool = SpillsBufferPool::instance();
                    let reader = buffer_pool.reader(
                        operator,
                        data.path,
                        self.join.desc.probe_schema.clone(),
                        data.row_groups,
                        target,
                        read_settings,
                    )?;
                    self.spills_reader = Some(reader);
                    break;
                }

                if self.spills_reader.is_none() {
                    return Ok(None);
                }
            }

            if let Some(mut reader) = self.spills_reader.take() {
                if let Some(block) = reader.read()? {
                    self.spills_reader = Some(reader);
                    return Ok(Some(block));
                }
            }
        }
    }
}

impl<'a> JoinStream for RestoreProbeStream<'a> {
    #[allow(clippy::missing_transmute_annotations)]
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if let Some(mut stream) = self.joined_stream.take() {
                if let Some(block) = stream.next()? {
                    self.joined_stream = Some(stream);
                    return Ok(Some(block));
                }
            }

            let Some(block) = self.next_probe_block()? else {
                return Ok(None);
            };

            // Safety: `self.joined_stream` was dropped above before borrowing `sub_join` again.
            unsafe {
                let join: &mut PartitionedHashJoin = &mut *(self.join as *mut _);
                let sub_join = match &mut join.local {
                    JoinLocal::Partitioned(local) => local
                        .restore_sub_join
                        .as_mut()
                        .expect("restore_sub_join must exist in RestoreProbe"),
                    JoinLocal::Flat(_) => unreachable!(),
                };
                self.joined_stream = Some(std::mem::transmute(sub_join.probe_block(block)?));
            }
        }
    }
}

fn hash_entry_size(method: &HashMethodKind) -> usize {
    match method {
        HashMethodKind::KeysU8(_) => std::mem::size_of::<RawEntry<u8>>(),
        HashMethodKind::KeysU16(_) => std::mem::size_of::<RawEntry<u16>>(),
        HashMethodKind::KeysU32(_) => std::mem::size_of::<RawEntry<u32>>(),
        HashMethodKind::KeysU64(_) => std::mem::size_of::<RawEntry<u64>>(),
        HashMethodKind::KeysU128(_) => std::mem::size_of::<RawEntry<u128>>(),
        HashMethodKind::KeysU256(_) => std::mem::size_of::<RawEntry<U256>>(),
        HashMethodKind::SingleBinary(_) | HashMethodKind::Serializer(_) => {
            std::mem::size_of::<StringRawEntry>()
        }
    }
}

fn record_join_spill_progress(
    ctx: &Arc<QueryContext>,
    written: usize,
    row_groups: &[parquet::file::metadata::RowGroupMetaData],
) {
    if written == 0 && row_groups.is_empty() {
        return;
    }

    ctx.get_join_spill_progress().incr(&ProgressValues {
        rows: row_groups
            .iter()
            .map(|row_group| row_group.num_rows() as usize)
            .sum(),
        bytes: written,
    });
}
