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
use std::sync::PoisonError;

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::ColumnRef;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::arrow::and_validities;
use databend_common_pipeline::core::check_interrupt;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::traits::Location;
use databend_common_sql::plans::JoinType;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ReadSettings;
use log::info;

use super::partition_state::BYTES_PER_MIB;
use super::partition_state::NUM_PARTITIONS;
use super::partition_state::PartitionStatus;
use super::partition_state::PartitionedJoinState;
use super::partition_state::SpillMetadata;
use super::partition_state::estimate_hash_table_bytes;
use super::partition_state::estimate_probe_headroom;
use super::partition_state::get_partition_id;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::get_hashes;
use crate::pipelines::processors::transforms::merge_join_runtime_filter_packets;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextSpillProgress;
use crate::spillers::Layout;
use crate::spillers::SpillAdapter;
use crate::spillers::SpillTarget;
use crate::spillers::SpillsBufferPool;
use crate::spillers::SpillsDataReader;
use crate::spillers::SpillsDataWriter;

#[derive(Clone, Copy, PartialEq, Eq)]
enum PartitionProbeAction {
    /// Empty resident partition whose probe side can be discarded (e.g. Inner / Right / LeftSemi).
    Skip,
    /// Resident in-memory partition (including empty partition for Left / LeftAnti).
    ProbeInMemory,
    /// Spilled partition whose probe rows are written to Parquet and restored later.
    Spill,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProbeFinalStage {
    FlushProbeBuffers,
    FlushResidentFinal,
    RestoreBuild,
    RestoreBuildFinal,
    RestoreProbe,
    RestoreProbeFinal,
    Done,
}

struct SpillPartitionWriter {
    path: String,
    writer: SpillsDataWriter,
}

impl SpillPartitionWriter {
    fn create(prefix: &str, writer_pool_bytes: usize) -> Result<Self> {
        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let operator = data_operator.spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let path = format!("{}/{}", prefix, GlobalUniq::unique());
        let writer = buffer_pool.writer(operator, path.clone(), writer_pool_bytes, target)?;
        Ok(SpillPartitionWriter { path, writer })
    }
}

struct PartitionedLocal {
    memory_joins: [Option<Box<dyn Join>>; NUM_PARTITIONS],
    build_writers: [Option<SpillPartitionWriter>; NUM_PARTITIONS],
    probe_writers: [Option<SpillPartitionWriter>; NUM_PARTITIONS],
    /// Tracks whether this processor has already performed the one-time local buffer flush when
    /// partition `pid` transitioned from `Collecting` to `Spilled`, so already-spilled partitions
    /// continue batching full blocks in `build_partition_stream` instead of flushing tiny chunks.
    flushed_on_spill: [bool; NUM_PARTITIONS],
    /// Lazily allocated during build/scatter and dropped once build partitioning finishes.
    build_partition_stream: Option<BlockPartitionStream>,
    /// Lazily allocated on first probe and dropped once `FlushProbeBuffers` finishes.
    probe_partition_stream: Option<BlockPartitionStream>,
    probe_actions: Option<[PartitionProbeAction; NUM_PARTITIONS]>,
    probe_final_stage: ProbeFinalStage,
    /// Per-processor flag set during Round 1 of `Stage::BuildFinal` when scattering flat chunks
    /// into partitions, so every processor re-enters `Stage::BuildFinal` together past the barrier.
    need_build_final_reenter: bool,
    restore_sub_join: Option<Box<PartitionedHashJoin>>,
    block_rows: usize,
    block_bytes: usize,
    location_prefix: String,
    writer_pool_bytes: usize,
    read_settings: ReadSettings,
}

impl PartitionedLocal {
    fn create(ctx: &Arc<QueryContext>) -> Result<Self> {
        let settings = ctx.get_settings();
        let block_rows = settings.get_max_block_size()? as usize;
        let block_bytes = settings.get_max_block_bytes()? as usize;
        let location_prefix = ctx.query_id_spill_prefix();
        let writer_pool_bytes = settings
            .get_spill_writer_memory_pool_size_mb()?
            .saturating_mul(BYTES_PER_MIB);

        Ok(PartitionedLocal {
            memory_joins: std::array::from_fn(|_| None),
            build_writers: std::array::from_fn(|_| None),
            probe_writers: std::array::from_fn(|_| None),
            flushed_on_spill: [false; NUM_PARTITIONS],
            build_partition_stream: None,
            probe_partition_stream: None,
            probe_actions: None,
            probe_final_stage: ProbeFinalStage::FlushProbeBuffers,
            need_build_final_reenter: false,
            restore_sub_join: None,
            block_rows,
            block_bytes,
            location_prefix,
            writer_pool_bytes,
            read_settings: ReadSettings::from_settings(&settings)?,
        })
    }

    fn build_stream(&mut self) -> &mut BlockPartitionStream {
        self.build_partition_stream.get_or_insert_with(|| {
            BlockPartitionStream::create(self.block_rows, self.block_bytes, NUM_PARTITIONS)
        })
    }

    fn probe_stream(&mut self) -> &mut BlockPartitionStream {
        self.probe_partition_stream.get_or_insert_with(|| {
            BlockPartitionStream::create(self.block_rows, self.block_bytes, NUM_PARTITIONS)
        })
    }

    fn write_build_spill(&mut self, pid: usize, block: DataBlock) -> Result<()> {
        if self.build_writers[pid].is_none() {
            self.build_writers[pid] = Some(SpillPartitionWriter::create(
                &self.location_prefix,
                self.writer_pool_bytes,
            )?);
        }
        let writer = &mut self.build_writers[pid].as_mut().unwrap().writer;
        writer.write(block)?;
        writer.flush()?;
        Ok(())
    }

    fn write_probe_spill(&mut self, pid: usize, block: DataBlock) -> Result<()> {
        if self.probe_writers[pid].is_none() {
            self.probe_writers[pid] = Some(SpillPartitionWriter::create(
                &self.location_prefix,
                self.writer_pool_bytes,
            )?);
        }
        let writer = &mut self.probe_writers[pid].as_mut().unwrap().writer;
        writer.write(block)?;
        writer.flush()?;
        Ok(())
    }

    fn close_build_writers(&mut self, state: &PartitionedJoinState) -> Result<()> {
        let mut metadata = Vec::new();
        for (pid, slot) in self.build_writers.iter_mut().enumerate() {
            check_interrupt()?;
            if let Some(partition) = slot.take() {
                let (written, row_groups) = partition.writer.close()?;
                state.ctx.add_spill_file(
                    Location::Remote(partition.path.clone()),
                    Layout::Parquet,
                    written,
                );
                record_join_spill_progress(&state.ctx, written, &row_groups);
                if !row_groups.is_empty() {
                    metadata.push((pid, SpillMetadata {
                        path: partition.path,
                        row_groups,
                    }));
                }
            }
        }
        if !metadata.is_empty() {
            let mut inner = state.lock();
            for (pid, meta) in metadata {
                inner.partitions[pid].build_spills.push(meta);
            }
        }
        Ok(())
    }

    fn close_probe_writers(&mut self, state: &PartitionedJoinState) -> Result<()> {
        let mut metadata = Vec::new();
        for (pid, slot) in self.probe_writers.iter_mut().enumerate() {
            check_interrupt()?;
            if let Some(partition) = slot.take() {
                let (written, row_groups) = partition.writer.close()?;
                state.ctx.add_spill_file(
                    Location::Remote(partition.path.clone()),
                    Layout::Parquet,
                    written,
                );
                record_join_spill_progress(&state.ctx, written, &row_groups);
                if !row_groups.is_empty() {
                    metadata.push((pid, SpillMetadata {
                        path: partition.path,
                        row_groups,
                    }));
                }
            }
        }
        if !metadata.is_empty() {
            let mut inner = state.lock();
            for (pid, meta) in metadata {
                inner.partitions[pid].probe_spills.push(meta);
            }
        }
        Ok(())
    }
}

enum JoinLocal {
    /// Zero-overhead flat single-partition path used for all non-spilling joins (and when
    /// spilling is disallowed, e.g. `CacheScan`, empty keys, or `max_level`).
    Flat(Box<dyn Join>),
    /// 16-way radix partitioned mode: each partition is independently `InMemory` (resident) or `Spilled`.
    Partitioned(Box<PartitionedLocal>),
}

/// Unified radix-partitioned hash join with zero-regression flat in-memory path, resident
/// partitions, deterministic `Stage::BuildFinal` partition planning with probe headroom, and
/// recursive multi-level spilling.
pub struct PartitionedHashJoin {
    local: JoinLocal,
    state: Arc<PartitionedJoinState>,
    memory_settings: MemorySettings,
    ctx: Arc<QueryContext>,
    function_ctx: FunctionContext,
    hash_method_kind: HashMethodKind,
    desc: Arc<HashJoinDesc>,
    join_type: JoinType,
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
        flat_join: Box<dyn Join>,
        join_type: JoinType,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let inlist_threshold = settings.get_inlist_runtime_filter_threshold()? as usize;
        let bloom_threshold = settings.get_bloom_runtime_filter_threshold()? as usize;
        let min_max_threshold = settings.get_min_max_runtime_filter_threshold()? as usize;

        Ok(PartitionedHashJoin {
            local: JoinLocal::Flat(flat_join),
            state,
            memory_settings,
            ctx,
            function_ctx,
            hash_method_kind,
            desc,
            join_type,
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

    /// Ensure this processor has switched its local mode to `Partitioned`.
    /// When `flush_flat` is true, any blocks buffered in the local flat squasher are flushed first.
    fn ensure_partitioned_local(&mut self, flush_flat: bool) -> Result<()> {
        if let JoinLocal::Flat(flat_join) = &mut self.local {
            if flush_flat {
                flat_join.add_block(None)?;
            }
            self.state.set_partitioned();
            self.state.move_flat_chunks_to_transition();
            self.local = JoinLocal::Partitioned(Box::new(PartitionedLocal::create(&self.ctx)?));
        }
        Ok(())
    }

    fn partition_build_block(&mut self, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        let mut hashes = Vec::with_capacity(block.num_rows());
        get_hashes(
            &self.function_ctx,
            &block,
            &self.desc.build_keys,
            &self.hash_method_kind,
            self.join_type,
            true,
            &self.desc.is_null_equal,
            &mut hashes,
        )?;
        let shift_bits = self.state.shift_bits();
        for hash in hashes.iter_mut() {
            *hash = get_partition_id(*hash, shift_bits);
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
                    slot.basic_state.push_chunk(block);
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

    fn drain_transition_queue(&mut self) -> Result<()> {
        while let Ok(block) = self.state.transition_queue.pop() {
            check_interrupt()?;
            self.partition_build_block(block)?;
        }
        Ok(())
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

    /// Under memory pressure during `Stage::Build` (or scatter), spill the largest `Collecting`
    /// partitions one by one until pressure eases (or all 16 partitions are `Spilled`).
    ///
    /// For partitions that were already marked `Spilled`, each processor flushes its pre-spill
    /// `build_partition_stream` buffer at most once (`flushed_on_spill`), so subsequent rows batch
    /// up to full blocks before writing Parquet row groups instead of creating tiny fragments.
    fn spill_partitions_on_build_pressure(&mut self) -> Result<()> {
        if !self.state.can_spill() {
            return Ok(());
        }

        // Fast path: once all 16 partitions are Spilled, this thread has already done its one-time
        // flush, and no shared pending blocks remain, let `build_partition_stream` batch normally.
        if self.state.are_all_spilled()
            && !self.state.has_any_pending_spill_blocks()
            && matches!(
                &self.local,
                JoinLocal::Partitioned(local) if local.flushed_on_spill.iter().all(|&f| f)
            )
        {
            return Ok(());
        }

        // One-time flush of this processor's pre-spill local buffers for already-spilled partitions.
        let already_spilled: [bool; NUM_PARTITIONS] = {
            let inner = self.state.lock();
            std::array::from_fn(|pid| inner.partitions[pid].status == PartitionStatus::Spilled)
        };
        if let JoinLocal::Partitioned(local) = &mut self.local {
            for (pid, is_spilled) in already_spilled.iter().enumerate() {
                if *is_spilled && !local.flushed_on_spill[pid] {
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

    /// Inspect collected `chunks` to estimate `(valid_rows, hash_table_bytes)` accurately:
    /// - `valid_rows` excludes rows with NULL values in non-null-equal key columns, because
    ///   `BasicHashJoin::final_build` filters them out before inserting into the entry arena.
    /// - `binary_key_bytes` (for `SingleBinary` / `Serializer` joins with a non-empty build
    ///   projection) sums only the key columns' memory size rather than all payload columns.
    fn estimate_chunks_hash_table_bytes(
        &self,
        basic_state: &BasicHashJoinState,
        total_rows: usize,
    ) -> (usize, usize) {
        if total_rows == 0 {
            return (0, 0);
        }

        let locked = basic_state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);
        let chunks: &[DataBlock] = &basic_state.chunks;
        if chunks.is_empty() {
            return (0, 0);
        }

        // 1. Estimate `valid_rows` (rows surviving NULL-key filtering during `BuildFinal`).
        let mut nullable_col_ids: Vec<usize> = Vec::new();
        let mut has_non_colref_nullable_key = false;
        let mut has_all_null_type = false;

        for (expr, &is_null_equal) in self
            .desc
            .build_keys
            .iter()
            .zip(self.desc.is_null_equal.iter())
        {
            if is_null_equal {
                continue;
            }
            let dt = expr.data_type();
            if dt.is_null() {
                has_all_null_type = true;
                break;
            }
            if dt.is_nullable() {
                match expr {
                    Expr::ColumnRef(ColumnRef { id, .. }) => {
                        if !nullable_col_ids.contains(id) {
                            nullable_col_ids.push(*id);
                        }
                    }
                    _ => {
                        has_non_colref_nullable_key = true;
                    }
                }
            }
        }

        let valid_rows = if has_all_null_type {
            0
        } else if !has_non_colref_nullable_key && nullable_col_ids.is_empty() {
            total_rows
        } else if !has_non_colref_nullable_key {
            let mut valid = 0usize;
            if nullable_col_ids.len() == 1 {
                let col_id = nullable_col_ids[0];
                for chunk in chunks {
                    match chunk.get_by_offset(col_id) {
                        BlockEntry::Const(scalar, _, n) => {
                            if !scalar.is_null() {
                                valid = valid.saturating_add(*n);
                            }
                        }
                        BlockEntry::Column(col) => {
                            let (all_null, validity) = col.validity();
                            if !all_null {
                                valid = valid.saturating_add(match validity {
                                    Some(bm) => chunk.num_rows().saturating_sub(bm.null_count()),
                                    None => chunk.num_rows(),
                                });
                            }
                        }
                    }
                }
            } else {
                for chunk in chunks {
                    let mut combined = None;
                    let mut all_null_chunk = false;
                    for &col_id in &nullable_col_ids {
                        match chunk.get_by_offset(col_id) {
                            BlockEntry::Const(scalar, _, _) => {
                                if scalar.is_null() {
                                    all_null_chunk = true;
                                    break;
                                }
                            }
                            BlockEntry::Column(col) => {
                                let (all_null, validity) = col.validity();
                                if all_null {
                                    all_null_chunk = true;
                                    break;
                                }
                                combined = and_validities(combined, validity.cloned());
                                if combined
                                    .as_ref()
                                    .is_some_and(|bm| bm.null_count() == bm.len())
                                {
                                    all_null_chunk = true;
                                    break;
                                }
                            }
                        }
                    }
                    if !all_null_chunk {
                        valid = valid.saturating_add(match combined {
                            Some(bm) => chunk.num_rows().saturating_sub(bm.null_count()),
                            None => chunk.num_rows(),
                        });
                    }
                }
            }
            valid.min(total_rows)
        } else if let Some(sample) = chunks.iter().find(|c| !c.is_empty()) {
            match self
                .desc
                .build_key(sample, &self.function_ctx)
                .and_then(|entries| {
                    let keys_block = DataBlock::new(entries, sample.num_rows());
                    self.desc.build_valids_by_keys(&keys_block)
                }) {
                Ok(Some(bm)) => {
                    ((total_rows as u128 * bm.true_count() as u128) / sample.num_rows() as u128)
                        as usize
                }
                _ => total_rows,
            }
        } else {
            total_rows
        };

        // 2. Estimate `binary_key_bytes` for `SingleBinary` / `Serializer` methods only.
        let need_binary_key_bytes = valid_rows > 0
            && !self.desc.build_projection.is_empty()
            && matches!(
                self.hash_method_kind,
                HashMethodKind::Serializer(_) | HashMethodKind::SingleBinary(_)
            );

        let binary_key_bytes = if !need_binary_key_bytes {
            0
        } else {
            let mut key_col_ids: Vec<usize> = Vec::with_capacity(self.desc.build_keys.len());
            let mut all_colrefs = true;
            for expr in &self.desc.build_keys {
                match expr {
                    Expr::ColumnRef(ColumnRef { id, .. }) => {
                        if !key_col_ids.contains(id) {
                            key_col_ids.push(*id);
                        }
                    }
                    _ => {
                        all_colrefs = false;
                        break;
                    }
                }
            }

            if all_colrefs {
                let mut key_bytes = 0usize;
                for chunk in chunks {
                    for &col_id in &key_col_ids {
                        let col_bytes = match chunk.get_by_offset(col_id) {
                            BlockEntry::Const(scalar, _, n) => {
                                if scalar.is_null() {
                                    0
                                } else {
                                    scalar.as_ref().memory_size().saturating_mul(*n)
                                }
                            }
                            BlockEntry::Column(col) => col.memory_size(false),
                        };
                        key_bytes = key_bytes.saturating_add(col_bytes);
                    }
                }
                key_bytes
            } else if let Some(sample) = chunks.iter().find(|c| !c.is_empty()) {
                match self.desc.build_key(sample, &self.function_ctx) {
                    Ok(entries) => {
                        let sample_bytes: usize = entries
                            .iter()
                            .map(|entry| match entry {
                                BlockEntry::Const(scalar, _, n) => {
                                    if scalar.is_null() {
                                        0
                                    } else {
                                        scalar.as_ref().memory_size().saturating_mul(*n)
                                    }
                                }
                                BlockEntry::Column(col) => col.memory_size(false),
                            })
                            .sum();
                        ((sample_bytes as u128 * valid_rows as u128) / sample.num_rows() as u128)
                            as usize
                    }
                    Err(_) => chunks.iter().map(|c| c.memory_size()).sum(),
                }
            } else {
                0
            }
        };

        let est = estimate_hash_table_bytes(
            total_rows,
            valid_rows,
            binary_key_bytes,
            &self.hash_method_kind,
            self.join_type,
        );
        (valid_rows, est)
    }

    /// Upfront admission check at the start of `Stage::BuildFinal` when the join stayed in `Flat`
    /// mode throughout `Stage::Build`.
    ///
    /// - If the flat hash table fits in remaining memory (accounting for concurrent in-flight flat
    ///   builds across other joins), reserve its estimate and stay 100% on the `Flat` path — zero
    ///   partitioning overhead, zero memory inflation.
    /// - Otherwise, switch to `Partitioned` mode and enter Round 1 of `Stage::BuildFinal` to
    ///   scatter the untouched flat chunks into 16 partitions before the stage barrier.
    fn check_flat_final_build_admission(&mut self) -> Result<bool> {
        if !self.state.can_spill() {
            return Ok(false);
        }

        let mut inner = self.state.lock();
        if self.state.is_partitioned() {
            return Ok(true);
        }
        if inner.flat_final_build_checked {
            return Ok(false);
        }

        let rows = *self.state.flat_basic.build_rows;
        if rows == 0 {
            inner.flat_final_build_checked = true;
            return Ok(false);
        }

        let (valid_rows, est) = self.estimate_chunks_hash_table_bytes(&self.state.flat_basic, rows);
        let global_in_flight = PartitionedJoinState::global_in_flight_flat_build_bytes();
        let need_spill = self.memory_settings.check_spill()
            || self
                .memory_settings
                .check_spill_remain()
                .is_some_and(|remain| remain < (global_in_flight.saturating_add(est)) as isize);

        inner.flat_final_build_checked = true;
        if !need_spill {
            self.state.reserve_flat_build(est);
            return Ok(false);
        }

        info!(
            "HashJoin flat build final admission switching to partitioned mode, level: {}, rows: {}, valid_rows: {}, estimated_bytes: {}, global_in_flight: {}",
            self.state.level, rows, valid_rows, est, global_in_flight
        );
        inner.scattering_in_build_final = true;
        self.state.move_flat_chunks_to_transition();
        self.state.set_partitioned();
        Ok(true)
    }

    /// Deterministically plan which `Collecting` partitions remain resident (`InMemory`) and which
    /// flip to `Spilled` (largest first) at the start of `BuildFinal`, accounting for:
    /// - hash table memory of kept resident partitions,
    /// - build chunk memory freed by spilling victim partitions, and
    /// - `Stage::Probe` headroom (partition buffers + Parquet probe writers + resident contexts).
    fn plan_build_final_partitions(&self) {
        let mut inner = self.state.lock();
        if inner.build_final_planned {
            return;
        }
        inner.scattering_in_build_final = false;
        inner.build_final_planned = true;

        // Empty partitions cost 0 bytes and become InMemory immediately.
        for slot in inner.partitions.iter_mut() {
            if slot.status == PartitionStatus::Collecting && slot.build_rows == 0 {
                slot.status = PartitionStatus::InMemory;
            }
        }

        if !self.state.can_spill() {
            return;
        }

        // Collect non-empty Collecting partitions sorted smallest-first (tie-broken by pid for
        // deterministic decisions across runs).
        let mut candidates: Vec<(usize, usize, usize, usize)> = inner
            .partitions
            .iter()
            .enumerate()
            .filter(|(_, s)| s.status == PartitionStatus::Collecting)
            .map(|(pid, s)| {
                let (_, est) = self.estimate_chunks_hash_table_bytes(&s.basic_state, s.build_rows);
                (pid, s.build_rows, s.build_bytes, est)
            })
            .collect();

        if candidates.is_empty() {
            let spilled_count = inner
                .partitions
                .iter()
                .filter(|s| s.status == PartitionStatus::Spilled)
                .count();
            info!(
                "HashJoin build final plan, level: {}, resident: {}, spilled: {}",
                self.state.level,
                NUM_PARTITIONS - spilled_count,
                spilled_count
            );
            return;
        }

        candidates.sort_by_key(|&(pid, rows, bytes, est)| (est, bytes, rows, pid));

        let already_spilled = inner
            .partitions
            .iter()
            .filter(|s| s.status == PartitionStatus::Spilled)
            .count();

        let remain = self.memory_settings.check_spill_remain();
        let force_spill = self
            .ctx
            .get_settings()
            .get_force_join_data_spill()
            .unwrap_or(false);

        let keep_count = if force_spill {
            0
        } else if let Some(remain_bytes) = remain {
            let settings = self.ctx.get_settings();
            let max_threads = settings.get_max_threads().unwrap_or(1) as usize;
            let max_block_size = settings.get_max_block_size().unwrap_or(65536) as usize;
            let probe_cols = self.desc.probe_schema.fields().len();
            let total_candidate_bytes: usize = candidates.iter().map(|c| c.2).sum();

            // Find the largest `k` (keeping the `k` smallest partitions resident) whose net memory
            // requirement fits within `remain_bytes`.
            let mut best_k = 0usize;
            let mut resident_est = 0usize;
            let mut resident_chunk_bytes = 0usize;
            for (idx, &(_, _, bytes, est)) in candidates.iter().enumerate() {
                let k = idx + 1;
                resident_est = resident_est.saturating_add(est);
                resident_chunk_bytes = resident_chunk_bytes.saturating_add(bytes);
                let spilled_count = already_spilled + (candidates.len() - k);
                let spilled_chunk_freed =
                    total_candidate_bytes.saturating_sub(resident_chunk_bytes);
                let probe_headroom = estimate_probe_headroom(
                    max_threads,
                    max_block_size,
                    probe_cols,
                    k,
                    spilled_count,
                );
                let net_needed = (resident_est.saturating_add(probe_headroom) as isize)
                    - (spilled_chunk_freed as isize);
                if net_needed <= remain_bytes {
                    best_k = k;
                } else {
                    break;
                }
            }
            best_k
        } else {
            candidates.len()
        };

        let mut spilled_now = 0usize;
        for &(pid, rows, bytes, _) in &candidates[keep_count..] {
            info!(
                "HashJoin spilling partition {} during build final, level: {}, rows: {}, bytes: {}",
                pid, self.state.level, rows, bytes
            );
            self.state.mark_partition_spilled(&mut inner, pid);
            spilled_now += 1;
        }

        let total_spilled = already_spilled + spilled_now;
        info!(
            "HashJoin build final plan, level: {}, resident: {}, spilled: {} (newly_spilled: {}, remain: {:?})",
            self.state.level,
            NUM_PARTITIONS - total_spilled,
            total_spilled,
            spilled_now,
            remain
        );
    }

    /// Execute one step of `Stage::BuildFinal` in `Partitioned` mode:
    /// - If Round 1 scatter from `Flat` mode is active, drain `transition_queue`, flush and drop
    ///   `build_partition_stream`, mark `local.need_build_final_reenter = true`, and return `None`
    ///   so all processors synchronize at the `Stage::BuildFinal` barrier before Round 2.
    /// - Otherwise:
    ///   1. Run deterministic partition planning (once).
    ///   2. Drain all `pending_spill_blocks` and close `build_writers` FIRST (freeing spilled
    ///      chunks and writer buffers before allocating resident hash tables!).
    ///   3. Claim remaining `Collecting` partitions one by one to build independent hash tables
    ///      (with a dynamic `check_spill()` safety check in case external memory pressure spikes).
    fn final_build_partitioned(&mut self) -> Result<Option<ProgressValues>> {
        let is_scattering = {
            let inner = self.state.lock();
            inner.scattering_in_build_final
        };

        if is_scattering {
            self.drain_transition_queue()?;
            self.flush_and_drop_build_partition_stream()?;
            self.spill_partitions_on_build_pressure()?;
            if let JoinLocal::Partitioned(local) = &mut self.local {
                local.need_build_final_reenter = true;
            }
            return Ok(None);
        }

        // Release `build_partition_stream` buffers if any remain from `Stage::Build`.
        self.flush_and_drop_build_partition_stream()?;

        // Step 1: deterministic resident vs spilled plan (runs once under lock).
        self.plan_build_final_partitions();

        // Step 2: drain all planned spilled build chunks and close build writers FIRST so their
        // chunks and Parquet writer buffers are released BEFORE building resident hash tables.
        let drained = self.drain_pending_spill_blocks()?;
        if drained.rows > 0 {
            return Ok(Some(drained));
        }
        if let JoinLocal::Partitioned(local) = &mut self.local {
            local.close_build_writers(&self.state)?;
        }

        // Step 3: claim next planned resident `Collecting` partition to build (with dynamic spill
        // safety guard in case concurrent queries eat memory while resident tables are building).
        enum BuildAction {
            Build {
                pid: usize,
                est: usize,
                basic_state: Arc<BasicHashJoinState>,
            },
            Spilled,
            Done,
        }

        let action = {
            let mut inner = self.state.lock();

            let smallest = inner
                .partitions
                .iter()
                .enumerate()
                .filter(|(_, s)| s.status == PartitionStatus::Collecting)
                .min_by_key(|&(pid, s)| (s.build_bytes, s.build_rows, pid))
                .map(|(pid, _)| pid);

            match smallest {
                None => BuildAction::Done,
                Some(smallest_pid) => {
                    let (_, est) = self.estimate_chunks_hash_table_bytes(
                        &inner.partitions[smallest_pid].basic_state,
                        inner.partitions[smallest_pid].build_rows,
                    );

                    // Dynamic guard: only spill an already-planned resident partition if real
                    // memory pressure has since exceeded remaining headroom.
                    let over_limit =
                        self.state.can_spill()
                            && (self.memory_settings.check_spill()
                                || self.memory_settings.check_spill_remain().is_some_and(
                                    |remain| remain < (inner.in_flight_build_bytes + est) as isize,
                                ));

                    if over_limit {
                        let largest_pid = inner
                            .partitions
                            .iter()
                            .enumerate()
                            .filter(|(_, s)| s.status == PartitionStatus::Collecting)
                            .max_by_key(|&(pid, s)| (s.build_bytes, s.build_rows, pid))
                            .map(|(pid, _)| pid)
                            .unwrap_or(smallest_pid);

                        let rows = inner.partitions[largest_pid].build_rows;
                        let bytes = inner.partitions[largest_pid].build_bytes;
                        info!(
                            "HashJoin spilling partition {} dynamically during build final, level: {}, rows: {}, bytes: {}",
                            largest_pid, self.state.level, rows, bytes
                        );
                        self.state.mark_partition_spilled(&mut inner, largest_pid);
                        BuildAction::Spilled
                    } else {
                        inner.in_flight_build_bytes += est;
                        inner.partitions[smallest_pid].status = PartitionStatus::Building;
                        let basic_state = inner.partitions[smallest_pid].basic_state.clone();
                        BuildAction::Build {
                            pid: smallest_pid,
                            est,
                            basic_state,
                        }
                    }
                }
            }
        };

        match action {
            BuildAction::Build {
                pid,
                est,
                basic_state,
            } => {
                let build_res = (|| -> Result<ProgressValues> {
                    let join = self.get_or_create_partition_join(pid, basic_state)?;
                    let mut progress = ProgressValues::default();
                    while let Some(p) = join.final_build()? {
                        check_interrupt()?;
                        progress.rows += p.rows;
                        progress.bytes += p.bytes;
                    }
                    Ok(progress)
                })();

                let mut inner = self.state.lock();
                inner.in_flight_build_bytes = inner.in_flight_build_bytes.saturating_sub(est);
                match build_res {
                    Ok(progress) => {
                        inner.partitions[pid].status = PartitionStatus::InMemory;
                        Ok(Some(progress))
                    }
                    Err(err) => Err(err),
                }
            }
            BuildAction::Spilled => {
                let progress = self.drain_pending_spill_blocks()?;
                if let JoinLocal::Partitioned(local) = &mut self.local {
                    local.close_build_writers(&self.state)?;
                }
                Ok(Some(progress))
            }
            BuildAction::Done => {
                let drained = self.drain_pending_spill_blocks()?;
                if drained.rows > 0 {
                    return Ok(Some(drained));
                }
                if let JoinLocal::Partitioned(local) = &mut self.local {
                    local.close_build_writers(&self.state)?;
                }
                Ok(None)
            }
        }
    }

    fn get_or_create_partition_join(
        &mut self,
        pid: usize,
        basic_state: Arc<BasicHashJoinState>,
    ) -> Result<&mut dyn Join> {
        let JoinLocal::Partitioned(local) = &mut self.local else {
            unreachable!("get_or_create_partition_join called in Flat mode");
        };
        if local.memory_joins[pid].is_none() {
            local.memory_joins[pid] = Some(
                self.state
                    .factory
                    .create_memory_join(self.join_type, basic_state)?,
            );
        }
        Ok(local.memory_joins[pid].as_mut().unwrap().as_mut())
    }

    fn ensure_probe_actions(&mut self) -> [PartitionProbeAction; NUM_PARTITIONS] {
        let can_skip = self.can_skip_probe();
        let JoinLocal::Partitioned(local) = &mut self.local else {
            unreachable!("ensure_probe_actions called in Flat mode");
        };
        if let Some(actions) = local.probe_actions {
            return actions;
        }

        let inner = self.state.lock();
        let actions = std::array::from_fn(|pid| match inner.partitions[pid].status {
            PartitionStatus::Spilled => PartitionProbeAction::Spill,
            PartitionStatus::InMemory => {
                if inner.partitions[pid].build_rows == 0 && can_skip {
                    PartitionProbeAction::Skip
                } else {
                    PartitionProbeAction::ProbeInMemory
                }
            }
            PartitionStatus::Collecting | PartitionStatus::Building => {
                unreachable!("partition must be InMemory or Spilled when probing")
            }
        });
        local.probe_actions = Some(actions);
        actions
    }

    fn has_any_nonempty_resident(&self) -> bool {
        let inner = self.state.lock();
        inner
            .partitions
            .iter()
            .any(|s| s.status == PartitionStatus::InMemory && s.build_rows > 0)
    }

    fn probe_partitioned(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() {
            return Ok(Box::new(EmptyJoinStream));
        }

        let actions = self.ensure_probe_actions();

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
        for hash in hashes.iter_mut() {
            *hash = get_partition_id(*hash, shift_bits);
        }

        let ready = match &mut self.local {
            JoinLocal::Partitioned(local) => local.probe_stream().partition(hashes, data, true),
            JoinLocal::Flat(_) => unreachable!(),
        };

        let mut pending = VecDeque::new();
        if let JoinLocal::Partitioned(local) = &mut self.local {
            for (pid, block) in ready {
                check_interrupt()?;
                match actions[pid] {
                    PartitionProbeAction::Skip => {}
                    PartitionProbeAction::Spill => local.write_probe_spill(pid, block)?,
                    PartitionProbeAction::ProbeInMemory => pending.push_back((pid, block)),
                }
            }
        }

        if pending.is_empty() {
            return Ok(Box::new(EmptyJoinStream));
        }

        Ok(PartitionedProbeStream::create(self, pending))
    }

    fn final_probe_partitioned(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
        let stage = match &self.local {
            JoinLocal::Partitioned(local) => local.probe_final_stage,
            JoinLocal::Flat(_) => unreachable!(),
        };

        match stage {
            ProbeFinalStage::FlushProbeBuffers => {
                let actions = self.ensure_probe_actions();
                let mut pending = VecDeque::new();

                if let JoinLocal::Partitioned(local) = &mut self.local {
                    if let Some(mut stream) = local.probe_partition_stream.take() {
                        let ids = stream.partition_ids();
                        for pid in ids {
                            check_interrupt()?;
                            if let Some(block) = stream.finalize_partition(pid) {
                                match actions[pid] {
                                    PartitionProbeAction::Skip => {}
                                    PartitionProbeAction::Spill => {
                                        local.write_probe_spill(pid, block)?;
                                    }
                                    PartitionProbeAction::ProbeInMemory => {
                                        pending.push_back((pid, block));
                                    }
                                }
                            }
                        }
                    }
                    local.close_probe_writers(&self.state)?;
                }

                let next_stage = if self.has_right_final_probe() && self.has_any_nonempty_resident()
                {
                    ProbeFinalStage::FlushResidentFinal
                } else {
                    ProbeFinalStage::RestoreBuild
                };

                if let JoinLocal::Partitioned(local) = &mut self.local {
                    local.probe_final_stage = next_stage;
                }

                if pending.is_empty() {
                    Ok(Some(Box::new(EmptyJoinStream)))
                } else {
                    Ok(Some(PartitionedProbeStream::create(self, pending)))
                }
            }
            ProbeFinalStage::FlushResidentFinal => {
                if let JoinLocal::Partitioned(local) = &mut self.local {
                    local.probe_final_stage = ProbeFinalStage::RestoreBuild;
                }
                Ok(Some(ResidentFinalProbeStream::create(self)))
            }
            ProbeFinalStage::RestoreBuild => {
                // Past the barrier after FlushProbeBuffers / FlushResidentFinal, no processor will
                // ever read any resident partition again: free all resident memory before restoring.
                if let JoinLocal::Partitioned(local) = &mut self.local {
                    for slot in local.memory_joins.iter_mut() {
                        *slot = None;
                    }
                }

                let sub_state = {
                    let mut inner = self.state.lock();
                    if !inner.residents_released {
                        for slot in inner.partitions.iter_mut() {
                            if slot.status == PartitionStatus::InMemory {
                                slot.basic_state.clear();
                            }
                        }
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

                let mut sub_join = Box::new(
                    self.state
                        .factory
                        .create_partitioned_join(self.join_type, sub_state)?,
                );
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
                    while sub_join.final_build()?.is_some() {
                        check_interrupt()?;
                    }
                    if sub_join.needs_build_final_reenter() {
                        // Stay in RestoreBuildFinal so all processors pass the TransformHashJoin
                        // barrier before running Round 2 of `sub_join.final_build()`.
                        sub_join.finish_build_final_reenter()?;
                    } else {
                        local.probe_final_stage = ProbeFinalStage::RestoreProbe;
                    }
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

        // 1. If another processor already switched to Partitioned mode, follow it.
        if self.state.is_partitioned() && matches!(self.local, JoinLocal::Flat(_)) {
            self.ensure_partitioned_local(true)?;
        }

        let finished = data.is_none();

        match &mut self.local {
            JoinLocal::Flat(flat_join) => {
                flat_join.add_block(data)?;

                // Stay 100% on the Flat path during Stage::Build unless memory pressure triggers
                // spilling (or another processor has already switched to Partitioned mode).
                let should_switch = self.state.can_spill()
                    && (self.state.is_partitioned()
                        || (self.state.build_input_rows() > 0
                            && self.memory_settings.check_spill()));

                if should_switch {
                    self.ensure_partitioned_local(!finished)?;
                    self.drain_transition_queue()?;
                    if finished {
                        self.flush_and_drop_build_partition_stream()?;
                    }
                    self.spill_partitions_on_build_pressure()?;
                }
                Ok(())
            }
            JoinLocal::Partitioned(_) => {
                if let Some(block) = data {
                    self.partition_build_block(block)?;
                }
                self.drain_transition_queue()?;
                if finished {
                    self.flush_and_drop_build_partition_stream()?;
                }
                self.spill_partitions_on_build_pressure()
            }
        }
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        // Follow any switch to Partitioned mode made during Stage::Build.
        if self.state.is_partitioned() && matches!(self.local, JoinLocal::Flat(_)) {
            self.ensure_partitioned_local(false)?;
        }

        // If still in Flat mode, check upfront whether the flat hash table fits in memory.
        if matches!(self.local, JoinLocal::Flat(_)) && self.check_flat_final_build_admission()? {
            self.ensure_partitioned_local(false)?;
        }

        match &mut self.local {
            JoinLocal::Flat(flat_join) => {
                let res = flat_join.final_build()?;
                if res.is_none() {
                    self.state.release_flat_build_reservation();
                }
                Ok(res)
            }
            JoinLocal::Partitioned(_) => self.final_build_partitioned(),
        }
    }

    fn needs_build_final_reenter(&self) -> bool {
        match &self.local {
            JoinLocal::Flat(_) => false,
            JoinLocal::Partitioned(local) => local.need_build_final_reenter,
        }
    }

    fn finish_build_final_reenter(&mut self) -> Result<()> {
        if let JoinLocal::Partitioned(local) = &mut self.local {
            local.need_build_final_reenter = false;
        }
        // Safe to clear here: `needs_build_final_reenter()` only reads the per-processor
        // `local.need_build_final_reenter` flag, and clearing `scattering_in_build_final` is
        // idempotent across all processors before Round 2 of `Stage::BuildFinal`.
        self.state.lock().scattering_in_build_final = false;
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

struct PartitionedProbeStream<'a> {
    join: &'a mut PartitionedHashJoin,
    pending: VecDeque<(usize, DataBlock)>,
    current: Option<Box<dyn JoinStream + 'a>>,
}

impl<'a> PartitionedProbeStream<'a> {
    fn create(
        join: &'a mut PartitionedHashJoin,
        pending: VecDeque<(usize, DataBlock)>,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(PartitionedProbeStream {
            join,
            pending,
            current: None,
        })
    }
}

impl<'a> JoinStream for PartitionedProbeStream<'a> {
    #[allow(clippy::missing_transmute_annotations)]
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if let Some(mut stream) = self.current.take() {
                if let Some(block) = stream.next()? {
                    self.current = Some(stream);
                    return Ok(Some(block));
                }
            }

            let Some((pid, block)) = self.pending.pop_front() else {
                return Ok(None);
            };

            let basic_state = {
                let inner = self.join.state.lock();
                inner.partitions[pid].basic_state.clone()
            };

            // Safety: `self.current` was just dropped above, so no mutable reference into
            // `self.join` aliases when creating the next partition's probe stream.
            unsafe {
                let join: &mut PartitionedHashJoin = &mut *(self.join as *mut _);
                let mem_join = join.get_or_create_partition_join(pid, basic_state)?;
                self.current = Some(std::mem::transmute(mem_join.probe_block(block)?));
            }
        }
    }
}

struct ResidentFinalProbeStream<'a> {
    join: &'a mut PartitionedHashJoin,
    next_pid: usize,
    current: Option<Box<dyn JoinStream + 'a>>,
}

impl<'a> ResidentFinalProbeStream<'a> {
    fn create(join: &'a mut PartitionedHashJoin) -> Box<dyn JoinStream + 'a> {
        Box::new(ResidentFinalProbeStream {
            join,
            next_pid: 0,
            current: None,
        })
    }
}

impl<'a> JoinStream for ResidentFinalProbeStream<'a> {
    #[allow(clippy::missing_transmute_annotations)]
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if let Some(mut stream) = self.current.take() {
                if let Some(block) = stream.next()? {
                    self.current = Some(stream);
                    return Ok(Some(block));
                }
            }

            while self.next_pid < NUM_PARTITIONS {
                let pid = self.next_pid;
                self.next_pid += 1;

                let basic_state = {
                    let inner = self.join.state.lock();
                    let slot = &inner.partitions[pid];
                    if slot.status == PartitionStatus::InMemory && slot.build_rows > 0 {
                        Some(slot.basic_state.clone())
                    } else {
                        None
                    }
                };

                let Some(basic_state) = basic_state else {
                    continue;
                };

                // Safety: `self.current` was dropped above before borrowing `self.join` again.
                unsafe {
                    let join: &mut PartitionedHashJoin = &mut *(self.join as *mut _);
                    let mem_join = join.get_or_create_partition_join(pid, basic_state)?;
                    if let Some(stream) = mem_join.final_probe()? {
                        self.current = Some(std::mem::transmute(stream));
                        break;
                    }
                }
            }

            if self.current.is_none() {
                return Ok(None);
            }
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
