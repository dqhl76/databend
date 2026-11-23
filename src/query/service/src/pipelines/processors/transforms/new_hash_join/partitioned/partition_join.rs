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

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
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
    /// Lazily allocated during `Stage::Build` and dropped in `add_block(None)` before `BuildFinal`.
    build_partition_stream: Option<BlockPartitionStream>,
    /// Lazily allocated on first probe and dropped in `FlushProbeBuffers`.
    probe_partition_stream: Option<BlockPartitionStream>,
    probe_actions: Option<[PartitionProbeAction; NUM_PARTITIONS]>,
    probe_final_stage: ProbeFinalStage,
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

    /// Use compact per-partition buffers when all partitions are in memory (avoids accumulating
    /// 16 × `max_block_size` probe buffers per thread) and full block sizes when spilling to
    /// Parquet (keeps spilled probe row groups large).
    fn probe_stream(&mut self, has_spilled: bool) -> &mut BlockPartitionStream {
        self.probe_partition_stream.get_or_insert_with(|| {
            if has_spilled {
                BlockPartitionStream::create(self.block_rows, self.block_bytes, NUM_PARTITIONS)
            } else {
                let compact_rows = (self.block_rows / 4).max(4096);
                let compact_bytes = (self.block_bytes / NUM_PARTITIONS).max(512 * 1024);
                BlockPartitionStream::create(compact_rows, compact_bytes, NUM_PARTITIONS)
            }
        })
    }

    fn write_build_spill(&mut self, pid: usize, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }
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
        if block.is_empty() {
            return Ok(());
        }
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
    /// Used only when spilling/partitioning is disallowed (`CacheScan`, empty keys, or `max_level`).
    Flat(Box<dyn Join>),
    /// 16-way radix partitioned mode: every partition is independently `InMemory` or `Spilled`,
    /// controlled solely by real `memory_settings.check_spill()` checks.
    Partitioned(Box<PartitionedLocal>),
}

/// Unified 16-way radix-partitioned hash join with resident partitions, real `check_spill()`
/// responsiveness during both `Stage::Build` and `Stage::BuildFinal`, and recursive spilling.
pub struct PartitionedHashJoin {
    local: JoinLocal,
    state: Arc<PartitionedJoinState>,
    memory_settings: MemorySettings,
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
        flat_join: Option<Box<dyn Join>>,
        join_type: JoinType,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let inlist_threshold = settings.get_inlist_runtime_filter_threshold()? as usize;
        let bloom_threshold = settings.get_bloom_runtime_filter_threshold()? as usize;
        let min_max_threshold = settings.get_min_max_runtime_filter_threshold()? as usize;

        let local = match flat_join {
            Some(join) => JoinLocal::Flat(join),
            None => JoinLocal::Partitioned(Box::new(PartitionedLocal::create(&ctx)?)),
        };

        Ok(PartitionedHashJoin {
            local,
            state,
            memory_settings,
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
                PartitionStatus::InMemory => {
                    unreachable!("route_build_block called after build stage")
                }
            }
        };

        if spilled && let JoinLocal::Partitioned(local) = &mut self.local {
            local.write_build_spill(pid, block)?;
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

    /// Under real `check_spill()` pressure during `Stage::Build`, spill the largest `Collecting`
    /// partitions one by one until pressure eases (or all 16 partitions are `Spilled`).
    ///
    /// For partitions that just flipped `Collecting -> Spilled`, each processor flushes its
    /// pre-spill `build_partition_stream` buffer at most once (`flushed_on_spill`), so subsequent
    /// rows batch up to full blocks before writing Parquet row groups instead of creating tiny
    /// fragments.
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

    /// Build resident partitions one at a time (with all threads collaborating on the active
    /// partition's chunks in parallel) and check real `memory_settings.check_spill()` before
    /// starting each next `Collecting` partition.
    ///
    /// As soon as `check_spill()` triggers (from this query's own hash table growth OR from any
    /// concurrent query), all remaining `Collecting` partitions immediately flip to `Spilled` and
    /// their untouched chunks are written to Parquet in parallel — releasing their memory before
    /// `Stage::Probe` begins.
    fn final_build_partitioned(&mut self) -> Result<Option<ProgressValues>> {
        loop {
            check_interrupt()?;

            enum BuildStep {
                BuildChunk {
                    pid: usize,
                    basic_state: Arc<BasicHashJoinState>,
                },
                DrainSpills,
                Done,
            }

            let step = {
                let mut inner = self.state.lock();

                if let Some(pid) = inner.active_build_pid {
                    BuildStep::BuildChunk {
                        pid,
                        basic_state: inner.partitions[pid].basic_state.clone(),
                    }
                } else if self.state.has_any_pending_spill_blocks() {
                    BuildStep::DrainSpills
                } else {
                    // Empty partitions cost 0 bytes and become InMemory immediately.
                    for slot in inner.partitions.iter_mut() {
                        if slot.status == PartitionStatus::Collecting && slot.build_rows == 0 {
                            slot.status = PartitionStatus::InMemory;
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
                            if self.state.can_spill() && self.memory_settings.check_spill() {
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
                                    "HashJoin build final spill triggered by check_spill, level: {}, resident: {}, spilled: {} (newly_spilled: {})",
                                    self.state.level,
                                    NUM_PARTITIONS - total_spilled,
                                    total_spilled,
                                    newly_spilled
                                );
                                BuildStep::DrainSpills
                            } else {
                                inner.partitions[pid].status = PartitionStatus::InMemory;
                                inner.active_build_pid = Some(pid);
                                BuildStep::BuildChunk {
                                    pid,
                                    basic_state: inner.partitions[pid].basic_state.clone(),
                                }
                            }
                        }
                    }
                }
            };

            match step {
                BuildStep::BuildChunk { pid, basic_state } => {
                    let join = self.get_or_create_partition_join(pid, basic_state)?;
                    if let Some(progress) = join.final_build()? {
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
                        local.close_build_writers(&self.state)?;
                    }
                    return Ok(None);
                }
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
            PartitionStatus::Collecting => {
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
        if actions.iter().all(|&a| a == PartitionProbeAction::Skip) {
            return Ok(Box::new(EmptyJoinStream));
        }

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

        let has_spilled = self.state.has_spilled();
        let ready = match &mut self.local {
            JoinLocal::Partitioned(local) => local
                .probe_stream(has_spilled)
                .partition(hashes, data, true),
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
                } else if self.state.has_spilled() {
                    ProbeFinalStage::RestoreBuild
                } else {
                    ProbeFinalStage::Done
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
                let next_stage = if self.state.has_spilled() {
                    ProbeFinalStage::RestoreBuild
                } else {
                    ProbeFinalStage::Done
                };
                if let JoinLocal::Partitioned(local) = &mut self.local {
                    local.probe_final_stage = next_stage;
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
                    local.probe_final_stage = ProbeFinalStage::RestoreProbe;
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

        let finished = data.is_none();

        match &mut self.local {
            JoinLocal::Flat(flat_join) => flat_join.add_block(data),
            JoinLocal::Partitioned(_) => {
                if let Some(block) = data {
                    self.partition_build_block(block)?;
                }
                if finished {
                    self.flush_and_drop_build_partition_stream()?;
                }
                self.spill_partitions_on_build_pressure()?;
                if finished && let JoinLocal::Partitioned(local) = &mut self.local {
                    local.close_build_writers(&self.state)?;
                }
                Ok(())
            }
        }
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        match &mut self.local {
            JoinLocal::Flat(flat_join) => flat_join.final_build(),
            JoinLocal::Partitioned(_) => self.final_build_partitioned(),
        }
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
