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

use std::mem;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_pipeline_transforms::traits::Location;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ReadSettings;
use log::debug;
use log::info;

use crate::pipelines::processors::transforms::aggregator::SerializedPayload;
use crate::pipelines::processors::transforms::aggregator::SpilledPayload;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextSpillProgress;
use crate::spillers::Layout;
use crate::spillers::SpillAdapter;
use crate::spillers::SpillTarget;
use crate::spillers::SpilledSegment;
use crate::spillers::SpillsBufferPool;
use crate::spillers::SpillsSegmentWriter;
use crate::spillers::read_segment;

const BYTES_PER_MIB: usize = 1024 * 1024;

struct PayloadWriter {
    path: String,
    writer: SpillsSegmentWriter,
}

impl PayloadWriter {
    fn try_create(prefix: &str, writer_pool_bytes: usize) -> Result<Self> {
        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let operator = data_operator.spill_operator();
        let buffer_pool = SpillsBufferPool::instance();
        let file_path = format!("{}/{}", prefix, GlobalUniq::unique());
        let writer = SpillsSegmentWriter::try_create(
            &buffer_pool,
            operator,
            file_path.clone(),
            writer_pool_bytes,
            target,
        )?;

        Ok(PayloadWriter {
            path: file_path,
            writer,
        })
    }

    /// Writes `blocks` as one segment.
    fn write_row_group(&mut self, blocks: Vec<DataBlock>) -> Result<()> {
        self.writer.write_segment(blocks)
    }

    fn close(self) -> Result<(String, usize, Vec<SpilledSegment>)> {
        let (bytes_written, segments) = self.writer.close()?;
        Ok((self.path, bytes_written, segments))
    }
}

#[derive(Default)]
struct WriteStats {
    rows: usize,
    bytes: usize,
    count: usize,
    elapsed: Duration,
}

impl WriteStats {
    fn accumulate(&mut self, elapsed: Duration) {
        self.count += 1;
        self.elapsed += elapsed;
    }

    fn add_rows(&mut self, rows: usize) {
        self.rows += rows;
    }

    fn add_bytes(&mut self, bytes: usize) {
        self.bytes += bytes;
    }

    fn take(&mut self) -> Self {
        mem::take(self)
    }
}

struct AggregatePayloadWriters {
    spill_prefix: String,
    partition_count: usize,
    writer_pool_bytes: usize,
    writers: Vec<Option<PayloadWriter>>,
    write_stats: WriteStats,
    ctx: Arc<QueryContext>,
}

impl AggregatePayloadWriters {
    pub fn create(
        prefix: &str,
        partition_count: usize,
        writer_pool_bytes: usize,
        ctx: Arc<QueryContext>,
    ) -> Self {
        AggregatePayloadWriters {
            spill_prefix: prefix.to_string(),
            partition_count,
            writer_pool_bytes,
            writers: Self::empty_writers(partition_count),
            write_stats: WriteStats::default(),
            ctx,
        }
    }

    fn empty_writers(partition_count: usize) -> Vec<Option<PayloadWriter>> {
        std::iter::repeat_with(|| None)
            .take(partition_count)
            .collect::<Vec<_>>()
    }

    fn ensure_writer(&mut self, bucket: usize) -> Result<&mut PayloadWriter> {
        if self.writers[bucket].is_none() {
            self.writers[bucket] = Some(PayloadWriter::try_create(
                &self.spill_prefix,
                self.writer_pool_bytes,
            )?);
        }

        Ok(self.writers[bucket].as_mut().unwrap())
    }

    fn write_row_group(&mut self, bucket: usize, blocks: Vec<DataBlock>) -> Result<()> {
        let start = Instant::now();
        self.ensure_writer(bucket)?.write_row_group(blocks)?;
        self.write_stats.accumulate(start.elapsed());
        Ok(())
    }

    fn has_writer(&self) -> bool {
        self.writers.iter().any(Option::is_some)
    }

    pub fn finalize(&mut self) -> Result<Vec<SpilledPayload>> {
        let writers = mem::replace(&mut self.writers, Self::empty_writers(self.partition_count));

        if writers.iter().all(|writer| writer.is_none()) {
            return Ok(Vec::new());
        }

        let mut spilled_payloads = Vec::new();
        for (partition_id, writer) in writers.into_iter().enumerate() {
            let Some(writer) = writer else {
                continue;
            };

            let (path, written_size, segments) = writer.close()?;

            if written_size != 0 {
                info!(
                    "Write aggregate spill finished: (bucket: {}, location: {}, bytes: {}, rows: {}, batch_count: {})",
                    partition_id,
                    path,
                    written_size,
                    segments.iter().map(|segment| segment.rows).sum::<usize>(),
                    segments.len()
                );
            }

            self.ctx.add_spill_file(
                Location::Remote(path.clone()),
                Layout::Aggregate,
                written_size,
            );

            if segments.is_empty() {
                continue;
            }

            if written_size > 0 {
                self.write_stats.add_bytes(written_size);
            }

            for segment in segments {
                self.write_stats.add_rows(segment.rows);
                spilled_payloads.push(SpilledPayload {
                    bucket: partition_id as isize,
                    location: path.clone(),
                    segment,
                });
            }
        }

        let stats = self.write_stats.take();
        flush_write_profile(&self.ctx, stats);

        Ok(spilled_payloads)
    }
}

/// Blocks of one bucket waiting to fill a row group.
#[derive(Default)]
struct PendingBlocks {
    blocks: Vec<DataBlock>,
    bytes: usize,
}

/// Spills the buckets of a final aggregate table, and restores them.
///
/// A bucket's blocks are kept as they come, without copying them, until they hold
/// `buffer_bytes`; they are then written as one row group. The blocks of a round that never
/// filled a row group are not written at all, see [`Self::take_pending_if_unspilled`].
pub struct AggregateSpiller {
    read_setting: ReadSettings,
    /// The types of the spilled columns: states, then group keys.
    types: Vec<DataType>,
    buffer_bytes: usize,
    pending: Vec<PendingBlocks>,
    payload_writers: AggregatePayloadWriters,
}

impl AggregateSpiller {
    /// Take the blocks of every bucket only if this round has not written any of them. Once a
    /// writer exists, the pending blocks must stay with the spilled ones for merging. A
    /// bucket's blocks may repeat groups.
    pub(super) fn take_pending_if_unspilled(&mut self) -> Option<Vec<(usize, Vec<DataBlock>)>> {
        if self.payload_writers.has_writer() {
            return None;
        }
        Some(
            self.pending
                .iter_mut()
                .map(|pending| mem::take(pending).blocks)
                .enumerate()
                .filter(|(_, blocks)| !blocks.is_empty())
                .collect(),
        )
    }

    pub fn try_create(
        ctx: Arc<QueryContext>,
        partition_count: usize,
        data_schema: DataSchemaRef,
        buffer_bytes: usize,
    ) -> Result<Self> {
        let table_ctx: Arc<dyn TableContext> = ctx.clone();
        let read_setting = ReadSettings::from_settings(&table_ctx.get_settings())?;
        let spill_prefix = ctx.query_id_spill_prefix();
        let writer_pool_bytes = ctx
            .get_settings()
            .get_spill_writer_memory_pool_size_mb()?
            .saturating_mul(BYTES_PER_MIB);

        let payload_writers =
            AggregatePayloadWriters::create(&spill_prefix, partition_count, writer_pool_bytes, ctx);

        Ok(Self {
            read_setting,
            types: data_schema
                .fields()
                .iter()
                .map(|field| field.data_type().clone())
                .collect(),
            buffer_bytes,
            pending: std::iter::repeat_with(PendingBlocks::default)
                .take(partition_count)
                .collect(),
            payload_writers,
        })
    }

    pub fn spill(&mut self, partition_id: usize, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }
        let pending = &mut self.pending[partition_id];
        pending.bytes += block.memory_size();
        pending.blocks.push(block);
        if pending.bytes >= self.buffer_bytes {
            self.write_pending(partition_id)?;
        }
        Ok(())
    }

    fn write_pending(&mut self, partition_id: usize) -> Result<()> {
        let pending = mem::take(&mut self.pending[partition_id]);
        if pending.blocks.is_empty() {
            return Ok(());
        }
        self.payload_writers
            .write_row_group(partition_id, pending.blocks)
    }

    pub fn spill_finish(&mut self) -> Result<Vec<SpilledPayload>> {
        for partition_id in 0..self.pending.len() {
            self.write_pending(partition_id)?;
        }

        let payloads = self.payload_writers.finalize()?;
        debug!(
            "[AggregateSpiller] spill finish with {} payloads",
            payloads.len()
        );
        Ok(payloads)
    }

    /// Reads `payload` back as the blocks it was written from.
    pub fn restore(&self, payload: SpilledPayload) -> Result<SerializedPayload> {
        let SpilledPayload {
            bucket,
            location,
            segment,
        } = payload;

        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let instant = Instant::now();
        let data_block = read_segment(
            &SpillsBufferPool::instance(),
            data_operator.spill_operator(),
            location.clone(),
            &segment,
            &self.types,
            target,
            self.read_setting,
        )?;

        info!(
            "Read aggregate spill finished: (bucket: {}, location: {}, bytes: {}, rows: {}, elapsed: {:?})",
            bucket,
            location,
            segment.len,
            segment.rows,
            instant.elapsed()
        );

        Ok(SerializedPayload { bucket, data_block })
    }
}

fn flush_write_profile(ctx: &Arc<QueryContext>, stats: WriteStats) {
    if stats.count == 0 && stats.bytes == 0 && stats.rows == 0 {
        return;
    }

    if stats.rows > 0 || stats.bytes > 0 {
        let progress_val = ProgressValues {
            rows: stats.rows,
            bytes: stats.bytes,
        };
        ctx.get_aggregate_spill_progress().incr(&progress_val);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use databend_common_exception::Result;
    use databend_common_expression::DataBlock;
    use databend_common_expression::DataSchema;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;

    use crate::pipelines::processors::transforms::aggregator::AggregateSpiller;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_aggregate_payload_writers_local_stream() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        let partition_count = 4;
        let mut spiller = AggregateSpiller::try_create(
            ctx.clone(),
            partition_count,
            Arc::new(DataSchema::empty()),
            1024 * 1024,
        )?;

        let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1i32, 2, 3])]);

        spiller.spill(0, block.clone())?;
        spiller.spill(2, block)?;

        let payloads = spiller.spill_finish()?;

        assert_eq!(payloads.len(), 2);

        let spilled_files = ctx.get_spilled_files();
        assert_eq!(spilled_files.len(), 2);

        let buckets: HashSet<_> = payloads.iter().map(|p| p.bucket).collect();
        assert!(buckets.contains(&0));
        assert!(buckets.contains(&2));

        Ok(())
    }
}
