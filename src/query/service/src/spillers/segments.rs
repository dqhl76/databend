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

//! Spill files made of segments: each segment is the rows of a run of blocks, encoded as one
//! block by [`super::column_codec`], and read back alone by its byte range. The reader knows
//! the types of the columns, so a segment carries no schema.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::types::DataType;
use databend_common_storages_parquet::ReadSettings;
use opendal::Operator;

use super::BufferWriter;
use super::SpillTarget;
use super::SpillsBufferPool;
use super::column_codec::decode_block;
use super::column_codec::encode_blocks;
use super::record_read_profile;
use super::record_write_profile;

/// The byte range of a segment in its file, and the rows it holds.
#[derive(Clone, Debug)]
pub struct SpilledSegment {
    pub offset: u64,
    pub len: u64,
    pub rows: usize,
}

/// Writes the segments of one spill file.
pub struct SpillsSegmentWriter {
    writer: BufferWriter,
    target: SpillTarget,
    offset: u64,
    segments: Vec<SpilledSegment>,
    write_duration: Duration,
}

impl SpillsSegmentWriter {
    pub fn try_create(
        pool: &Arc<SpillsBufferPool>,
        op: Operator,
        path: String,
        pool_bytes: usize,
        target: SpillTarget,
    ) -> Result<Self> {
        Ok(SpillsSegmentWriter {
            writer: pool.buffer_writer(op, path, pool_bytes)?,
            target,
            offset: 0,
            segments: vec![],
            write_duration: Duration::ZERO,
        })
    }

    /// Writes the rows of `blocks`, which must share their columns, as one segment.
    pub fn write_segment(&mut self, blocks: Vec<DataBlock>) -> Result<()> {
        let blocks = blocks
            .into_iter()
            .filter(|block| !block.is_empty())
            .collect::<Vec<_>>();
        if blocks.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        let mut bytes =
            Vec::with_capacity(blocks.iter().map(DataBlock::memory_size).sum::<usize>() / 2);
        encode_blocks(&blocks, &mut bytes)?;
        let rows = blocks.iter().map(DataBlock::num_rows).sum::<usize>();
        std::io::Write::write_all(&mut self.writer, &bytes)?;
        let len = bytes.len() as u64;
        self.segments.push(SpilledSegment {
            offset: self.offset,
            len,
            rows,
        });
        self.offset += len;
        self.write_duration += start.elapsed();
        Ok(())
    }

    /// Closes the file; returns its size and its segments.
    pub fn close(self) -> Result<(usize, Vec<SpilledSegment>)> {
        let start = Instant::now();
        self.writer.close()?;
        let write_duration = self.write_duration + start.elapsed();
        let bytes = self.offset as usize;
        record_write_profile(self.target, write_duration, bytes);
        Ok((bytes, self.segments))
    }
}

/// Reads the rows of `segment` of the file at `location`, with columns of `types`.
pub fn read_segment(
    pool: &Arc<SpillsBufferPool>,
    op: Operator,
    location: String,
    segment: &SpilledSegment,
    types: &[DataType],
    target: SpillTarget,
    settings: ReadSettings,
) -> Result<DataBlock> {
    let start = Instant::now();
    let range = segment.offset..segment.offset + segment.len;
    let mut chunks = pool.fetch_ranges(op, location, vec![range], settings)?;
    let bytes = match chunks.len() {
        1 => chunks.pop().unwrap(),
        _ => chunks.concat().into(),
    };
    let block = decode_block(&bytes, types)?;
    if block.num_rows() != segment.rows {
        return Err(ErrorCode::Internal(format!(
            "corrupted spilled segment: {} rows instead of {}",
            block.num_rows(),
            segment.rows
        )));
    }
    record_read_profile(target, start.elapsed(), bytes.len());
    Ok(block)
}
