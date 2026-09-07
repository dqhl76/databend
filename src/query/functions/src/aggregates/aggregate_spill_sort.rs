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

use std::cmp::Ordering;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::AggregateFunctionSpill;
use databend_common_expression::AggregateSpillFile;
use databend_common_expression::AggregateSpillReader;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::types::DataType;

use super::aggregate_spill::SPILL_BATCH_ROWS;

/// External merge for function-owned ordered states. Runs contain bounded
/// blocks. Pairwise passes keep the number of simultaneously restored blocks
/// constant even when one group has spilled thousands of runs.
pub(super) struct AggregateSpillSort {
    spill: Arc<dyn AggregateFunctionSpill>,
    descriptions: Vec<SortColumnDescription>,
    data_types: Vec<DataType>,
    runs: Vec<Vec<AggregateSpillFile>>,
}

impl AggregateSpillSort {
    pub fn new(
        spill: Arc<dyn AggregateFunctionSpill>,
        descriptions: Vec<SortColumnDescription>,
    ) -> Self {
        Self {
            spill,
            descriptions,
            data_types: vec![],
            runs: vec![],
        }
    }

    pub fn push(&mut self, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }
        if self.data_types.is_empty() {
            self.data_types = block.columns().iter().map(|c| c.data_type()).collect();
        }
        // The caller supplies a bounded native spill run. Keep that run size
        // instead of producing one object and manifest per input batch.
        self.spill.check_interrupt()?;
        let block = DataBlock::sort(&block, &self.descriptions, None)?;
        self.runs.push(vec![self.spill.spill(block)?]);
        Ok(())
    }

    pub fn finish(mut self, mut consume: impl FnMut(DataBlock) -> Result<()>) -> Result<()> {
        while self.runs.len() > 2 {
            let runs = std::mem::take(&mut self.runs);
            let mut iter = runs.into_iter();
            while let Some(left) = iter.next() {
                let Some(right) = iter.next() else {
                    self.runs.push(left);
                    break;
                };
                let mut merged = Vec::new();
                self.merge(left, right, |block| {
                    merged.push(self.spill.spill(block)?);
                    Ok(())
                })?;
                self.runs.push(merged);
            }
        }
        let mut runs = std::mem::take(&mut self.runs).into_iter();
        match (runs.next(), runs.next()) {
            (Some(left), Some(right)) => self.merge(left, right, consume),
            (Some(files), None) => {
                for file in files {
                    for block in self.spill.restore(&file, &self.data_types)? {
                        self.spill.check_interrupt()?;
                        consume(block?)?;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn merge(
        &self,
        left: Vec<AggregateSpillFile>,
        right: Vec<AggregateSpillFile>,
        mut consume: impl FnMut(DataBlock) -> Result<()>,
    ) -> Result<()> {
        let mut left = RunCursor::new(left);
        let mut right = RunCursor::new(right);
        let mut builders = self.builders();
        let mut rows = 0;
        loop {
            self.spill.check_interrupt()?;
            let has_left = left.ensure_row(self.spill.as_ref(), &self.data_types)?;
            let has_right = right.ensure_row(self.spill.as_ref(), &self.data_types)?;
            let cursor = match (has_left, has_right) {
                (false, false) => break,
                (true, false) => &mut left,
                (false, true) => &mut right,
                (true, true) => {
                    if self.compare(&left, &right).is_gt() {
                        &mut right
                    } else {
                        &mut left
                    }
                }
            };
            let block = cursor.block.as_ref().unwrap();
            for (builder, column) in builders.iter_mut().zip(block.columns()) {
                builder.push(column.index(cursor.row).unwrap());
            }
            cursor.row += 1;
            rows += 1;
            if rows
                >= (self.spill.restore_memory_limit() / size_of::<usize>()).max(SPILL_BATCH_ROWS)
                || builders
                    .iter()
                    .map(ColumnBuilder::memory_size)
                    .sum::<usize>()
                    >= self.spill.restore_memory_limit()
            {
                consume(DataBlock::new_from_columns(
                    builders.into_iter().map(ColumnBuilder::build).collect(),
                ))?;
                builders = self.builders();
                rows = 0;
            }
        }
        if rows != 0 {
            consume(DataBlock::new_from_columns(
                builders.into_iter().map(ColumnBuilder::build).collect(),
            ))?;
        }
        Ok(())
    }

    fn builders(&self) -> Vec<ColumnBuilder> {
        self.data_types
            .iter()
            .map(|ty| ColumnBuilder::with_capacity(ty, 0))
            .collect()
    }

    fn compare(&self, left: &RunCursor, right: &RunCursor) -> Ordering {
        for desc in &self.descriptions {
            let lhs = left
                .block
                .as_ref()
                .unwrap()
                .get_by_offset(desc.offset)
                .index(left.row)
                .unwrap();
            let rhs = right
                .block
                .as_ref()
                .unwrap()
                .get_by_offset(desc.offset)
                .index(right.row)
                .unwrap();
            let ordering = match (&lhs, &rhs) {
                (ScalarRef::Null, ScalarRef::Null) => Ordering::Equal,
                (ScalarRef::Null, _) => {
                    if desc.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (_, ScalarRef::Null) => {
                    if desc.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                _ => {
                    if desc.asc {
                        lhs.cmp(&rhs)
                    } else {
                        rhs.cmp(&lhs)
                    }
                }
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    }
}

struct RunCursor {
    files: std::vec::IntoIter<AggregateSpillFile>,
    reader: Option<AggregateSpillReader>,
    block: Option<DataBlock>,
    row: usize,
}

impl RunCursor {
    fn new(files: Vec<AggregateSpillFile>) -> Self {
        Self {
            files: files.into_iter(),
            reader: None,
            block: None,
            row: 0,
        }
    }

    fn ensure_row(
        &mut self,
        spill: &dyn AggregateFunctionSpill,
        types: &[DataType],
    ) -> Result<bool> {
        loop {
            if self.block.as_ref().is_some_and(|b| self.row < b.num_rows()) {
                return Ok(true);
            }
            // Drop the previous block before fetching the next one.
            self.block = None;
            if let Some(reader) = &mut self.reader {
                if let Some(block) = reader.next() {
                    self.block = Some(block?);
                    self.row = 0;
                    continue;
                }
            }
            self.reader = None;
            let Some(file) = self.files.next() else {
                return Ok(false);
            };
            self.reader = Some(spill.restore(&file, types)?);
        }
    }
}
