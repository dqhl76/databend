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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::AggregateFunctionSpill;
use databend_common_expression::AggregateSpillFile;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::StateAddr;
use databend_common_expression::StatesLayout;
use databend_common_expression::get_states_layout;
use databend_common_expression::group_hash_entries;
use serde::Deserialize;
use serde::Serialize;

#[cfg(test)]
mod tests;

pub(super) const SPILL_BATCH_ROWS: usize = 2048;
pub(super) const PARTITIONS: usize = 4;
pub(super) const PARTITION_BITS: u32 = PARTITIONS.trailing_zeros();

pub(super) struct FunctionState {
    pub function: AggregateFunctionRef,
    pub layout: StatesLayout,
    pub addr: StateAddr,
    _arena: bumpalo::Bump,
}

impl FunctionState {
    pub fn new(function: AggregateFunctionRef) -> Result<Self> {
        let layout = get_states_layout(std::slice::from_ref(&function))?;
        let arena = bumpalo::Bump::new();
        let addr = arena.alloc_layout(layout.layout).into();
        function.init_state(AggrState::new(addr, &layout.states_loc[0]));
        Ok(Self {
            function,
            layout,
            addr,
            _arena: arena,
        })
    }

    pub fn place(&self) -> AggrState<'_> {
        AggrState::new(self.addr, &self.layout.states_loc[0])
    }
}

impl Drop for FunctionState {
    fn drop(&mut self) {
        if self.function.need_manual_drop_state() {
            unsafe { self.function.drop_state(self.place()) };
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct SpillPartition {
    pub bucket: usize,
    pub file: AggregateSpillFile,
}

/// Only query execution uses this manifest. Runs remain query-owned when a
/// state is cloned, serialized, or merged, so consuming one state cannot remove
/// files that another worker or a read-only result evaluation still needs.
#[derive(Default)]
pub(super) struct SpillState {
    pub runs: Vec<SpillPartition>,
    pub rows_since_check: usize,
}

impl SpillState {
    pub fn memory_size(&self) -> usize {
        self.runs.capacity() * size_of::<SpillPartition>()
            + self
                .runs
                .iter()
                .map(|run| run.file.location.capacity() + run.file.metadata.capacity())
                .sum::<usize>()
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.runs.is_empty() {
            Ok(vec![])
        } else {
            Ok(serde_json::to_vec(&self.runs)?)
        }
    }

    pub fn merge(&mut self, bytes: &[u8]) -> Result<()> {
        if !bytes.is_empty() {
            let runs: Vec<SpillPartition> = serde_json::from_slice(bytes)?;
            if runs.iter().any(|run| run.bucket >= PARTITIONS) {
                return Err(ErrorCode::BadBytes(
                    "Invalid aggregate-function spill partition",
                ));
            }
            self.runs.extend(runs);
        }
        Ok(())
    }
}

/// Split a single serialized state into its durable fields and optional query
/// manifest. Accepting the durable layout also allows reading aggregate indexes.
pub(super) fn split_spill_state(
    state: &BlockEntry,
    durable_fields: usize,
) -> Result<(BlockEntry, Vec<u8>)> {
    let Column::Tuple(mut fields) = state.to_column() else {
        return Err(ErrorCode::BadBytes("Aggregate state must be a tuple"));
    };
    let metadata = if fields.len() == durable_fields + 1 {
        let Column::Binary(column) = fields.pop().unwrap() else {
            return Err(ErrorCode::BadBytes(
                "Aggregate spill manifest must be binary",
            ));
        };
        column.index(0).unwrap().to_vec()
    } else if fields.len() == durable_fields {
        vec![]
    } else {
        return Err(ErrorCode::BadBytes(
            "Unexpected aggregate state field count",
        ));
    };
    Ok((Column::Tuple(fields).into(), metadata))
}

/// Partition on function-selected keys. Additional radix bits are consumed on
/// each restore pass; hashing only the outer SQL GROUP BY key cannot subdivide
/// an individual aggregate state.
pub(super) fn spill_hash_partitions(
    spill: &dyn AggregateFunctionSpill,
    block: DataBlock,
    key_columns: &[usize],
    depth: u32,
) -> Result<Vec<SpillPartition>> {
    if depth * PARTITION_BITS >= u64::BITS {
        return Err(ErrorCode::MemoryExceedsLimit(
            "Aggregate-function partition still exceeds its memory limit after exhausting hash bits",
        ));
    }
    let keys = databend_common_expression::ProjectedBlock::project(key_columns, &block);
    let mut hashes = vec![0; block.num_rows()];
    group_hash_entries(keys, &mut hashes);
    let indices = hashes
        .into_iter()
        .map(|hash| ((hash >> (depth * PARTITION_BITS)) & (PARTITIONS as u64 - 1)) as u32)
        .collect::<Vec<_>>();
    let mut runs = Vec::new();
    for (bucket, block) in block.scatter(&indices, PARTITIONS)?.into_iter().enumerate() {
        if !block.is_empty() {
            spill.check_interrupt()?;
            runs.push(SpillPartition {
                bucket,
                file: spill.spill(block)?,
            });
        }
    }
    Ok(runs)
}
