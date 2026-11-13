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

//! The states of groups that hold one input row each, without a hash table.

use std::alloc::Layout;

use bumpalo::Bump;
use databend_common_exception::Result;

use super::AggrState;
use super::BATCH_SIZE;
use super::StateAddr;
use super::StatesLayout;
use super::aggregate_function::AggregateCallRef;
use super::aggregate_function::AggregateStateSet;
use super::aggregate_function::get_states_layout;
use super::partitioned_payload::PartitionMask;
use crate::BlockEntry;
use crate::ColumnBuilder;
use crate::ProjectedBlock;

/// Serializes, for each input row, the states of a group that holds that row alone: the state
/// columns a hash table flushes when every row is a group of its own, without probing a table
/// or storing the rows in its pages.
pub struct RowStatesSerializer {
    aggrs: Vec<AggregateCallRef>,
    layout: StatesLayout,
    arena: Bump,
    places: Vec<StateAddr>,
}

// The addresses only point into the serializer's own arena, which moves with it.
unsafe impl Send for RowStatesSerializer {}

impl RowStatesSerializer {
    pub fn try_create(aggrs: Vec<AggregateCallRef>) -> Result<Self> {
        let layout = get_states_layout(&aggrs)?;
        Ok(RowStatesSerializer {
            aggrs,
            layout,
            arena: Bump::new(),
            places: Vec::with_capacity(BATCH_SIZE),
        })
    }

    /// The serialized states of `row_count` rows with the arguments `params`, one entry per
    /// aggregate.
    pub fn serialize(
        &mut self,
        params: &[ProjectedBlock],
        row_count: usize,
    ) -> Result<Vec<BlockEntry>> {
        let mut builders = self.layout.serialize_builders(row_count);
        let row_layout = self.layout.layout.pad_to_align();
        for start in (0..row_count).step_by(BATCH_SIZE) {
            let end = (start + BATCH_SIZE).min(row_count);
            let rows = end - start;
            let step_params: Vec<Vec<BlockEntry>> = params
                .iter()
                .map(|entries| entries.iter().map(|x| x.slice(start..end)).collect())
                .collect();
            let step_params = step_params
                .iter()
                .map(|entries| entries.into())
                .collect::<Vec<ProjectedBlock>>();

            // The states of a batch live in one allocation of the arena, reused by the next.
            self.arena.reset();
            let batch = Layout::from_size_align(row_layout.size() * rows, row_layout.align())
                .expect("the states of a batch fit in memory");
            let base = StateAddr::from(self.arena.alloc_layout(batch));
            self.places.clear();
            self.places
                .extend((0..rows).map(|row| base.next(row * row_layout.size())));

            for (aggr, loc) in self.aggrs.iter().zip(self.layout.states_loc.iter()) {
                for place in &self.places {
                    aggr.init_state(AggrState::new(*place, loc));
                }
            }
            let result = self.accumulate_and_serialize(&step_params, &mut builders);
            for (aggr, loc) in self.aggrs.iter().zip(self.layout.states_loc.iter()) {
                if aggr.state().need_manual_drop() {
                    for place in &self.places {
                        unsafe { aggr.drop_state(AggrState::new(*place, loc)) };
                    }
                }
            }
            result?;
        }
        Ok(builders
            .into_iter()
            .map(|builder| builder.build().into())
            .collect())
    }

    fn accumulate_and_serialize(
        &self,
        params: &[ProjectedBlock],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        for (((aggr, loc), params), builder) in self
            .aggrs
            .iter()
            .zip(self.layout.states_loc.iter())
            .zip(params.iter())
            .zip(builders.iter_mut())
        {
            aggr.accumulate_keys(AggregateStateSet::new(&self.places, loc), *params)?;
            aggr.serialize(
                AggregateStateSet::new(&self.places, loc),
                builder.as_tuple_mut().unwrap().as_mut_slice(),
            )?;
        }
        Ok(())
    }
}

/// How rows split among `partition_count` partitions of their group hashes from `start_bit`,
/// as a hash table of that depth partitions them.
pub enum RowPartitions {
    /// All rows are in this partition.
    Single(usize),
    /// The rows in the order of their partitions, and the end of each partition in that order.
    Ordered { rows: Vec<u32>, ends: Vec<usize> },
}

impl RowPartitions {
    pub fn of_hashes(hashes: &[u64], partition_count: usize, start_bit: u64) -> Self {
        let mask = PartitionMask::with_start_bit(partition_count as u64, start_bit);
        let mut ends = vec![0; partition_count];
        for hash in hashes {
            ends[mask.index(*hash)] += 1;
        }
        if let Some(partition) = ends.iter().position(|count| *count == hashes.len()) {
            return RowPartitions::Single(partition);
        }
        let mut next = Vec::with_capacity(partition_count);
        let mut end = 0;
        for count in ends.iter_mut() {
            next.push(end);
            end += *count;
            *count = end;
        }
        let mut rows = vec![0; hashes.len()];
        for (row, hash) in hashes.iter().enumerate() {
            let partition = mask.index(*hash);
            rows[next[partition]] = row as u32;
            next[partition] += 1;
        }
        RowPartitions::Ordered { rows, ends }
    }
}
