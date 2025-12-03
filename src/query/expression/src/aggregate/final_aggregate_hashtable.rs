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

use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::Result;

use crate::aggregate::hash_index::AdapterImpl;
use crate::types::DataType;
use crate::AggregateFunctionRef;
use crate::AggregateHashTable;
use crate::HashTableConfig;
use crate::Payload;
use crate::PayloadFlushState;
use crate::ProbeState;
use crate::ProjectedBlock;

pub struct FinalAggregateHashTable {
    pub activate: AggregateHashTable,
    pub deactivate: AggregateHashTable,
    pub mask_v: u64,
    pub shift_v: u64,
}

impl FinalAggregateHashTable {
    pub fn new(
        radix_bits: u64,
        offset: u64,
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
    ) -> Self {
        Self {
            activate: AggregateHashTable::new(
                group_types.clone(),
                aggrs.clone(),
                HashTableConfig::default().with_initial_radix_bits(0),
                Arc::new(Bump::new()),
            ),
            deactivate: AggregateHashTable::new(
                group_types.clone(),
                aggrs.clone(),
                HashTableConfig::default().with_initial_radix_bits(radix_bits),
                Arc::new(Bump::new()),
            ),
            mask_v: mask(radix_bits, offset),
            shift_v: shift(radix_bits, offset),
        }
    }

    pub fn combine_payload(
        &mut self,
        payload: &Payload,
        flush_state: &mut PayloadFlushState,
    ) -> Result<()> {
        flush_state.clear();

        while payload.flush(flush_state) {
            let row_count = flush_state.row_count;

            let state = &mut *flush_state.probe_state;
            let (partitions, activate_sel, deactivate_sel) =
                self.partition_rows(&state.group_hashes, row_count);
            let _ = self.probe_and_create(
                state,
                (&flush_state.group_columns).into(),
                &activate_sel,
                &deactivate_sel,
            );

            let places = &mut state.state_places[..row_count];

            // set state places
            if !self.activate.payload.aggrs.is_empty() {
                for (idx, (place, ptr)) in places
                    .iter_mut()
                    .zip(&state.addresses[..row_count])
                    .enumerate()
                {
                    let layout = if partitions[idx] == 0 {
                        &self.activate.payload.row_layout
                    } else {
                        &self.deactivate.payload.row_layout
                    };

                    *place = ptr.state_addr(layout);
                }
            }

            if let Some(layout) = self.activate.payload.row_layout.states_layout.as_ref() {
                let rhses = &flush_state.state_places[..row_count];
                for (aggr, loc) in self.activate.payload.aggrs.iter().zip(layout.states_loc.iter())
                {
                    aggr.batch_merge_states(places, rhses, loc)?;
                }
            }
        }

        Ok(())
    }

    fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        activate_sel: &[usize],
        deactivate_sel: &[usize],
    ) -> usize {
        if activate_sel.len() + self.activate.hash_index.count
            > self.activate.hash_index.resize_threshold()
        {
            self.activate.resize(self.activate.hash_index.capacity * 2);
        }

        if deactivate_sel.len() + self.deactivate.hash_index.count
            > self.deactivate.hash_index.resize_threshold()
        {
            self.deactivate
                .resize(self.deactivate.hash_index.capacity * 2);
        }

        self.activate
            .hash_index
            .probe_and_create_selected(state, &activate_sel, AdapterImpl {
                payload: &mut self.activate.payload,
                group_columns,
            })
            + self.deactivate.hash_index.probe_and_create_selected(
                state,
                &deactivate_sel,
                AdapterImpl {
                    payload: &mut self.deactivate.payload,
                    group_columns,
                },
            )
    }

    fn partition_rows(
        &self,
        hashes: &[u64],
        row_count: usize,
    ) -> (Vec<usize>, Vec<usize>, Vec<usize>) {
        let mut partitions = vec![0; row_count];
        let mut activate_sel = Vec::with_capacity(row_count);
        let mut deactivate_sel = Vec::with_capacity(row_count);

        for (idx, hash) in hashes[..row_count].iter().copied().enumerate() {
            let partition_idx = self.partition_idx(hash);
            partitions[idx] = partition_idx;

            if partition_idx == 0 {
                activate_sel.push(idx);
            } else {
                deactivate_sel.push(idx);
            }
        }

        (partitions, activate_sel, deactivate_sel)
    }

    #[inline]
    fn partition_idx(&self, hash: u64) -> usize {
        ((hash & self.mask_v) >> self.shift_v) as usize
    }
}

#[inline]
fn shift(radix_bits: u64, offset: u64) -> u64 {
    debug_assert!(radix_bits + offset <= 48);
    48 - radix_bits - offset
}

#[inline]
fn mask(radix_bits: u64, offset: u64) -> u64 {
    ((1 << radix_bits) - 1) << shift(radix_bits, offset)
}
