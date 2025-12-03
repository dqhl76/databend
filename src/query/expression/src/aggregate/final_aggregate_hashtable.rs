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
            let _ = self.probe_and_create(state, (&flush_state.group_columns).into(), row_count);
        }

        Ok(())
    }

    fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        row_count: usize,
    ) -> usize {
        let hash = &state.group_hashes[..row_count];

        let mut activate_sel = vec![];
        let mut deactivate_sel = vec![];
        hash.iter().enumerate().for_each(|(idx, hash)| {
            let partition_idx = ((*hash & self.mask_v) >> self.shift_v) as usize;
            if partition_idx == 0 {
                activate_sel.push(idx);
            } else {
                deactivate_sel.push(idx);
            }
        });

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
