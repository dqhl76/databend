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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::Result;

use crate::aggregate::group_hash_columns;
use crate::aggregate::hash_index::AdapterImpl;
use crate::types::DataType;
use crate::AggregateFunctionRef;
use crate::AggregateHashTable;
use crate::BlockEntry;
use crate::HashTableConfig;
use crate::PartitionedPayload;
use crate::Payload;
use crate::PayloadFlushState;
use crate::ProbeState;
use crate::ProjectedBlock;

const BATCH_ADD_SIZE: usize = 2048;

pub struct FinalAggregateHashTable {
    pub activate: AggregateHashTable,
    pub deactivate: Option<AggregateHashTable>,
    pub mask_v: u64,
    pub shift_v: u64,
}

impl FinalAggregateHashTable {
    pub fn new(
        radix_bits: u64,
        offset: u64,
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        direct_append: bool,
    ) -> Self {
        let mut activate = AggregateHashTable::new(
            group_types.clone(),
            aggrs.clone(),
            HashTableConfig::default().with_initial_radix_bits(0),
            Arc::new(Bump::new()),
        );
        let mut deactivate = AggregateHashTable::new(
            group_types.clone(),
            aggrs.clone(),
            HashTableConfig::default().with_initial_radix_bits(radix_bits),
            Arc::new(Bump::new()),
        );
        activate.direct_append = direct_append;
        deactivate.direct_append = direct_append;

        Self {
            activate,
            deactivate: Some(deactivate),
            mask_v: mask(radix_bits, offset),
            shift_v: shift(radix_bits, offset),
        }
    }

    pub fn new_with_capcity(
        radix_bits: u64,
        offset: u64,
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        direct_append: bool,
        capacity: usize,
    ) -> Self {
        let mut activate = AggregateHashTable::new_with_capacity(
            group_types.clone(),
            aggrs.clone(),
            HashTableConfig::default().with_initial_radix_bits(0),
            capacity,
            Arc::new(Bump::new()),
        );
        let mut deactivate = AggregateHashTable::new_with_capacity(
            group_types.clone(),
            aggrs.clone(),
            HashTableConfig::default().with_initial_radix_bits(radix_bits),
            capacity,
            Arc::new(Bump::new()),
        );
        activate.direct_append = direct_append;
        deactivate.direct_append = direct_append;

        Self {
            activate,
            deactivate: Some(deactivate),
            mask_v: mask(radix_bits, offset),
            shift_v: shift(radix_bits, offset),
        }
    }

    pub fn add_groups(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        params: &[ProjectedBlock],
        agg_states: ProjectedBlock,
        row_count: usize,
    ) -> Result<usize> {
        if row_count <= BATCH_ADD_SIZE {
            self.add_groups_inner(state, group_columns, params, agg_states, row_count)
        } else {
            let mut new_count = 0;
            for start in (0..row_count).step_by(BATCH_ADD_SIZE) {
                let end = (start + BATCH_ADD_SIZE).min(row_count);
                let step_group_columns = group_columns
                    .iter()
                    .map(|entry| entry.slice(start..end))
                    .collect::<Vec<_>>();

                let step_params: Vec<Vec<BlockEntry>> = params
                    .iter()
                    .map(|c| c.iter().map(|x| x.slice(start..end)).collect())
                    .collect();
                let step_params = step_params.iter().map(|v| v.into()).collect::<Vec<_>>();
                let agg_states = agg_states
                    .iter()
                    .map(|c| c.slice(start..end))
                    .collect::<Vec<_>>();

                new_count += self.add_groups_inner(
                    state,
                    (&step_group_columns).into(),
                    &step_params,
                    (&agg_states).into(),
                    end - start,
                )?;
            }
            Ok(new_count)
        }
    }

    fn add_groups_inner(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        params: &[ProjectedBlock],
        agg_states: ProjectedBlock,
        row_count: usize,
    ) -> Result<usize> {
        #[cfg(debug_assertions)]
        {
            for (i, group_column) in group_columns.iter().enumerate() {
                if group_column.data_type() != self.activate.payload.group_types[i] {
                    return Err(databend_common_exception::ErrorCode::UnknownException(
                        format!(
                            "group_column type not match in index {}, expect: {:?}, actual: {:?}",
                            i,
                            self.activate.payload.group_types[i],
                            group_column.data_type()
                        ),
                    ));
                }
            }
        }

        state.row_count = row_count;
        group_hash_columns(group_columns, &mut state.group_hashes);

        let (partitions, activate_sel, deactivate_sel) =
            self.partition_rows(&state.group_hashes, row_count);
        let direct_append = self.activate.direct_append;
        if let Some(deactivate) = self.deactivate.as_ref() {
            debug_assert_eq!(direct_append, deactivate.direct_append);
        }
        let new_group_count = if direct_append {
            self.direct_append_rows(state, group_columns, &activate_sel, &deactivate_sel)
        } else {
            self.probe_and_create(state, group_columns, &activate_sel, &deactivate_sel)
        };

        if !self.activate.payload.aggrs.is_empty() {
            for (idx, (place, ptr)) in state.state_places[..row_count]
                .iter_mut()
                .zip(&state.addresses[..row_count])
                .enumerate()
            {
                let layout = if partitions[idx] == 0 {
                    &self.activate.payload.row_layout
                } else {
                    &self.deactivate().payload.row_layout
                };

                *place = ptr.state_addr(layout);
            }

            let state_places = &state.state_places.as_slice()[0..row_count];
            let states_layout = self
                .activate
                .payload
                .row_layout
                .states_layout
                .as_ref()
                .unwrap();
            if agg_states.is_empty() {
                for ((func, params), loc) in self
                    .activate
                    .payload
                    .aggrs
                    .iter()
                    .zip(params.iter())
                    .zip(states_layout.states_loc.iter())
                {
                    func.accumulate_keys(state_places, loc, *params, row_count)?;
                }
            } else {
                for ((func, state), loc) in self
                    .activate
                    .payload
                    .aggrs
                    .iter()
                    .zip(agg_states.iter())
                    .zip(states_layout.states_loc.iter())
                {
                    func.batch_merge(state_places, loc, state, None)?;
                }
            }
        }

        Ok(new_group_count)
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
                        &self.deactivate().payload.row_layout
                    };

                    *place = ptr.state_addr(layout);
                }
            }

            if let Some(layout) = self.activate.payload.row_layout.states_layout.as_ref() {
                let rhses = &flush_state.state_places[..row_count];
                for (aggr, loc) in self
                    .activate
                    .payload
                    .aggrs
                    .iter()
                    .zip(layout.states_loc.iter())
                {
                    aggr.batch_merge_states(places, rhses, loc)?;
                }
            }
        }

        Ok(())
    }

    pub fn combine_payloads(
        &mut self,
        payloads: &PartitionedPayload,
        flush_state: &mut PayloadFlushState,
    ) -> Result<()> {
        for payload in payloads.payloads.iter() {
            self.combine_payload(payload, flush_state)?;
        }
        Ok(())
    }

    pub fn merge_result(&mut self, flush_state: &mut PayloadFlushState) -> Result<bool> {
        let activate_partition_count = self.activate.payload.payloads.len();
        let Some(deactivate) = self.deactivate.as_mut() else {
            return self.activate.merge_result(flush_state);
        };
        let deactivate_partition_count = deactivate.payload.payloads.len();

        // Drain activate hashtable first.
        if flush_state.flush_partition < activate_partition_count {
            if self.activate.merge_result(flush_state)? {
                return Ok(true);
            }
            // Switch to deactivate hashtable.
            flush_state.flush_partition = 0;
        } else {
            // Already in deactivate hashtable, adjust partition index.
            flush_state.flush_partition = flush_state
                .flush_partition
                .saturating_sub(activate_partition_count);
        }

        if flush_state.flush_partition >= deactivate_partition_count {
            flush_state.flush_partition = activate_partition_count + deactivate_partition_count;
            return Ok(false);
        }

        let result = deactivate.merge_result(flush_state)?;
        flush_state.flush_partition += activate_partition_count;
        Ok(result)
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

        if deactivate_sel.len() + self.deactivate().hash_index.count
            > self.deactivate().hash_index.resize_threshold()
        {
            let new_capacity = self.deactivate().hash_index.capacity * 2;
            self.deactivate_mut().resize(new_capacity);
        }

        let activate_new =
            self.activate
                .hash_index
                .probe_and_create_selected(state, &activate_sel, AdapterImpl {
                    payload: &mut self.activate.payload,
                    group_columns,
                });

        let deactivate_new = {
            let deactivate = self.deactivate_mut();
            deactivate
                .hash_index
                .probe_and_create_selected(state, &deactivate_sel, AdapterImpl {
                    payload: &mut deactivate.payload,
                    group_columns,
                })
        };

        activate_new + deactivate_new
    }

    fn direct_append_rows(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        activate_sel: &[usize],
        deactivate_sel: &[usize],
    ) -> usize {
        if !activate_sel.is_empty() {
            for (idx, row) in activate_sel.iter().copied().enumerate() {
                state.empty_vector[idx] = row;
            }
            self.activate
                .payload
                .append_rows(state, activate_sel.len(), group_columns);
        }

        if !deactivate_sel.is_empty() {
            if let Some(deactivate) = self.deactivate.as_mut() {
                for (idx, row) in deactivate_sel.iter().copied().enumerate() {
                    state.empty_vector[idx] = row;
                }
                deactivate
                    .payload
                    .append_rows(state, deactivate_sel.len(), group_columns);
            }
        }

        activate_sel.len() + deactivate_sel.len()
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

    #[inline]
    fn deactivate(&self) -> &AggregateHashTable {
        self.deactivate
            .as_ref()
            .expect("deactivate hashtable is not initialized")
    }

    #[inline]
    fn deactivate_mut(&mut self) -> &mut AggregateHashTable {
        self.deactivate
            .as_mut()
            .expect("deactivate hashtable is not initialized")
    }

    pub fn mark_min_cardinality(&mut self) {
        self.activate.payload.mark_min_cardinality();
        if let Some(deactivate) = self.deactivate.as_mut() {
            deactivate.payload.mark_min_cardinality();
        }
    }

    pub fn spill_deactivate(&mut self) -> Vec<Payload> {
        let Some(deactivate) = self.deactivate.take() else {
            unreachable!("logic error: deactivate hashtable is not initialized")
        };

        let group_types = deactivate.payload.group_types.clone();
        let aggrs = deactivate.payload.aggrs.clone();
        let config = deactivate.config.clone();

        let payloads = deactivate.payload.payloads;

        let new_deactivate =
            AggregateHashTable::new(group_types, aggrs, config, Arc::new(Bump::new()));
        self.deactivate = Some(new_deactivate);

        payloads
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

impl Debug for FinalAggregateHashTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        struct DebugRow {
            hash: u64,
            groups: Vec<String>,
        }

        #[derive(Debug)]
        struct DebugPage {
            page_index: usize,
            rows: Vec<DebugRow>,
        }

        #[derive(Debug)]
        struct DebugPartition {
            partition: usize,
            pages: Vec<DebugPage>,
        }

        fn payload_debug_info(payload: &Payload) -> Vec<DebugPage> {
            let mut pages = Vec::new();
            let mut state = PayloadFlushState::default();

            while payload.flush(&mut state) {
                let page_index = state.flush_page;
                if pages
                    .last()
                    .map(|page: &DebugPage| page.page_index != page_index)
                    .unwrap_or(true)
                {
                    pages.push(DebugPage {
                        page_index,
                        rows: Vec::new(),
                    });
                }

                if let Some(page) = pages.last_mut() {
                    let hashes = &state.probe_state.group_hashes[..state.row_count];
                    for row in 0..state.row_count {
                        let groups = state
                            .group_columns
                            .iter()
                            .map(|entry| match entry.index(row) {
                                Some(value) => format!("{value:?}"),
                                None => "None".to_string(),
                            })
                            .collect();

                        page.rows.push(DebugRow {
                            hash: hashes[row],
                            groups,
                        });
                    }
                }
            }

            pages
        }

        fn partitions_debug_info(payloads: &PartitionedPayload) -> Vec<DebugPartition> {
            payloads
                .payloads
                .iter()
                .enumerate()
                .filter_map(|(partition, payload)| {
                    let pages = payload_debug_info(payload);
                    if pages.is_empty() {
                        None
                    } else {
                        Some(DebugPartition { partition, pages })
                    }
                })
                .collect()
        }

        let activate = partitions_debug_info(&self.activate.payload);
        let deactivate = self
            .deactivate
            .as_ref()
            .map(|table| partitions_debug_info(&table.payload))
            .unwrap_or_default();

        f.debug_struct("FinalAggregateHashTable")
            .field("activate", &activate)
            .field("deactivate", &deactivate)
            .finish()
    }
}
