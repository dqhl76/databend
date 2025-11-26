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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use itertools::Itertools;
use log::warn;

use super::payload::Payload;
use super::probe_state::ProbeState;
use super::row_ptr::RowLayout;
use crate::get_states_layout;
use crate::types::DataType;
use crate::AggregateFunctionRef;
use crate::PayloadFlushState;
use crate::ProjectedBlock;
use crate::StatesLayout;
use crate::BATCH_SIZE;

pub struct PartitionedPayload {
    pub payloads: Vec<Payload>,
    pub group_types: Vec<DataType>,
    pub aggrs: Vec<AggregateFunctionRef>,

    pub(super) row_layout: RowLayout,

    pub arenas: Vec<Arc<Bump>>,

    separated_arenas: bool,
    partition_count: u64,
    mask_v: u64,
    shift_v: u64,
}

unsafe impl Send for PartitionedPayload {}
unsafe impl Sync for PartitionedPayload {}

impl PartitionedPayload {
    pub fn new(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        partition_count: u64,
        arenas: Vec<Arc<Bump>>,
    ) -> Self {
        Self::new_inner(group_types, aggrs, partition_count, arenas, false)
    }

    pub fn new_with_individual_arenas(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        partition_count: u64,
        arenas: Vec<Arc<Bump>>,
    ) -> Self {
        Self::new_inner(group_types, aggrs, partition_count, arenas, true)
    }

    pub fn new_with_partition_bumps(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        partition_count: u64,
    ) -> Self {
        let arenas = (0..partition_count)
            .map(|_| Arc::new(Bump::new()))
            .collect();
        Self::new_with_individual_arenas(group_types, aggrs, partition_count, arenas)
    }

    fn new_inner(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        partition_count: u64,
        arenas: Vec<Arc<Bump>>,
        separated_arenas: bool,
    ) -> Self {
        let radix_bits = partition_count.trailing_zeros() as u64;
        debug_assert_eq!(1 << radix_bits, partition_count);
        debug_assert!(!arenas.is_empty());
        debug_assert!(
            !separated_arenas || arenas.len() == partition_count as usize,
            "arenas length must match partition count when separated"
        );

        let states_layout = if !aggrs.is_empty() {
            Some(get_states_layout(&aggrs).unwrap())
        } else {
            None
        };

        let payloads = (0..partition_count)
            .map(|idx| {
                let arena_idx = if separated_arenas { idx as usize } else { 0 };
                Payload::new(
                    arenas[arena_idx].clone(),
                    group_types.clone(),
                    aggrs.clone(),
                    states_layout.clone(),
                )
            })
            .collect_vec();

        let offsets = RowLayout {
            states_layout,
            ..payloads[0].row_layout.clone()
        };

        PartitionedPayload {
            payloads,
            group_types,
            aggrs,
            row_layout: offsets,
            separated_arenas,
            partition_count,

            arenas,
            mask_v: mask(radix_bits),
            shift_v: shift(radix_bits),
        }
    }

    pub fn states_layout(&self) -> Option<&StatesLayout> {
        self.row_layout.states_layout.as_ref()
    }

    pub fn take_payloads(&mut self, buckets: &[usize]) -> Result<Vec<Payload>> {
        let mut taken = Vec::with_capacity(buckets.len());
        let partition_count = self.partition_count();
        let states_layout = self.row_layout.states_layout.clone();

        for &bucket in buckets {
            if bucket >= partition_count {
                return Err(ErrorCode::Internal(format!(
                    "Bucket {} out of range, partition count: {}",
                    bucket, partition_count
                )));
            }

            let new_arena = if self.separated_arenas {
                let arena = Arc::new(Bump::new());
                self.arenas[bucket] = arena.clone();
                arena
            } else {
                warn!("Reusing the same arena for all partitions during take_payloads");
                self.arenas[0].clone()
            };

            let new_payload = Payload::new(
                new_arena,
                self.group_types.clone(),
                self.aggrs.clone(),
                states_layout.clone(),
            );

            let old = std::mem::replace(&mut self.payloads[bucket], new_payload);
            taken.push(old);
        }

        Ok(taken)
    }

    pub fn mark_min_cardinality(&mut self) {
        for payload in self.payloads.iter_mut() {
            payload.mark_min_cardinality();
        }
    }

    pub fn append_rows(
        &mut self,
        state: &mut ProbeState,
        new_group_rows: usize,
        group_columns: ProjectedBlock,
    ) {
        if self.payloads.len() == 1 {
            self.payloads[0].reserve_append_rows(
                &state.empty_vector,
                &state.group_hashes,
                &mut state.addresses,
                &mut state.page_index,
                new_group_rows,
                group_columns,
            );
        } else {
            // generate partition selection indices
            state.reset_partitions(self.partition_count());
            let select_vector = &state.empty_vector;

            for idx in select_vector.iter().take(new_group_rows).copied() {
                let hash = state.group_hashes[idx];
                let partition_idx = ((hash & self.mask_v) >> self.shift_v) as usize;
                let sel = &mut state.partition_entries[partition_idx];

                sel[state.partition_count[partition_idx]] = idx;
                state.partition_count[partition_idx] += 1;
            }

            for partition_index in 0..self.payloads.len() {
                let count = state.partition_count[partition_index];
                if count > 0 {
                    let sel = &state.partition_entries[partition_index];

                    self.payloads[partition_index].reserve_append_rows(
                        sel,
                        &state.group_hashes,
                        &mut state.addresses,
                        &mut state.page_index,
                        count,
                        group_columns,
                    );
                }
            }
        }
    }

    pub fn repartition(self, new_partition_count: usize, state: &mut PayloadFlushState) -> Self {
        if self.partition_count() == new_partition_count {
            return self;
        }

        let mut new_partition_payload = if self.separated_arenas {
            PartitionedPayload::new_with_partition_bumps(
                self.group_types.clone(),
                self.aggrs.clone(),
                new_partition_count as u64,
            )
        } else {
            PartitionedPayload::new(
                self.group_types.clone(),
                self.aggrs.clone(),
                new_partition_count as u64,
                self.arenas.clone(),
            )
        };

        new_partition_payload.combine(self, state);
        new_partition_payload
    }

    pub fn combine(&mut self, other: PartitionedPayload, state: &mut PayloadFlushState) {
        if other.partition_count == self.partition_count {
            for (l, r) in self.payloads.iter_mut().zip(other.payloads.into_iter()) {
                l.combine(r);
            }
        } else {
            state.clear();

            for payload in other.payloads.into_iter() {
                self.combine_single(payload, state, None)
            }
        }
    }

    pub fn combine_single(
        &mut self,
        mut other: Payload,
        state: &mut PayloadFlushState,
        only_bucket: Option<usize>,
    ) {
        if other.len() == 0 {
            return;
        }

        if self.partition_count == 1 {
            self.payloads[0].combine(other);
        } else {
            state.clear();

            // flush for other's each page to correct partition
            while self.gather_flush(&other, state) {
                // copy rows
                for partition in (0..self.partition_count as usize)
                    .filter(|x| only_bucket.is_none() || only_bucket == Some(*x))
                {
                    let payload = &mut self.payloads[partition];
                    let count = state.probe_state.partition_count[partition];

                    if count > 0 {
                        let sel = &state.probe_state.partition_entries[partition];
                        payload.copy_rows(sel, count, &state.addresses);
                    }
                }
            }
            other.state_move_out = true;
        }
    }

    // for each page's row, compute which partition it belongs to
    pub fn gather_flush(&self, other: &Payload, state: &mut PayloadFlushState) -> bool {
        if state.flush_page >= other.pages.len() {
            return false;
        }

        let page = &other.pages[state.flush_page];

        // ToNext
        if state.flush_page_row >= page.rows {
            state.flush_page += 1;
            state.flush_page_row = 0;
            state.row_count = 0;
            return self.gather_flush(other, state);
        }

        let end = (state.flush_page_row + BATCH_SIZE).min(page.rows);
        let rows = end - state.flush_page_row;
        state.row_count = rows;

        state.probe_state.reset_partitions(self.partition_count());

        for idx in 0..rows {
            state.addresses[idx] = other.data_ptr(page, idx + state.flush_page_row);

            let hash = state.addresses[idx].hash(&self.row_layout);

            let partition_idx = ((hash & self.mask_v) >> self.shift_v) as usize;

            let sel = &mut state.probe_state.partition_entries[partition_idx];
            sel[state.probe_state.partition_count[partition_idx]] = idx;
            state.probe_state.partition_count[partition_idx] += 1;
        }
        state.flush_page_row = end;
        true
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.payloads.iter().map(|x| x.len()).sum()
    }

    #[inline]
    pub fn partition_count(&self) -> usize {
        self.partition_count as usize
    }

    #[allow(dead_code)]
    pub fn page_count(&self) -> usize {
        self.payloads.iter().map(|x| x.pages.len()).sum()
    }

    #[allow(dead_code)]
    pub fn memory_size(&self) -> usize {
        self.payloads.iter().map(|x| x.memory_size()).sum()
    }
}

#[inline]
fn shift(radix_bits: u64) -> u64 {
    48 - radix_bits
}

#[inline]
fn mask(radix_bits: u64) -> u64 {
    ((1 << radix_bits) - 1) << shift(radix_bits)
}
