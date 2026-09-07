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

use std::alloc::Layout;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::AggregateFunction;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::AggregateFunctionSpill;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::StateAddr;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::number::NumberColumnBuilder;

use super::aggregate_distinct_state::DistinctStateFunc;
use super::aggregate_spill::FunctionState;
use super::aggregate_spill::PARTITIONS;
use super::aggregate_spill::SPILL_BATCH_ROWS;
use super::aggregate_spill::SpillPartition;
use super::aggregate_spill::SpillState;
use super::aggregate_spill::spill_hash_partitions;
use super::aggregate_spill::split_spill_state;

pub(super) struct AggregateDistinctSpillFunction<State> {
    name: String,
    nested: AggregateFunctionRef,
    arguments: Vec<DataType>,
    spill: Arc<dyn AggregateFunctionSpill>,
    _state: PhantomData<fn(State)>,
}

impl<State: DistinctStateFunc> AggregateDistinctSpillFunction<State> {
    pub fn create(
        name: String,
        nested: AggregateFunctionRef,
        arguments: Vec<DataType>,
        spill: Arc<dyn AggregateFunctionSpill>,
    ) -> Result<AggregateFunctionRef> {
        let nested = nested.clone().with_spill(spill.clone())?.unwrap_or(nested);
        Ok(Arc::new(Self {
            name,
            nested,
            arguments,
            spill,
            _state: PhantomData,
        }))
    }

    fn state(place: AggrState<'_>) -> &mut State {
        AggrState::new(place.addr, &place.loc[..1]).get()
    }

    fn spilled(place: AggrState<'_>) -> &mut SpillState {
        AggrState::new(place.addr, &place.loc[1..2]).get()
    }

    fn spill_state(&self, state: &mut State, depth: u32) -> Result<Vec<SpillPartition>> {
        if state.is_empty() {
            return Ok(vec![]);
        }
        let rows = state.len();
        let entries = state.spill_entries(&self.arguments)?;
        let key_columns = (0..entries.len()).collect::<Vec<_>>();
        let runs = spill_hash_partitions(
            self.spill.as_ref(),
            DataBlock::new(entries, rows),
            &key_columns,
            depth,
        )?;
        *state = State::new();
        Ok(runs)
    }

    fn check_spill(&self, place: AggrState<'_>) -> Result<()> {
        let state = Self::state(place);
        if !state.is_empty() && self.spill.should_spill(state.memory_size()) {
            Self::spilled(place)
                .runs
                .extend(self.spill_state(state, 0)?);
        }
        Ok(())
    }

    fn visit_partitions(
        &self,
        place: AggrState<'_>,
        mut consume: impl FnMut(&mut State) -> Result<()>,
    ) -> Result<()> {
        if Self::spilled(place).runs.is_empty() {
            return consume(Self::state(place));
        }
        let runs = self.spill_state(Self::state(place), 0)?;
        Self::spilled(place).runs.extend(runs);
        let mut buckets = vec![Vec::new(); PARTITIONS];
        for run in &Self::spilled(place).runs {
            buckets[run.bucket].push(run.file.clone());
        }
        let mut tasks = buckets
            .into_iter()
            .filter(|files| !files.is_empty())
            .map(|files| (1, files))
            .collect::<Vec<_>>();
        let data_types = State::spill_types(&self.arguments);
        while let Some((depth, files)) = tasks.pop() {
            self.spill.check_interrupt()?;
            let mut state = State::new();
            let mut repartitioned = false;
            let mut children = vec![Vec::new(); PARTITIONS];
            for file in files {
                for block in self.spill.restore(&file, &data_types)? {
                    let block = block?;
                    for start in (0..block.num_rows()).step_by(SPILL_BATCH_ROWS) {
                        self.spill.check_interrupt()?;
                        let end = (start + SPILL_BATCH_ROWS).min(block.num_rows());
                        let block = block.slice(start..end);
                        state.merge_spilled(block.columns().into(), block.num_rows())?;
                        // A single value cannot be subdivided. The limit is soft
                        // by one input batch/value, like the processor spill limit.
                        if state.len() > 1
                            && state.memory_size() > self.spill.restore_memory_limit()
                        {
                            repartitioned = true;
                            for run in self.spill_state(&mut state, depth)? {
                                children[run.bucket].push(run.file);
                            }
                        }
                    }
                }
            }
            if repartitioned {
                for run in self.spill_state(&mut state, depth)? {
                    children[run.bucket].push(run.file);
                }
                tasks.extend(
                    children
                        .into_iter()
                        .filter(|files| !files.is_empty())
                        .map(|files| (depth + 1, files)),
                );
            } else {
                consume(&mut state)?;
            }
        }
        Ok(())
    }
}

impl<State: DistinctStateFunc> AggregateFunction for AggregateDistinctSpillFunction<State> {
    fn state_memory_size(&self, place: AggrState) -> usize {
        Self::state(place).memory_size() + Self::spilled(place).memory_size()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataType> {
        self.nested.return_type()
    }

    fn init_state(&self, place: AggrState) {
        AggrState::new(place.addr, &place.loc[..1]).write(State::new);
        AggrState::new(place.addr, &place.loc[1..2]).write(SpillState::default);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<State>()));
        registry.register(AggrStateType::Custom(Layout::new::<SpillState>()));
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        for start in (0..input_rows).step_by(SPILL_BATCH_ROWS) {
            self.spill.check_interrupt()?;
            let end = (start + SPILL_BATCH_ROWS).min(input_rows);
            let columns = columns
                .iter()
                .map(|c| c.slice(start..end))
                .collect::<Vec<_>>();
            let validity = validity.map(|v| v.clone().sliced(start, end - start));
            Self::state(place).batch_add((&columns).into(), validity.as_ref(), end - start)?;
            self.check_spill(place)?;
        }
        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        Self::state(place).add(columns, row)?;
        // Poll by input rows, including duplicates: a set above the size threshold
        // must still respond to memory pressure when no new keys arrive.
        let spilled = Self::spilled(place);
        spilled.rows_since_check += 1;
        if spilled.rows_since_check >= SPILL_BATCH_ROWS {
            spilled.rows_since_check = 0;
            self.check_spill(place)?;
        }
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        State::serialize_type(None)
            .into_iter()
            .chain([StateSerdeItem::Binary(None)])
            .collect()
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let n = builders.len() - 1;
        for addr in places {
            let place = AggrState::new(*addr, loc);
            self.check_spill(place)?;
            if !Self::spilled(place).runs.is_empty() {
                let runs = self.spill_state(Self::state(place), 0)?;
                Self::spilled(place).runs.extend(runs);
            }
            let metadata = Self::spilled(place).encode()?;
            let builder = builders[n].as_binary_mut().unwrap();
            builder.put_slice(&metadata);
            builder.commit_row();
        }
        State::batch_serialize(places, &loc[..1], &mut builders[..n])
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        for (row, addr) in places.iter().enumerate() {
            if filter.is_some_and(|v| !v.get_bit(row)) {
                continue;
            }
            let place = AggrState::new(*addr, loc);
            let (state, metadata) = split_spill_state(
                &state.slice(row..row + 1),
                State::serialize_type(None).len(),
            )?;
            Self::spilled(place).merge(&metadata)?;
            State::visit_serialized(&state, |block| {
                Self::state(place).merge_spilled(block.columns().into(), block.num_rows())?;
                self.check_spill(place)
            })?;
        }
        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        Self::state(place).merge(Self::state(rhs))?;
        Self::spilled(place)
            .runs
            .extend(Self::spilled(rhs).runs.iter().cloned());
        self.check_spill(place)
    }

    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        if self.nested.name() == "AggregateCountFunction" {
            let mut count = 0_u64;
            self.visit_partitions(place, |state| {
                count += state.len() as u64;
                Ok(())
            })?;
            let ColumnBuilder::Number(NumberColumnBuilder::UInt64(builder)) = builder else {
                unreachable!()
            };
            builder.push(count);
        } else {
            // The reducer spans partitions; DISTINCT values do not. A fresh
            // reducer also makes repeated read-only finalization idempotent.
            let nested = FunctionState::new(self.nested.clone())?;
            self.visit_partitions(place, |state| {
                let entries = state.build_entries(&self.arguments)?;
                nested
                    .function
                    .accumulate(nested.place(), (&entries).into(), None, state.len())
            })?;
            nested
                .function
                .merge_result(nested.place(), false, builder)?;
        }
        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        unsafe {
            std::ptr::drop_in_place(Self::state(place));
            std::ptr::drop_in_place(Self::spilled(place));
        }
    }

    fn get_if_condition(&self, columns: ProjectedBlock) -> Option<Bitmap> {
        self.nested.get_if_condition(columns)
    }
}

impl<State> fmt::Display for AggregateDistinctSpillFunction<State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}
