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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::AggregateFunction;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::AggregateFunctionSpill;
use databend_common_expression::AggregateSpillReader;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateAddr;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;

use super::aggregate_spill::FunctionState;
use super::aggregate_spill::SPILL_BATCH_ROWS;
use super::aggregate_spill::SpillPartition;
use super::aggregate_spill::SpillState;
use super::aggregate_spill::split_spill_state;

/// Functions with a streaming final merge can use the common run lifecycle.
/// Unlike hash partitioning, run order is retained. The merger must consume
/// bounded states, and must not reconstruct an unbounded intermediate state.
/// An output such as ARRAY_AGG can still require memory proportional to the
/// final SQL value; that allocation belongs to the output builder.
pub(super) trait AggregateSpillResult: Send + Sync {
    /// Feed bounded pieces of a durable state to the in-memory merger. Pieces
    /// must compose in order to exactly the original state, including when a
    /// persisted state is larger than the current query's spill limit.
    fn visit_serialized(
        &self,
        state: &BlockEntry,
        _memory_limit: usize,
        consume: &mut dyn FnMut(BlockEntry) -> Result<()>,
    ) -> Result<()> {
        consume(state.clone())
    }

    fn merge_result(
        &self,
        function: &AggregateFunctionRef,
        spill: Arc<dyn AggregateFunctionSpill>,
        states: &mut dyn Iterator<Item = Result<DataBlock>>,
        builder: &mut ColumnBuilder,
    ) -> Result<()>;
}

#[derive(Default)]
struct StreamingState {
    spill: SpillState,
    pending_rows: usize,
}

pub(super) struct AggregateStreamingSpillFunction {
    inner: AggregateFunctionRef,
    spill: Arc<dyn AggregateFunctionSpill>,
    result: Arc<dyn AggregateSpillResult>,
}

impl AggregateStreamingSpillFunction {
    pub fn create(
        inner: AggregateFunctionRef,
        spill: Arc<dyn AggregateFunctionSpill>,
        result: Arc<dyn AggregateSpillResult>,
    ) -> AggregateFunctionRef {
        Arc::new(Self {
            inner,
            spill,
            result,
        })
    }

    fn state(place: AggrState<'_>) -> &mut StreamingState {
        AggrState::new(place.addr, &place.loc[..1]).get()
    }

    fn flush(&self, place: AggrState<'_>) -> Result<()> {
        if Self::state(place).pending_rows == 0 {
            return Ok(());
        }
        self.spill.check_interrupt()?;
        let mut builder = ColumnBuilder::with_capacity(&self.inner.serialize_data_type(), 1);
        self.inner.batch_serialize(
            &[place.addr],
            &place.loc[1..],
            builder.as_tuple_mut().unwrap(),
        )?;
        let file = self
            .spill
            .spill(DataBlock::new_from_columns(vec![builder.build()]))?;
        Self::state(place)
            .spill
            .runs
            .push(SpillPartition { bucket: 0, file });
        if self.inner.need_manual_drop_state() {
            unsafe { self.inner.drop_state(place.remove_first_loc()) };
        }
        self.inner.init_state(place.remove_first_loc());
        Self::state(place).pending_rows = 0;
        Ok(())
    }

    fn check_spill(&self, place: AggrState<'_>) -> Result<()> {
        if Self::state(place).pending_rows != 0
            && self
                .spill
                .should_spill(self.inner.state_memory_size(place.remove_first_loc()))
        {
            self.flush(place)?;
        }
        Ok(())
    }
}

impl AggregateFunction for AggregateStreamingSpillFunction {
    fn state_memory_size(&self, place: AggrState) -> usize {
        self.inner.state_memory_size(place.remove_first_loc())
            + Self::state(place).spill.memory_size()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
    fn return_type(&self) -> Result<DataType> {
        self.inner.return_type()
    }

    fn init_state(&self, place: AggrState) {
        AggrState::new(place.addr, &place.loc[..1]).write(StreamingState::default);
        self.inner.init_state(place.remove_first_loc());
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<StreamingState>()));
        self.inner.register_state(registry);
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
            self.inner.accumulate(
                place.remove_first_loc(),
                (&columns).into(),
                validity.as_ref(),
                end - start,
            )?;
            Self::state(place).pending_rows += end - start;
            self.check_spill(place)?;
        }
        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        self.inner
            .accumulate_row(place.remove_first_loc(), columns, row)?;
        let state = Self::state(place);
        state.pending_rows += 1;
        if state.pending_rows.is_multiple_of(SPILL_BATCH_ROWS) {
            self.check_spill(place)?;
        }
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        self.inner
            .serialize_type()
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
            if !Self::state(place).spill.runs.is_empty() {
                self.flush(place)?;
            }
            let bytes = Self::state(place).spill.encode()?;
            let builder = builders[n].as_binary_mut().unwrap();
            builder.put_slice(&bytes);
            builder.commit_row();
        }
        self.inner
            .batch_serialize(places, &loc[1..], &mut builders[..n])
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
            let (state, bytes) = split_spill_state(
                &state.slice(row..row + 1),
                self.inner.serialize_type().len(),
            )?;
            if !bytes.is_empty() {
                self.flush(place)?;
                Self::state(place).spill.merge(&bytes)?;
            }
            self.result.visit_serialized(
                &state,
                self.spill.restore_memory_limit(),
                &mut |state| {
                    self.inner.batch_merge(&[*addr], &loc[1..], &state, None)?;
                    Self::state(place).pending_rows += 1;
                    self.check_spill(place)
                },
            )?;
        }
        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        if !Self::state(rhs).spill.runs.is_empty() {
            self.flush(place)?;
            Self::state(place)
                .spill
                .runs
                .extend(Self::state(rhs).spill.runs.iter().cloned());
        }
        self.inner
            .merge_states(place.remove_first_loc(), rhs.remove_first_loc())?;
        Self::state(place).pending_rows += Self::state(rhs).pending_rows;
        self.check_spill(place)
    }

    fn merge_result(
        &self,
        place: AggrState,
        read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        if Self::state(place).spill.runs.is_empty() {
            return self
                .inner
                .merge_result(place.remove_first_loc(), read_only, builder);
        }
        self.flush(place)?;
        let data_types = [self.inner.serialize_data_type()];
        let files = &Self::state(place).spill.runs;
        let mut index = 0;
        let mut reader: Option<AggregateSpillReader> = None;
        let mut states = std::iter::from_fn(|| -> Option<Result<DataBlock>> {
            loop {
                if let Some(reader) = &mut reader {
                    if let Some(block) = Iterator::next(reader) {
                        return Some(block);
                    }
                }
                reader = None;
                let run = files.get(index)?;
                index += 1;
                match self.spill.restore(&run.file, &data_types) {
                    Ok(next) => reader = Some(next),
                    Err(error) => return Some(Err(error)),
                }
            }
        });
        self.result
            .merge_result(&self.inner, self.spill.clone(), &mut states, builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }
    unsafe fn drop_state(&self, place: AggrState) {
        unsafe {
            std::ptr::drop_in_place(Self::state(place));
            if self.inner.need_manual_drop_state() {
                self.inner.drop_state(place.remove_first_loc());
            }
        }
    }
    fn get_if_condition(&self, columns: ProjectedBlock) -> Option<Bitmap> {
        self.inner.get_if_condition(columns)
    }
}

impl fmt::Display for AggregateStreamingSpillFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

pub(super) struct ArraySpillResult;

impl AggregateSpillResult for ArraySpillResult {
    fn visit_serialized(
        &self,
        state: &BlockEntry,
        _memory_limit: usize,
        consume: &mut dyn FnMut(BlockEntry) -> Result<()>,
    ) -> Result<()> {
        let column = state.to_column();
        let fields = column.as_tuple().unwrap();
        let ScalarRef::Array(values) = fields[0].index(0).unwrap() else {
            unreachable!()
        };
        for start in (0..values.len()).step_by(SPILL_BATCH_ROWS) {
            let end = (start + SPILL_BATCH_ROWS).min(values.len());
            consume(BlockEntry::new_const_column(
                state.data_type(),
                Scalar::Tuple(vec![Scalar::Array(values.slice(start..end))]),
                1,
            ))?;
        }
        Ok(())
    }

    fn merge_result(
        &self,
        function: &AggregateFunctionRef,
        _spill: Arc<dyn AggregateFunctionSpill>,
        states: &mut dyn Iterator<Item = Result<DataBlock>>,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let output = builder.as_array_mut().unwrap();
        for block in states {
            let block = block?;
            for row in 0..block.num_rows() {
                let state = FunctionState::new(function.clone())?;
                function.batch_merge(
                    &[state.addr],
                    &state.layout.states_loc[0],
                    &block.get_by_offset(0).slice(row..row + 1),
                    None,
                )?;
                let mut result = ColumnBuilder::with_capacity(&function.return_type()?, 1);
                function.merge_result(state.place(), false, &mut result)?;
                let result = result.build();
                let databend_common_expression::ScalarRef::Array(values) = result.index(0).unwrap()
                else {
                    unreachable!()
                };
                output.builder.append_column(&values);
            }
        }
        output.commit_row();
        Ok(())
    }
}

pub(super) struct StringSpillResult {
    pub delimiter: String,
}

impl AggregateSpillResult for StringSpillResult {
    fn visit_serialized(
        &self,
        state: &BlockEntry,
        memory_limit: usize,
        consume: &mut dyn FnMut(BlockEntry) -> Result<()>,
    ) -> Result<()> {
        let Column::Tuple(fields) = state.to_column() else {
            unreachable!()
        };
        let mut value = fields[0].as_string().unwrap().index(0).unwrap();
        while !value.is_empty() {
            let mut end = value.len().min(memory_limit.max(4));
            while !value.is_char_boundary(end) {
                end -= 1;
            }
            consume(BlockEntry::new_const_column(
                state.data_type(),
                Scalar::Tuple(vec![Scalar::String(value[..end].to_owned())]),
                1,
            ))?;
            value = &value[end..];
        }
        Ok(())
    }

    fn merge_result(
        &self,
        _function: &AggregateFunctionRef,
        _spill: Arc<dyn AggregateFunctionSpill>,
        states: &mut dyn Iterator<Item = Result<DataBlock>>,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let output = builder.as_string_mut().unwrap();
        let mut last = String::new();
        for block in states {
            let block = block?;
            let column = block.get_by_offset(0).to_column();
            let fields = column.as_tuple().unwrap();
            for value in fields[0].as_string().unwrap().iter() {
                if !value.is_empty() {
                    last.push_str(value);
                    // A durable state can be split inside the delimiter. Keep
                    // its trailing bytes across runs and strip only once.
                    let mut end = last.len().saturating_sub(self.delimiter.len());
                    while !last.is_char_boundary(end) {
                        end -= 1;
                    }
                    output.put_str(&last[..end]);
                    last.drain(..end);
                }
            }
        }
        if !last.is_empty() {
            let value = last.strip_suffix(&self.delimiter).ok_or_else(|| {
                databend_common_exception::ErrorCode::BadBytes(
                    "Invalid spilled STRING_AGG delimiter",
                )
            })?;
            output.put_str(value);
        }
        output.commit_row();
        Ok(())
    }
}
