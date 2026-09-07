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

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::AggregateFunctionSpill;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::decimal::Decimal;

use super::aggregate_spill_sort::AggregateSpillSort;
use super::aggregate_streaming_spill::AggregateSpillResult;
use super::aggregate_streaming_spill::ArraySpillResult;

/// Exact quantiles need ordered values, not a merge of per-run quantiles.
/// Retain only the requested ranks while streaming the external merge.
pub(super) struct QuantileSpillResult<T: ValueType> {
    pub levels: Vec<f64>,
    pub interpolate: fn(T::Scalar, T::Scalar, f64) -> Result<T::Scalar>,
    pub _value: PhantomData<fn(T)>,
}

impl<T: ValueType> AggregateSpillResult for QuantileSpillResult<T> {
    fn visit_serialized(
        &self,
        state: &BlockEntry,
        memory_limit: usize,
        consume: &mut dyn FnMut(BlockEntry) -> Result<()>,
    ) -> Result<()> {
        ArraySpillResult.visit_serialized(state, memory_limit, consume)
    }

    fn merge_result(
        &self,
        function: &AggregateFunctionRef,
        spill: Arc<dyn AggregateFunctionSpill>,
        states: &mut dyn Iterator<Item = Result<DataBlock>>,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let mut sorter = AggregateSpillSort::new(spill.clone(), vec![SortColumnDescription {
            offset: 0,
            asc: true,
            nulls_first: false,
        }]);
        let mut count = 0;
        for block in states {
            let block = block?;
            let column = block.get_by_offset(0).to_column();
            let fields = column.as_tuple().unwrap();
            for row in 0..block.num_rows() {
                spill.check_interrupt()?;
                let ScalarRef::Array(values) = fields[0].index(row).unwrap() else {
                    unreachable!()
                };
                count += values.len();
                sorter.push(DataBlock::new_from_columns(vec![values]))?;
            }
        }
        if count == 0 {
            builder.push_default();
            return Ok(());
        }
        let positions = self
            .levels
            .iter()
            .map(|level| {
                let (fraction, whole) = libm::modf((count - 1) as f64 * level);
                (whole as usize, fraction)
            })
            .collect::<Vec<_>>();
        let mut samples: BTreeMap<usize, Option<T::Scalar>> = BTreeMap::new();
        for (whole, _) in &positions {
            samples.insert(*whole, None);
            samples.insert((*whole + 1).min(count - 1), None);
        }
        let mut offset = 0;
        sorter.finish(|block| {
            for (rank, sample) in samples.range_mut(offset..offset + block.num_rows()) {
                let value = block.get_by_offset(0).index(*rank - offset).unwrap();
                *sample = Some(T::to_owned_scalar(T::try_downcast_scalar(&value).unwrap()));
            }
            offset += block.num_rows();
            Ok(())
        })?;
        let return_type = function.return_type()?;
        let value_type = return_type
            .as_array()
            .map(|ty| ty.as_ref())
            .unwrap_or(&return_type);
        let values = positions
            .into_iter()
            .map(|(whole, fraction)| {
                let left = samples[&whole].clone().unwrap();
                let right = samples[&(whole + 1).min(count - 1)].clone().unwrap();
                (self.interpolate)(left, right, fraction)
                    .map(|value| T::upcast_scalar_with_type(value, value_type))
            })
            .collect::<Result<Vec<_>>>()?;
        if let Some(array) = builder.as_array_mut() {
            for value in values {
                array.builder.push(value.as_ref());
            }
            array.commit_row();
        } else {
            builder.push(values[0].as_ref());
        }
        Ok(())
    }
}

pub(super) fn interpolate_decimal<T: Decimal>(left: T, right: T, fraction: f64) -> Result<T> {
    right
        .checked_sub(left)
        .and_then(|difference| difference.checked_mul(Decimal::from_float(fraction)))
        .and_then(|part| left.checked_add(part))
        .ok_or_else(|| ErrorCode::Overflow("Decimal overflow when interpolate"))
}
