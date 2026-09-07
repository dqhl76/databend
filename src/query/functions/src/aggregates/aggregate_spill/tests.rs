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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::AggregateFunctionSpill;
use databend_common_expression::AggregateSpillFile;
use databend_common_expression::AggregateSpillReader;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::SymbolOrOffset;
use databend_common_expression::group_hash_entries;
use databend_common_expression::types::*;

use super::FunctionState;
use super::PARTITIONS;
use crate::aggregates::AggregateFunctionFactory;
use crate::aggregates::AggregateFunctionSortDesc;

struct TestSpill {
    files: Mutex<HashMap<String, DataBlock>>,
    force: AtomicBool,
    pressure: AtomicBool,
    fail_write: AtomicBool,
    fail_read: AtomicBool,
    interrupted: AtomicBool,
    readers: Arc<AtomicUsize>,
    max_readers: AtomicUsize,
    max_state_bytes: AtomicUsize,
    limit: usize,
}

impl TestSpill {
    fn new(limit: usize, force: bool) -> Arc<Self> {
        Arc::new(Self {
            files: Mutex::new(HashMap::new()),
            force: AtomicBool::new(force),
            pressure: AtomicBool::new(false),
            fail_write: AtomicBool::new(false),
            fail_read: AtomicBool::new(false),
            interrupted: AtomicBool::new(false),
            readers: Arc::new(AtomicUsize::new(0)),
            max_readers: AtomicUsize::new(0),
            max_state_bytes: AtomicUsize::new(0),
            limit,
        })
    }
}

impl AggregateFunctionSpill for TestSpill {
    fn restore_memory_limit(&self) -> usize {
        self.limit
    }
    fn should_spill(&self, bytes: usize) -> bool {
        self.max_state_bytes.fetch_max(bytes, Ordering::Relaxed);
        self.force.load(Ordering::Relaxed) || self.pressure.load(Ordering::Relaxed)
    }
    fn check_interrupt(&self) -> Result<()> {
        if self.interrupted.load(Ordering::Relaxed) {
            Err(ErrorCode::AbortedQuery("injected cancellation"))
        } else {
            Ok(())
        }
    }
    fn spill(&self, block: DataBlock) -> Result<AggregateSpillFile> {
        if self.fail_write.load(Ordering::Relaxed) {
            return Err(ErrorCode::Internal("injected spill failure"));
        }
        let mut files = self.files.lock().unwrap();
        let location = format!("run-{}", files.len());
        files.insert(location.clone(), block);
        Ok(AggregateSpillFile {
            location,
            metadata: vec![],
        })
    }
    fn restore(
        &self,
        file: &AggregateSpillFile,
        data_types: &[DataType],
    ) -> Result<AggregateSpillReader> {
        if self.fail_read.load(Ordering::Relaxed) {
            return Ok(Box::new(std::iter::once(Err(ErrorCode::Internal(
                "injected restore failure",
            )))));
        }
        let block = self
            .files
            .lock()
            .unwrap()
            .get(&file.location)
            .unwrap()
            .clone();
        assert_eq!(
            block
                .columns()
                .iter()
                .map(BlockEntry::data_type)
                .collect::<Vec<_>>(),
            data_types
        );
        let readers = self.readers.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_readers.fetch_max(readers, Ordering::Relaxed);
        Ok(Box::new(TestReader {
            block: Some(block),
            readers: self.readers.clone(),
        }))
    }
}

struct TestReader {
    block: Option<DataBlock>,
    readers: Arc<AtomicUsize>,
}

impl Iterator for TestReader {
    type Item = Result<DataBlock>;
    fn next(&mut self) -> Option<Self::Item> {
        self.block.take().map(Ok)
    }
}

impl Drop for TestReader {
    fn drop(&mut self) {
        self.readers.fetch_sub(1, Ordering::Relaxed);
    }
}

fn function(name: &str, params: Vec<Scalar>, block: &DataBlock) -> Result<AggregateFunctionRef> {
    AggregateFunctionFactory::instance().get(
        name,
        params,
        block.columns().iter().map(BlockEntry::data_type).collect(),
        vec![],
    )
}

fn add(state: &FunctionState, block: &DataBlock) -> Result<()> {
    state.function.accumulate(
        state.place(),
        block.columns().into(),
        None,
        block.num_rows(),
    )
}

fn serialize(state: &FunctionState) -> Result<BlockEntry> {
    let mut builders = state.layout.serialize_builders(1);
    state.function.batch_serialize(
        &[state.addr],
        &state.layout.states_loc[0],
        builders[0].as_tuple_mut().unwrap(),
    )?;
    Ok(builders.remove(0).build().into())
}

fn merge(state: &FunctionState, serialized: &BlockEntry) -> Result<()> {
    state
        .function
        .batch_merge(&[state.addr], &state.layout.states_loc[0], serialized, None)
}

fn result(state: &FunctionState) -> Result<Scalar> {
    let mut builder = ColumnBuilder::with_capacity(&state.function.return_type()?, 1);
    state
        .function
        .merge_result(state.place(), true, &mut builder)?;
    Ok(builder.build().index(0).unwrap().to_owned())
}

/// Compare a multi-stage, partially spilled execution to the durable/in-memory
/// implementation. Drop partial states before restore, mix inline and spilled
/// states, and exercise repeated read-only finalization.
fn check_round_trip(
    name: &str,
    params: Vec<Scalar>,
    block: DataBlock,
    duplicate: bool,
) -> Result<()> {
    let original = function(name, params, &block)?;
    let expected = FunctionState::new(original.clone())?;
    add(&expected, &block)?;
    if duplicate {
        add(&expected, &block)?;
    }
    let expected = result(&expected)?;
    let spill = TestSpill::new(1024 * 1024, true);
    let runtime = original.clone().with_spill(spill.clone())?.unwrap();
    let final_state = FunctionState::new(runtime.clone())?;
    let half = block.num_rows() / 2;
    for range in [0..half, half..block.num_rows()] {
        let partial = FunctionState::new(runtime.clone())?;
        add(&partial, &block.slice(range))?;
        let serialized = serialize(&partial)?;
        drop(partial);
        merge(&final_state, &serialized)?;
    }
    // Old durable states do not contain query-local spill references.
    if duplicate {
        let durable = FunctionState::new(original)?;
        add(&durable, &block)?;
        spill.force.store(false, Ordering::Relaxed);
        merge(&final_state, &serialize(&durable)?)?;
    }
    let serialized = serialize(&final_state)?;
    drop(final_state);
    let restored = FunctionState::new(runtime)?;
    merge(&restored, &serialized)?;
    assert!(!spill.files.lock().unwrap().is_empty());
    assert_eq!(result(&restored)?, expected);
    assert_eq!(result(&restored)?, expected);
    Ok(())
}

#[test]
fn distinct_spill_across_partial_states() -> Result<()> {
    let values = vec![3_u64, 1, 2, 3, 2, 4, 1, 4];
    let numbers = UInt64Type::from_data(values);
    check_round_trip(
        "count_distinct",
        vec![],
        DataBlock::new_from_columns(vec![numbers.clone()]),
        true,
    )?;
    check_round_trip(
        "sum_distinct",
        vec![],
        DataBlock::new_from_columns(vec![numbers.clone()]),
        true,
    )?;
    check_round_trip(
        "avg_distinct",
        vec![],
        DataBlock::new_from_columns(vec![numbers.clone()]),
        true,
    )?;
    let strings =
        StringType::from_data_with_validity(vec!["a", "b", "a", "c", "b", "d", "a", "d"], vec![
            true, true, true, false, true, true, false, true,
        ]);
    check_round_trip(
        "count_distinct",
        vec![],
        DataBlock::new_from_columns(vec![strings.clone()]),
        true,
    )?;
    check_round_trip(
        "uniq",
        vec![],
        DataBlock::new_from_columns(vec![strings.clone()]),
        true,
    )?;
    check_round_trip(
        "count_distinct",
        vec![],
        DataBlock::new_from_columns(vec![numbers, strings]),
        true,
    )?;
    check_round_trip(
        "count_distinct",
        vec![],
        DataBlock::new_from_columns(vec![DateType::from_data(vec![1, 2, 1, 3])]),
        true,
    )?;
    check_round_trip(
        "count_distinct",
        vec![],
        DataBlock::new_from_columns(vec![TimestampType::from_data(vec![1, 2, 1, 3])]),
        true,
    )?;
    Ok(())
}

#[test]
fn large_states_do_not_spill_without_pressure() -> Result<()> {
    let block = DataBlock::new_from_columns(vec![UInt64Type::from_data(
        (0..4096_u64).collect::<Vec<_>>(),
    )]);
    for name in ["count_distinct", "sum_distinct", "array_agg", "median"] {
        let original = function(name, vec![], &block)?;
        let expected = FunctionState::new(original.clone())?;
        add(&expected, &block)?;
        let spill = TestSpill::new(128, false);
        let runtime = original.with_spill(spill.clone())?.unwrap();
        let final_state = FunctionState::new(runtime.clone())?;
        for range in [0..2048, 2048..4096] {
            let partial = FunctionState::new(runtime.clone())?;
            add(&partial, &block.slice(range))?;
            assert!(spill.max_state_bytes.load(Ordering::Relaxed) > spill.restore_memory_limit());
            merge(&final_state, &serialize(&partial)?)?;
        }
        let transported = serialize(&final_state)?;
        let restored = FunctionState::new(runtime)?;
        merge(&restored, &transported)?;
        assert_eq!(result(&restored)?, result(&expected)?);
        assert!(spill.files.lock().unwrap().is_empty(), "{name}");
    }
    Ok(())
}

#[test]
fn grouped_distinct_checks_pressure_even_when_all_new_rows_are_duplicates() -> Result<()> {
    let block = DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![7])]);
    let spill = TestSpill::new(usize::MAX, false);
    let runtime = function("count_distinct", vec![], &block)?
        .with_spill(spill.clone())?
        .unwrap();
    let state = FunctionState::new(runtime)?;
    for _ in 0..2048 {
        state
            .function
            .accumulate_row(state.place(), block.columns().into(), 0)?;
    }
    assert!(spill.files.lock().unwrap().is_empty());
    spill.pressure.store(true, Ordering::Relaxed);
    for _ in 0..2048 {
        state
            .function
            .accumulate_row(state.place(), block.columns().into(), 0)?;
    }
    assert!(!spill.files.lock().unwrap().is_empty());
    spill.pressure.store(false, Ordering::Relaxed);
    assert_eq!(result(&state)?, Scalar::Number(NumberScalar::UInt64(1)));
    Ok(())
}

#[test]
fn restore_repartitions_a_large_single_bucket() -> Result<()> {
    let candidates = DataBlock::new_from_columns(vec![UInt64Type::from_data(
        (0_u64..50000).collect::<Vec<_>>(),
    )]);
    let mut hashes = vec![0; candidates.num_rows()];
    group_hash_entries(candidates.columns().into(), &mut hashes);
    let values = hashes
        .iter()
        .enumerate()
        .filter(|(_, hash)| **hash & (PARTITIONS as u64 - 1) == 0)
        .take(512)
        .map(|(value, _)| value as u64)
        .collect::<Vec<_>>();
    assert_eq!(values.len(), 512);
    let block = DataBlock::new_from_columns(vec![UInt64Type::from_data(values)]);
    let spill = TestSpill::new(2048, false);
    spill.pressure.store(true, Ordering::Relaxed);
    let function = function("count_distinct", vec![], &block)?
        .with_spill(spill.clone())?
        .unwrap();
    let state = FunctionState::new(function)?;
    add(&state, &block)?;
    let initial_runs = spill.files.lock().unwrap().len();
    assert_eq!(initial_runs, 1);
    // Pressure has cleared, but the independent restore budget still requires
    // repartitioning the large bucket rather than reconstructing the full set.
    spill.pressure.store(false, Ordering::Relaxed);
    assert_eq!(result(&state)?, Scalar::Number(NumberScalar::UInt64(512)));
    assert!(spill.files.lock().unwrap().len() > initial_runs);
    Ok(())
}

#[test]
fn streaming_spill_preserves_values_nulls_and_delimiters() -> Result<()> {
    let strings = DataBlock::new_from_columns(vec![StringType::from_data_with_validity(
        vec!["a", "", "ignored", "b", "", "c"],
        vec![true, true, false, true, true, true],
    )]);
    check_round_trip("array_agg", vec![], strings.clone(), false)?;
    check_round_trip(
        "string_agg",
        vec![Scalar::String("|".into())],
        strings,
        false,
    )?;
    check_round_trip(
        "array_agg",
        vec![],
        DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![1, 2, 3, 4])]),
        false,
    )?;
    Ok(())
}

#[test]
fn spilling_propagates_io_errors() -> Result<()> {
    let block = DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![1, 2, 3])]);
    let spill = TestSpill::new(1024, true);
    let runtime = function("count_distinct", vec![], &block)?
        .with_spill(spill.clone())?
        .unwrap();
    let state = FunctionState::new(runtime)?;
    spill.fail_write.store(true, Ordering::Relaxed);
    assert!(add(&state, &block).is_err());
    spill.fail_write.store(false, Ordering::Relaxed);
    add(&state, &block)?;
    spill.fail_read.store(true, Ordering::Relaxed);
    assert!(result(&state).is_err());
    spill.fail_read.store(false, Ordering::Relaxed);
    assert_eq!(result(&state)?, Scalar::Number(NumberScalar::UInt64(3)));
    Ok(())
}

#[test]
fn distinct_float_keys_and_grouped_state_merges() -> Result<()> {
    let values = vec![
        0.0,
        -0.0,
        f64::NAN,
        f64::from_bits(0x7ff8000000000001),
        1.0,
        1.0,
        -1.0,
    ];
    let block = DataBlock::new_from_columns(vec![Float64Type::from_data(values)]);
    let original = function("count_distinct", vec![], &block)?;
    let expected = FunctionState::new(original.clone())?;
    add(&expected, &block)?;
    let spill = TestSpill::new(1024, true);
    let runtime = original.with_spill(spill.clone())?.unwrap();
    let left = FunctionState::new(runtime.clone())?;
    let right = FunctionState::new(runtime)?;
    for row in 0..3 {
        left.function
            .accumulate_row(left.place(), block.columns().into(), row)?;
    }
    // Serialize to spill the left group, then merge another group's inline keys.
    serialize(&left)?;
    spill.force.store(false, Ordering::Relaxed);
    for row in 3..block.num_rows() {
        right
            .function
            .accumulate_row(right.place(), block.columns().into(), row)?;
    }
    left.function.merge_states(left.place(), right.place())?;
    assert_eq!(result(&left)?, result(&expected)?);
    Ok(())
}

#[test]
fn durable_streaming_states_are_split_before_merging() -> Result<()> {
    let block =
        DataBlock::new_from_columns(vec![StringType::from_data(vec!["中文", "", "abc", "尾巴"])]);
    for (name, params) in [
        ("array_agg", vec![]),
        ("string_agg", vec![Scalar::String("分隔".into())]),
    ] {
        let original = function(name, params, &block)?;
        let durable = FunctionState::new(original.clone())?;
        add(&durable, &block)?;
        let expected = result(&durable)?;
        let spill = TestSpill::new(4, false);
        spill.pressure.store(true, Ordering::Relaxed);
        let runtime = original.with_spill(spill.clone())?.unwrap();
        let restored = FunctionState::new(runtime)?;
        merge(&restored, &serialize(&durable)?)?;
        assert!(!spill.files.lock().unwrap().is_empty());
        assert_eq!(result(&restored)?, expected);
    }
    Ok(())
}

#[test]
fn ordered_spill_uses_bounded_merge_and_preserves_sort_semantics() -> Result<()> {
    let strings = StringType::from_data(vec!["c", "a", "b", "d", "e", "f", "g", "h"]);
    let keys = Int32Type::from_data_with_validity(vec![2, 1, 1, 0, 3, 2, 0, 3], vec![
        true, true, true, false, true, true, false, true,
    ]);
    let block = DataBlock::new_from_columns(vec![strings, keys]);
    let descriptions = vec![
        AggregateFunctionSortDesc {
            index: SymbolOrOffset::Offset(1),
            is_reuse_index: false,
            data_type: block.get_by_offset(1).data_type(),
            asc: false,
            nulls_first: true,
        },
        AggregateFunctionSortDesc {
            index: SymbolOrOffset::Offset(0),
            is_reuse_index: true,
            data_type: DataType::String,
            asc: true,
            nulls_first: false,
        },
    ];
    for (name, params) in [
        ("array_agg", vec![]),
        ("string_agg", vec![Scalar::String("|".into())]),
    ] {
        let original = AggregateFunctionFactory::instance().get(
            name,
            params,
            vec![DataType::String],
            descriptions.clone(),
        )?;
        let expected = FunctionState::new(original.clone())?;
        add(&expected, &block)?;
        let expected = result(&expected)?;
        let spill = TestSpill::new(1024, true);
        let runtime = original.with_spill(spill.clone())?.unwrap();
        let final_state = FunctionState::new(runtime.clone())?;
        // Eight independent partial runs require several merge passes.
        for row in 0..block.num_rows() {
            let partial = FunctionState::new(runtime.clone())?;
            add(&partial, &block.slice(row..row + 1))?;
            merge(&final_state, &serialize(&partial)?)?;
        }
        assert_eq!(result(&final_state)?, expected);
        assert_eq!(result(&final_state)?, expected);
        assert!(spill.max_readers.load(Ordering::Relaxed) <= 2);
        assert_eq!(spill.readers.load(Ordering::Relaxed), 0);
    }
    Ok(())
}

#[test]
fn exact_quantiles_merge_values_instead_of_partial_quantiles() -> Result<()> {
    let block = DataBlock::new_from_columns(vec![Int64Type::from_data(vec![
        0, 0, 1, 1, 20, 30, 40, 100,
    ])]);
    check_round_trip("median", vec![], block.clone(), true)?;
    check_round_trip(
        "quantile_cont",
        vec![Scalar::Number(NumberScalar::Float64(0.75.into()))],
        block.clone(),
        true,
    )?;
    check_round_trip(
        "quantile_cont",
        vec![0.0, 0.5, 0.75, 1.0]
            .into_iter()
            .map(|v| Scalar::Number(NumberScalar::Float64(v.into())))
            .collect(),
        block,
        true,
    )?;
    Ok(())
}

#[test]
fn durable_state_functions_do_not_export_spill_references() -> Result<()> {
    let block = DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![1, 2, 1, 3])]);
    let spill = TestSpill::new(1, true);
    for name in ["uniq_state", "array_agg_state", "median_state"] {
        let original = function(name, vec![], &block)?;
        assert_eq!(
            original.serialize_data_type(),
            databend_common_expression::StateSerdeType::new(original.spill_serialize_type())
                .data_type()
        );
        assert!(original.with_spill(spill.clone())?.is_none());
    }
    assert!(spill.files.lock().unwrap().is_empty());
    Ok(())
}

#[test]
fn restore_observes_cancellation_and_releases_readers() -> Result<()> {
    let block = DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![1, 2, 3])]);
    let spill = TestSpill::new(1024, true);
    let runtime = function("count_distinct", vec![], &block)?
        .with_spill(spill.clone())?
        .unwrap();
    let state = FunctionState::new(runtime)?;
    add(&state, &block)?;
    spill.interrupted.store(true, Ordering::Relaxed);
    assert!(result(&state).is_err());
    assert_eq!(spill.readers.load(Ordering::Relaxed), 0);
    spill.interrupted.store(false, Ordering::Relaxed);
    assert_eq!(result(&state)?, Scalar::Number(NumberScalar::UInt64(3)));
    Ok(())
}

#[test]
fn empty_and_all_null_states_keep_their_result_contract() -> Result<()> {
    let all_null = DataBlock::new_from_columns(vec![UInt64Type::from_data_with_validity(
        vec![0, 0, 0],
        vec![false, false, false],
    )]);
    for name in ["count_distinct", "sum_distinct", "array_agg", "median"] {
        for block in [all_null.slice(0..0), all_null.clone()] {
            let original = function(name, vec![], &block)?;
            let expected = FunctionState::new(original.clone())?;
            add(&expected, &block)?;
            let spill = TestSpill::new(1024, true);
            let runtime = original.with_spill(spill)?.unwrap();
            let partial = FunctionState::new(runtime.clone())?;
            add(&partial, &block)?;
            let final_state = FunctionState::new(runtime)?;
            merge(&final_state, &serialize(&partial)?)?;
            assert_eq!(result(&final_state)?, result(&expected)?);
        }
    }
    Ok(())
}

#[test]
fn filter_adaptors_forward_function_spilling() -> Result<()> {
    let block = DataBlock::new_from_columns(vec![
        UInt64Type::from_data(vec![1, 2, 1, 3, 4, 2]),
        BooleanType::from_data(vec![true, false, true, true, false, true]),
    ]);
    check_round_trip("uniq_if", vec![], block, true)?;
    let strings = DataBlock::new_from_columns(vec![
        StringType::from_data(vec!["a", "b", "c", "d"]),
        BooleanType::from_data(vec![true, false, false, true]),
    ]);
    check_round_trip(
        "string_agg_if",
        vec![Scalar::String("|".into())],
        strings,
        false,
    )
}
