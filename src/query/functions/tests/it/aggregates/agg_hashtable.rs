// Copyright 2022 Datafuse Labs.
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
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_expression::block_debug::assert_block_value_sort_eq;
use databend_common_expression::get_states_layout;
use databend_common_expression::types::i256;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int16Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::Int8Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::aggregates::DecimalSumState;
use itertools::Itertools;

// cargo test --package databend-common-functions --test it -- aggregates::agg_hashtable::test_agg_hashtable --exact --nocapture
#[test]
fn test_agg_hashtable() {
    let factory = AggregateFunctionFactory::instance();
    let m: usize = 4;
    for n in [100, 1000, 10_000, 100_000] {
        let columns = vec![
            StringType::from_data((0..n).map(|x| format!("{}", x % m)).collect_vec()),
            Int64Type::from_data((0..n).map(|x| (x % m) as i64).collect_vec()),
            Int32Type::from_data((0..n).map(|x| (x % m) as i32).collect_vec()),
            Int16Type::from_data((0..n).map(|x| (x % m) as i16).collect_vec()),
            Int8Type::from_data((0..n).map(|x| (x % m) as i8).collect_vec()),
            Float32Type::from_data((0..n).map(|x| F32::from((x % m) as f32)).collect_vec()),
            Float64Type::from_data((0..n).map(|x| F64::from((x % m) as f64)).collect_vec()),
            BooleanType::from_data((0..n).map(|x| (x % m) != 0).collect_vec()),
        ];

        let group_columns = columns.clone();
        let group_types: Vec<_> = group_columns.iter().map(|c| c.data_type()).collect();

        let aggrs = vec![
            factory
                .get("min", vec![], vec![Int64Type::data_type()], vec![])
                .unwrap(),
            factory
                .get("max", vec![], vec![Int64Type::data_type()], vec![])
                .unwrap(),
            factory
                .get("sum", vec![], vec![Int64Type::data_type()], vec![])
                .unwrap(),
            factory
                .get("count", vec![], vec![Int64Type::data_type()], vec![])
                .unwrap(),
        ];

        let params: Vec<Vec<BlockEntry>> = aggrs
            .iter()
            .map(|_| vec![columns[1].clone().into()])
            .collect();
        let params = params.iter().map(|v| v.into()).collect_vec();

        let config = HashTableConfig::default();
        let mut hashtable = AggregateHashTable::new(
            group_types.clone(),
            aggrs.clone(),
            config.clone(),
            Arc::new(Bump::new()),
        );

        let mut state = ProbeState::default();
        let group_block_entries: Vec<BlockEntry> =
            group_columns.iter().map(|c| c.clone().into()).collect();
        let _ = hashtable
            .add_groups(
                &mut state,
                (&group_block_entries).into(),
                &params,
                (&[]).into(),
                n,
            )
            .unwrap();

        let mut hashtable2 = AggregateHashTable::new(
            group_types.clone(),
            aggrs.clone(),
            config.clone(),
            Arc::new(Bump::new()),
        );

        let mut state2 = ProbeState::default();
        let group_block_entries2: Vec<BlockEntry> =
            group_columns.iter().map(|c| c.clone().into()).collect();
        let _ = hashtable2
            .add_groups(
                &mut state2,
                (&group_block_entries2).into(),
                &params,
                (&[]).into(),
                n,
            )
            .unwrap();

        let mut flush_state = PayloadFlushState::default();
        let _ = hashtable.combine(hashtable2, &mut flush_state);

        let mut merge_state = PayloadFlushState::default();

        let mut blocks = Vec::new();
        loop {
            match hashtable.merge_result(&mut merge_state) {
                Ok(true) => {
                    let mut entries = merge_state.take_group_columns();
                    let agg_results = merge_state.take_aggregate_results();
                    entries.extend(agg_results.into_iter());

                    let num_rows = entries[0].len();
                    blocks.push(DataBlock::new(entries, num_rows));
                }
                Ok(false) => break,
                Err(err) => panic!("{}", err),
            }
        }
        let block = DataBlock::concat(&blocks).unwrap();

        assert_eq!(block.num_columns(), group_columns.len() + aggrs.len());
        assert_eq!(block.num_rows(), m);

        let validities = vec![true, true, true, true];

        let rows = n as i64;
        let urows = rows as u64;

        let mut expected_results: Vec<Column> =
            group_columns.iter().map(|c| c.slice(0..m)).collect();

        expected_results.extend_from_slice(&[
            Int64Type::from_data_with_validity(vec![0, 1, 2, 3], validities.clone()),
            Int64Type::from_data_with_validity(vec![0, 1, 2, 3], validities.clone()),
            Int64Type::from_data_with_validity(
                vec![0, rows / 2, rows, rows / 2 * 3],
                validities.clone(),
            ),
            UInt64Type::from_data(vec![urows / 2, urows / 2, urows / 2, urows / 2]),
        ]);

        let block_expected = DataBlock::new_from_columns(expected_results.clone());
        assert_block_value_sort_eq(&block, &block_expected);
    }
}

#[test]
fn test_layout() {
    let factory = AggregateFunctionFactory::instance();
    let decimal_type = DataType::Decimal(DecimalSize::new_unchecked(20, 2));

    let aggrs = factory
        .get("sum", vec![], vec![decimal_type], vec![])
        .unwrap();
    type S = DecimalSumState<false, i128>;
    type M = DecimalSumState<false, i256>;

    let states_layout = get_states_layout(&[aggrs.clone()]).unwrap();

    assert_eq!(
        states_layout.layout,
        Layout::from_size_align(17, 8).unwrap()
    );
    assert_eq!(Layout::new::<S>(), Layout::from_size_align(16, 8).unwrap());
    assert_eq!(Layout::new::<M>(), Layout::from_size_align(32, 8).unwrap());
    assert_eq!(
        Layout::new::<i128>(),
        Layout::from_size_align(16, 16).unwrap()
    );

    assert_eq!(
        Layout::new::<i128>(),
        Layout::from_size_align(16, 16).unwrap()
    );
}

#[test]
fn test_partitioned_payload_with_individual_arenas() {
    let group_types = vec![Int64Type::data_type()];
    let aggrs = vec![];
    let partition_count = 4usize;

    let payload = PartitionedPayload::new_with_partition_bumps(
        group_types.clone(),
        aggrs.clone(),
        partition_count as u64,
    );

    assert_eq!(payload.partition_count(), partition_count);
    assert_eq!(payload.arenas.len(), partition_count);

    for i in 0..partition_count {
        for j in (i + 1)..partition_count {
            assert!(!Arc::ptr_eq(&payload.arenas[i], &payload.arenas[j]));
        }
    }

    let mut state = PayloadFlushState::default();
    let repartitioned = payload.repartition(partition_count * 2, &mut state);

    assert_eq!(repartitioned.partition_count(), partition_count * 2);
    assert_eq!(repartitioned.arenas.len(), partition_count * 2);
    for i in 0..repartitioned.partition_count() {
        for j in (i + 1)..repartitioned.partition_count() {
            assert!(!Arc::ptr_eq(
                &repartitioned.arenas[i],
                &repartitioned.arenas[j]
            ));
        }
    }
}

#[test]
fn test_take_payloads_and_rebuild() {
    let factory = AggregateFunctionFactory::instance();
    let group_types = vec![Int64Type::data_type()];
    let aggrs = vec![factory
        .get("count", vec![], vec![Int64Type::data_type()], vec![])
        .unwrap()];
    let config = HashTableConfig::default().with_initial_radix_bits(1);

    let mut hashtable =
        AggregateHashTable::new_with_separated_arenas(group_types.clone(), aggrs.clone(), config);

    let rows_first = 64usize;
    let columns = vec![Int64Type::from_data((0..rows_first as i64).collect_vec())];
    let group_block_entries: Vec<BlockEntry> = columns.iter().cloned().map(|c| c.into()).collect();
    let params = vec![vec![columns[0].clone().into()]];
    let params = params.iter().map(|v| v.into()).collect_vec();

    let mut probe_state = ProbeState::default();
    let _ = hashtable
        .add_groups(
            &mut probe_state,
            (&group_block_entries).into(),
            &params,
            (&[]).into(),
            rows_first,
        )
        .unwrap();

    let initial_len = hashtable.len();
    let taken = hashtable.take_payloads(&[0]).unwrap();
    assert_eq!(taken.len(), 1);
    let taken_rows = taken[0].len();
    assert_eq!(hashtable.len(), initial_len - taken_rows);

    // new rows should still be accepted and counted
    let rows_second = 32usize;
    let columns_second = vec![Int64Type::from_data(
        (rows_first as i64..rows_first as i64 + rows_second as i64).collect_vec(),
    )];
    let group_entries_second: Vec<BlockEntry> =
        columns_second.iter().cloned().map(|c| c.into()).collect();
    let params_second = vec![vec![columns_second[0].clone().into()]];
    let params_second = params_second.iter().map(|v| v.into()).collect_vec();

    let mut probe_state_second = ProbeState::default();
    let _ = hashtable
        .add_groups(
            &mut probe_state_second,
            (&group_entries_second).into(),
            &params_second,
            (&[]).into(),
            rows_second,
        )
        .unwrap();

    assert_eq!(hashtable.len(), initial_len - taken_rows + rows_second);

    // The arena for the taken bucket should be replaced with a new one.
    let replaced_arena = &hashtable.payload.arenas[0];
    assert!(!Arc::ptr_eq(replaced_arena, &taken[0].arena));
}

#[test]
fn test_repartition_keeps_states_with_separated_arenas() {
    let factory = AggregateFunctionFactory::instance();
    let group_types = vec![Int64Type::data_type()];
    let aggrs = vec![factory
        .get("count", vec![], vec![Int64Type::data_type()], vec![])
        .unwrap()];
    let config = HashTableConfig::default().with_initial_radix_bits(2);

    let mut hashtable = AggregateHashTable::new_with_separated_arenas(group_types, aggrs, config);

    let rows = 4usize;
    let column = Int64Type::from_data(vec![0, 0, 1, 1]);
    let group_entries: Vec<BlockEntry> = vec![column.clone().into()];
    let params = vec![vec![column.into()]];
    let params = params.iter().map(|v| v.into()).collect_vec();

    let mut probe_state = ProbeState::default();
    hashtable
        .add_groups(
            &mut probe_state,
            (&group_entries).into(),
            &params,
            (&[]).into(),
            rows,
        )
        .unwrap();
    println!("{}", hashtable.payload.partition_count());
    // Repartition to mimic spill-out path; states must remain valid.
    hashtable.repartition(4);

    let mut blocks = Vec::new();
    let mut flush_state = PayloadFlushState::default();
    while hashtable.merge_result(&mut flush_state).unwrap() {
        let mut entries = flush_state.take_aggregate_results();
        entries.extend(flush_state.take_group_columns());
        let num_rows = entries[0].len();
        blocks.push(DataBlock::new(entries, num_rows));
    }

    let block = DataBlock::concat(&blocks).unwrap();
    let count_col =
        UInt64Type::try_downcast_column(&block.columns()[0].to_column()).expect("count column");
    let total: u64 = UInt64Type::iter_column(&count_col).sum();

    assert_eq!(total as usize, rows);
}
