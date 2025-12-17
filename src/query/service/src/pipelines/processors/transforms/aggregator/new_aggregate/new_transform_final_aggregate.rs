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

use std::mem;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_common_pipeline_transforms::AccumulatingTransformer;

use crate::pipelines::processors::transforms::aggregator::transform_aggregate_partial::HashTable;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

pub struct NewTransformFinalAggregate {
    hashtable: HashTable,
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,
}

impl NewTransformFinalAggregate {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        let hashtable = AggregateHashTable::new(
            params.group_data_types.clone(),
            params.aggregate_functions.clone(),
            HashTableConfig::default().with_initial_radix_bits(0),
            Arc::new(Bump::new()),
        );
        let flush_state = PayloadFlushState::default();

        Ok(AccumulatingTransformer::create(
            input,
            output,
            NewTransformFinalAggregate {
                hashtable: HashTable::AggregateHashTable(hashtable),
                params,
                flush_state,
            },
        ))
    }

    pub fn finish(&mut self) -> Result<DataBlock> {
        if let HashTable::AggregateHashTable(mut ht) = mem::take(&mut self.hashtable) {
            let mut blocks = vec![];
            self.flush_state.clear();

            loop {
                if ht.merge_result(&mut self.flush_state)? {
                    let mut entries = self.flush_state.take_aggregate_results();
                    let group_columns = self.flush_state.take_group_columns();
                    entries.extend_from_slice(&group_columns);
                    let num_rows = entries[0].len();
                    blocks.push(DataBlock::new(entries, num_rows));
                } else {
                    break;
                }
            }

            if blocks.is_empty() {
                return Ok(self.params.empty_result_block());
            }
            return DataBlock::concat(&blocks);
        }

        Ok(self.params.empty_result_block())
    }
}

impl AccumulatingTransform for NewTransformFinalAggregate {
    const NAME: &'static str = "NewTransformFinalAggregate";

    fn transform(&mut self, mut data: DataBlock) -> Result<Vec<DataBlock>> {
        if let Some(meta) = data.take_meta() {
            if let Some(meta) = AggregateMeta::downcast_from(meta) {
                if let AggregateMeta::AggregatePayload(payload) = meta {
                    if let HashTable::AggregateHashTable(ht) = &mut self.hashtable {
                        ht.combine_payload(&payload.payload, &mut self.flush_state)?;
                    }
                }
            }
        }
        Ok(vec![])
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        Ok(vec![self.finish()?])
    }
}
