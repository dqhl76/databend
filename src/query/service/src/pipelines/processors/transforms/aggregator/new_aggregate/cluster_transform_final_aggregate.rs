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

use std::any::Any;
use std::mem;
use std::sync::Arc;

use bumpalo::Bump;
use concurrent_queue::ConcurrentQueue;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use itertools::Itertools;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::NewAggregateSpiller;

#[allow(clippy::enum_variant_names)]
enum HashTable {
    MovedOut,
    AggregateHashTable(AggregateHashTable),
}

impl Default for HashTable {
    fn default() -> Self {
        Self::MovedOut
    }
}

/// ClusterFinalAggregateTransform also servers for final stage's restoring and aggregation
/// the difference is that ClusterFinalAggregateTransform only accepts hash-partitioned,
/// which means the partitions are disjoin.
pub struct ClusterFinalAggregateTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<AggregateMeta>,
    id: usize,
    local_finished: bool,

    hashtable: HashTable,
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,

    global_working_queue: Arc<ConcurrentQueue<AggregateMeta>>,

    spiller: NewAggregateSpiller,

    max_aggregate_spill_level: usize,
}

impl Processor for ClusterFinalAggregateTransform {
    fn name(&self) -> String {
        "ClusterFinalAggregateTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;
            if let Some(block_meta) = data_block
                .take_meta()
                .and_then(AggregateMeta::downcast_from)
            {
                self.input_data = Some(block_meta);
                return Ok(Event::Sync);
            }
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let input_data = self.input_data.take();
        if let Some(meta) = input_data {
            match meta {
                AggregateMeta::Serialized(meta) => {
                    self.final_aggregate(AggregateMeta::Serialized(meta))?;
                }
                AggregateMeta::NewBucketSpilled(p) => {
                    let meta = self.spiller.restore(p)?;
                    self.final_aggregate(meta)?;
                }
                _ => {
                    unreachable!("[FINAL-AGG] Unexpected aggregate meta when processing");
                }
            }
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        todo!()
    }
}

impl ClusterFinalAggregateTransform {
    /// Finish local work, start work stealing mode
    pub fn local_finish(&mut self) -> Result<Option<DataBlock>> {
        if let HashTable::AggregateHashTable(mut ht) = mem::take(&mut self.hashtable) {
            let output_block = {
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
                    self.params.empty_result_block()
                } else {
                    DataBlock::concat(&blocks)?
                }
            };

            return Ok(Some(output_block));
        }
        Ok(None)
    }

    pub fn final_aggregate(&mut self, meta: AggregateMeta) -> Result<()> {
        let AggregateMeta::Serialized(payload) = meta else {
            unreachable!("[FINAL-AGG] Unexpected aggregate meta when final aggregating")
        };

        match self.hashtable.borrow_mut() {
            HashTable::MovedOut => {
                self.hashtable =
                    HashTable::AggregateHashTable(payload.convert_to_aggregate_table(
                        self.params.group_data_types.clone(),
                        self.params.aggregate_functions.clone(),
                        self.params.num_states(),
                        0,
                        Arc::new(Bump::new()),
                        true,
                    )?);
            }
            HashTable::AggregateHashTable(ht) => {
                let payload = payload.convert_to_partitioned_payload(
                    self.params.group_data_types.clone(),
                    self.params.aggregate_functions.clone(),
                    self.params.num_states(),
                    0,
                    Arc::new(Bump::new()),
                )?;
                ht.combine_payloads(&payload, &mut self.flush_state)?;
            }
        }

        if self.spiller.memory_settings.check_spill() {
            self.spill_out()?;
        }
        Ok(())
    }

    fn spill_out(&mut self) -> Result<()> {
        let spill_chunk_count = 2_usize.pow(self.max_aggregate_spill_level as u32);
        if let HashTable::AggregateHashTable(mut ht) = mem::take(&mut self.hashtable) {
            let payload_count = ht.payload.payloads.len();
            if payload_count < spill_chunk_count {
                ht.repartition(spill_chunk_count)
            }
            let payload_count_per_chunk = ht.payload.payloads.len() / spill_chunk_count;

            let taken_payload_indices =
                (payload_count_per_chunk..ht.payload.payloads.len()).collect();
            let spilled_payload = ht.take_payloads(taken_payload_indices)?;

            for (chunk_id, chunk) in spilled_payload
                .into_iter()
                .chunks(payload_count_per_chunk)
                .into_iter()
                .enumerate()
            {
                for payload in chunk {
                    let datablock = payload.aggregate_flush_all()?.consume_convert_to_full();
                    self.spiller.spill(chunk_id, datablock)?;
                }
            }
        }

        Ok(())
    }
}
