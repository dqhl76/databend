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
use std::cmp::min;
use std::sync::Arc;

use async_channel::Receiver;
use async_channel::Sender;
use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PayloadFlushState;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use itertools::Itertools;
use log::error;
use log::info;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::NewAggregateSpiller;
use crate::pipelines::processors::transforms::aggregator::SharedPartitionStream;
use crate::sessions::QueryContext;

pub struct ExperimentalFinalAggregator {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    id: usize,
    input_data: Option<AggregateMeta>,
    hashtable: Option<AggregateHashTable>,
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,
    spiller: NewAggregateSpiller,
    spill_happened: bool,

    should_finish: bool,
    tx: Option<Sender<AggregateMeta>>,
    rx: Receiver<AggregateMeta>,
}

#[async_trait::async_trait]
impl Processor for ExperimentalFinalAggregator {
    fn name(&self) -> String {
        "ExperimentalFinalAggregateFinal".to_string()
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

        if self.input_data.is_some() {
            return Ok(Event::Sync);
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
            if !self.should_finish {
                return Ok(Event::Async);
            }
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
                AggregateMeta::Partitioned { data, bucket, .. } => {
                    for meta in data {
                        match &meta {
                            AggregateMeta::Serialized(_) | AggregateMeta::AggregatePayload(_) => {
                                self.aggregate(meta, true)?;
                            }
                            AggregateMeta::NewBucketSpilled(_) => {
                                let meta = self.restore(meta)?;
                                self.aggregate(meta, true)?;
                            }
                            _ => self.unexpected_meta(&meta)?,
                        }
                    }

                    info!("[FINAL-AGG-{}] Final aggregate: {}", self.id, bucket);
                }
                AggregateMeta::NewSpilled(spilled_metas) => {
                    info!(
                        "[FINAL-AGG-{}] Restore aggregate spilled data: {}",
                        self.id,
                        spilled_metas.len()
                    );
                    for spilled_meta in spilled_metas {
                        let meta = self.restore(AggregateMeta::NewBucketSpilled(spilled_meta))?;
                        self.aggregate(meta, false)?;
                    }
                    info!(
                        "[FINAL-AGG-{}] Final aggregate after restore spilled data.",
                        self.id,
                    );
                }
                _ => self.unexpected_meta(&meta)?,
            }

            self.final_aggregate()?;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let _ = self.tx.take();

        match self.rx.recv().await {
            Ok(meta) => {
                info!(
                    "[FINAL-AGG-{}] Received spilled aggregate meta from spiller.",
                    self.id
                );
                self.input_data = Some(meta);
            }
            Err(_e) => {
                self.should_finish = true;
            }
        }

        Ok(())
    }
}

impl ExperimentalFinalAggregator {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        id: usize,
        params: Arc<AggregatorParams>,
        ctx: Arc<QueryContext>,
        tx: Sender<AggregateMeta>,
        rx: Receiver<AggregateMeta>,
    ) -> Result<Box<dyn Processor>> {
        let partition_count = min(128, 2_usize.pow(params.max_aggregate_spill_level as u32));
        let stream = SharedPartitionStream::new(
            1, // todo: need to support stream without sharing
            params.max_block_rows,
            params.max_block_bytes,
            partition_count,
        );
        let spiller = NewAggregateSpiller::try_create(ctx.clone(), partition_count, stream, true)?;
        Ok(Box::new(ExperimentalFinalAggregator {
            input,
            output,
            id,
            input_data: None,
            hashtable: None,
            params,
            flush_state: PayloadFlushState::default(),
            spiller,
            spill_happened: false,
            should_finish: false,
            tx: Some(tx),
            rx,
        }))
    }

    pub fn restore(&self, meta: AggregateMeta) -> Result<AggregateMeta> {
        let AggregateMeta::NewBucketSpilled(payload) = meta else {
            return self.unexpected_meta(&meta);
        };

        self.spiller.restore(payload)
    }

    pub fn aggregate(&mut self, meta: AggregateMeta, check_memory: bool) -> Result<()> {
        match meta {
            AggregateMeta::Serialized(payload) => {
                if let Some(hashtable) = self.hashtable.as_mut() {
                    let payload = payload.convert_to_partitioned_payload(
                        self.params.group_data_types.clone(),
                        self.params.aggregate_functions.clone(),
                        self.params.num_states(),
                        0,
                        Arc::new(Bump::new()),
                    )?;
                    hashtable.combine_payloads(&payload, &mut self.flush_state)?;
                } else {
                    self.hashtable =
                        Some(payload.convert_to_aggregate_table_with_separated_arenas(
                            self.params.group_data_types.clone(),
                            self.params.aggregate_functions.clone(),
                            self.params.num_states(),
                            0,
                            true,
                        )?);
                }
            }
            AggregateMeta::AggregatePayload(payload) => {
                let capacity = AggregateHashTable::get_capacity_for_count(payload.payload.len());
                let hashtable = self.hashtable.get_or_insert_with(|| {
                    AggregateHashTable::new_with_capacity_and_separated_arenas(
                        self.params.group_data_types.clone(),
                        self.params.aggregate_functions.clone(),
                        HashTableConfig::default().with_initial_radix_bits(0),
                        capacity,
                    )
                });

                hashtable.combine_payload(&payload.payload, &mut self.flush_state)?;
            }
            _ => self.unexpected_meta(&meta)?,
        }

        // If spill happened once, we always need to spill when aggregating new data
        // this is to prevent that part of payloads are spilled out but the others are kept in memory
        if (self.spill_happened || self.spiller.memory_settings.check_spill()) && check_memory {
            self.spill_happened = true;
            self.spill_out()?;
        }

        Ok(())
    }

    pub fn final_aggregate(&mut self) -> Result<()> {
        let hashtable = self.hashtable.take();

        if self.spill_happened {
            // reset spill_happened flag
            self.spill_happened = false;

            self.spill_finish()?;
            #[cfg(debug_assertions)]
            if let Some(ht) = hashtable.as_ref() {
                let (_, taken_payload_indices) = calculate_spill_payloads(
                    ht.payload.payloads.len(),
                    self.spiller.partition_count,
                )?;
                for idx in taken_payload_indices {
                    debug_assert_eq!(ht.payload.payloads[idx].len(), 0);
                }
            }
        }

        let output_block = if let Some(mut ht) = hashtable {
            let mut blocks = vec![];
            self.flush_state.clear();

            while ht.merge_result(&mut self.flush_state)? {
                let mut entries = self.flush_state.take_aggregate_results();
                let group_columns = self.flush_state.take_group_columns();
                entries.extend_from_slice(&group_columns);
                let num_rows = entries[0].len();
                blocks.push(DataBlock::new(entries, num_rows));
            }

            if blocks.is_empty() {
                self.params.empty_result_block()
            } else {
                DataBlock::concat(&blocks)?
            }
        } else {
            self.params.empty_result_block()
        };

        info!(
            "[FINAL-AGG-{}] Final aggregate output rows: {:?}",
            self.id, output_block
        );

        if !output_block.is_empty() {
            self.output.push_data(Ok(output_block));
        }

        Ok(())
    }

    pub fn spill_out(&mut self) -> Result<()> {
        let spill_chunk_count = self.spiller.partition_count;
        if let Some(hashtable) = self.hashtable.as_mut() {
            if hashtable.payload.payloads.len() < spill_chunk_count {
                hashtable.repartition(spill_chunk_count);
            }

            let partition_count = hashtable.payload.payloads.len();
            let (payload_count_per_chunk, taken_payload_indices) =
                calculate_spill_payloads(partition_count, spill_chunk_count)?;

            let spilled_payloads = hashtable.take_payloads(&taken_payload_indices)?;

            for (chunk_id, chunk) in spilled_payloads
                .into_iter()
                .chunks(payload_count_per_chunk)
                .into_iter()
                .enumerate()
            {
                for payload in chunk {
                    if payload.len() == 0 {
                        continue;
                    }
                    let datablock = payload.aggregate_flush_all()?.consume_convert_to_full();
                    info!("[FINAL-AGG-{}] spill out: {:?}", self.id, datablock);
                    self.spiller.spill(chunk_id, datablock)?;
                }
            }
        }

        Ok(())
    }

    pub fn spill_finish(&mut self) -> Result<()> {
        let payloads = self.spiller.spill_finish()?;
        let meta = AggregateMeta::NewSpilled(payloads);
        if self
            .tx
            .as_ref()
            .expect("Logic error: take before sending")
            .send_blocking(meta)
            .is_err()
        {
            error!(
                "[FINAL-AGG-{}] Failed to send spilled aggregate meta to async processor.",
                self.id
            );
        }
        Ok(())
    }

    fn unexpected_meta<T>(&self, meta: &AggregateMeta) -> Result<T> {
        Err(ErrorCode::Internal(format!(
            "[FINAL-AGG-{}] Unexpected AggregateMeta variant {:?}",
            self.id, meta
        )))
    }
}

fn calculate_spill_payloads(
    partition_count: usize,
    spill_chunk_count: usize,
) -> Result<(usize, Vec<usize>)> {
    let payload_count_per_chunk = partition_count / spill_chunk_count;
    let taken_payload_indices = (payload_count_per_chunk..partition_count).collect::<Vec<_>>();
    Ok((payload_count_per_chunk, taken_payload_indices))
}
