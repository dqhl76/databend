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
use std::sync::Arc;

use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;

/// ClusterFinalAggregateTransform also servers for final stage's restoring and aggregation
/// the difference is that ClusterFinalAggregateTransform only accepts hash-partitioned,
/// which means the partitions are orthogonal.
pub struct ClusterFinalAggregateTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: AggregateMeta,
    id: usize,
}

impl Processor for ClusterFinalAggregateTransform {
    fn name(&self) -> String {
        "ClusterFinalAggregateTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> databend_common_exception::Result<Event> {
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
                self.input_data = block_meta;
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

    fn process(&mut self) -> databend_common_exception::Result<()> {
        todo!()
    }

    async fn async_process(&mut self) -> databend_common_exception::Result<()> {
        todo!()
    }
}
