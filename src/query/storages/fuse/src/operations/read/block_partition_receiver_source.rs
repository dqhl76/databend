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

use async_channel::Receiver;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;

use crate::operations::read::block_partition_meta::BlockPartitionMeta;

pub struct BlockPartitionReceiverSource {
    pub meta_receiver: Receiver<databend_common_exception::Result<PartInfoPtr>>,
}

impl BlockPartitionReceiverSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        receiver: Receiver<databend_common_exception::Result<PartInfoPtr>>,
        output_port: Arc<OutputPort>,
    ) -> databend_common_exception::Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, output_port, Self {
            meta_receiver: receiver,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for BlockPartitionReceiverSource {
    const NAME: &'static str = "BlockPartitionReceiverSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> databend_common_exception::Result<Option<DataBlock>> {
        match self.meta_receiver.recv().await {
            Ok(Ok(part)) => Ok(Some(DataBlock::empty_with_meta(
                BlockPartitionMeta::create(vec![part]),
            ))),
            Ok(Err(e)) => Err(
                // The error is occurred in pruning process
                e,
            ),
            Err(_) => {
                // The channel is closed, we should return None to stop generating
                Ok(None)
            }
        }
    }
}
