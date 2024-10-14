use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockMetaAccumulatingTransform;
use databend_common_pipeline_transforms::processors::BlockMetaAccumulatingTransformer;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;

use crate::pruning_pipeline::meta_info::CompactSegmentMeta;
use crate::pruning_pipeline::meta_info::ExtractSegmentResult;

/// ExtractSegmentTransform Workflow:
/// 1. Extract the pruned segment to blocks
pub struct ExtractSegmentTransform {}

impl ExtractSegmentTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> databend_common_exception::Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(
            BlockMetaAccumulatingTransformer::create(input, output, ExtractSegmentTransform {}),
        ))
    }
}

impl BlockMetaAccumulatingTransform<CompactSegmentMeta> for ExtractSegmentTransform {
    const NAME: &'static str = "ExtractSegmentTransform";

    fn transform(
        &mut self,
        data: CompactSegmentMeta,
    ) -> databend_common_exception::Result<Option<DataBlock>> {
        let populate_cache = true;
        let block_metas = if let Some(cache) = CacheManager::instance().get_block_meta_cache() {
            if let Some(metas) = cache.get(data.location.location.0.clone()) {
                Ok::<_, ErrorCode>(metas)
            } else {
                match populate_cache {
                    true => Ok(cache.insert(
                        data.location.location.0.to_string(),
                        data.compact_segment.block_metas()?,
                    )),
                    false => Ok(Arc::new(data.compact_segment.block_metas()?)),
                }
            }
        } else {
            Ok(Arc::new(data.compact_segment.block_metas()?))
        }?;
        Ok(Some(DataBlock::empty_with_meta(
            ExtractSegmentResult::create(block_metas, data.location.clone()),
        )))
    }
}
