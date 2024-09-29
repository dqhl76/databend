use std::sync::Arc;

use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::processors::BlockMetaAccumulatingTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_pruner::RangePruner;

use crate::pruning_pipeline::compact_segment_meta::CompactSegmentMeta;

/// ExtractSegmentTransform Workflow:
/// 1. Extract the pruned segment to blocks
pub struct ExtractSegmentTransform {
    range_pruner: Arc<dyn RangePruner + Send + Sync>,
}

impl BlockMetaAccumulatingTransform<CompactSegmentMeta> for ExtractSegmentTransform {
    const NAME: &'static str = "ExtractSegmentTransform";

    fn transform(
        &mut self,
        data: CompactSegmentMeta,
    ) -> databend_common_exception::Result<Option<DataBlock>> {
        let populate_cache = true;
        let block_metas = if let Some(cache) = CacheManager::instance().get_block_meta_cache() {
            if let Some(metas) = cache.get(data.location.location.0) {
                Ok(metas)
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
    }
}
