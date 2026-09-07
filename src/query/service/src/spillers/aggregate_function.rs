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

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::ProgressValues;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateFunctionSpill;
use databend_common_expression::AggregateSpillFile;
use databend_common_expression::AggregateSpillReader;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::types::DataType;
use databend_common_pipeline::core::check_interrupt;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ReadSettings;
use databend_common_storages_parquet::deserialize_row_group_meta_from_bytes;
use databend_common_storages_parquet::serialize_row_group_meta_to_bytes;

use super::Layout;
use super::Location;
use super::SpillAdapter;
use super::SpillTarget;
use super::SpillsBufferPool;
use super::aggregate_function_policy::FunctionSpillPolicy;
use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextSpillProgress;

pub struct AggregateFunctionSpiller {
    ctx: Arc<QueryContext>,
    memory: MemorySettings,
    policy: FunctionSpillPolicy,
    restore_memory_limit: usize,
    writer_pool_bytes: usize,
    read_settings: ReadSettings,
    target: SpillTarget,
}

impl AggregateFunctionSpiller {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Arc<dyn AggregateFunctionSpill>> {
        let settings = ctx.get_settings();
        let operator = DataOperator::instance();
        let params = operator
            .spill_params()
            .cloned()
            .unwrap_or_else(|| operator.params());
        Ok(Arc::new(Self {
            memory: MemorySettings::from_aggregate_settings(&ctx)?,
            policy: FunctionSpillPolicy::new(
                settings.get_force_aggregate_data_spill()?,
                settings.get_aggregate_function_spilling_memory_threshold()?,
            ),
            restore_memory_limit: settings.get_aggregate_function_restore_memory_threshold()?,
            writer_pool_bytes: settings.get_spill_writer_memory_pool_size_mb()? * 1024 * 1024,
            read_settings: ReadSettings::from_settings(&settings)?,
            target: SpillTarget::from_storage_params(Some(&params)),
            ctx,
        }))
    }
}

impl AggregateFunctionSpill for AggregateFunctionSpiller {
    fn restore_memory_limit(&self) -> usize {
        self.restore_memory_limit
    }

    fn should_spill(&self, memory_bytes: usize) -> bool {
        self.policy
            .should_spill(memory_bytes, self.memory.check_spill())
    }

    fn check_interrupt(&self) -> Result<()> {
        check_interrupt()
    }

    fn spill(&self, block: DataBlock) -> Result<AggregateSpillFile> {
        check_interrupt()?;
        let data_operator = DataOperator::instance();
        // Function manifests travel as ordinary intermediate state columns.
        // A node-local file cannot be restored by the receiving worker.
        if self.target.is_local() && self.ctx.get_cluster().nodes.len() > 1 {
            return Err(ErrorCode::Unimplemented(
                "Aggregate-function spilling in a distributed query requires shared remote spill storage",
            ));
        }
        let location = format!(
            "{}/function-{}",
            self.ctx.query_id_spill_prefix(),
            GlobalUniq::unique()
        );
        // Register before opening the writer, including failed/cancelled writes
        // in the query's existing spill cleanup lifecycle.
        self.ctx
            .add_spill_file(Location::Remote(location.clone()), Layout::Aggregate, 0);
        let mut writer = SpillsBufferPool::instance().writer(
            data_operator.spill_operator(),
            location.clone(),
            self.writer_pool_bytes,
            self.target,
        )?;
        let rows = block.num_rows();
        writer.write(block)?;
        let (bytes, row_groups) = writer.close()?;
        self.ctx.incr_spill_progress(0, bytes);
        self.ctx
            .get_aggregate_spill_progress()
            .incr(&ProgressValues { rows, bytes });
        let metadata = row_groups
            .iter()
            .map(serialize_row_group_meta_to_bytes)
            .collect::<Result<Vec<_>>>()?;
        Ok(AggregateSpillFile {
            location,
            metadata: serde_json::to_vec(&metadata)?,
        })
    }

    fn restore(
        &self,
        file: &AggregateSpillFile,
        data_types: &[DataType],
    ) -> Result<AggregateSpillReader> {
        check_interrupt()?;
        let metadata: Vec<Vec<u8>> = serde_json::from_slice(&file.metadata)?;
        let row_groups = metadata
            .iter()
            .map(|bytes| deserialize_row_group_meta_from_bytes(bytes))
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(DataSchema::new(
            data_types
                .iter()
                .enumerate()
                .map(|(i, ty)| DataField::new(&format!("col_{i}"), ty.clone()))
                .collect(),
        ));
        let data_operator = DataOperator::instance();
        let mut reader = SpillsBufferPool::instance().reader(
            data_operator.spill_operator(),
            file.location.clone(),
            schema,
            row_groups,
            self.target,
            self.read_settings,
        )?;
        let mut finished = false;
        Ok(Box::new(std::iter::from_fn(move || {
            if finished {
                return None;
            }
            match check_interrupt().and_then(|_| reader.read()) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => {
                    finished = true;
                    None
                }
                Err(error) => {
                    finished = true;
                    Some(Err(error))
                }
            }
        })))
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::StringType;
    use futures::TryStreamExt;

    use super::*;
    use crate::interpreters::InterpreterFactory;
    use crate::sql::Planner;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn global_aggregates_restore_function_spills_through_the_pipeline() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();
        settings.set_setting("force_aggregate_data_spill".into(), "1".into())?;
        settings.set_setting("max_threads".into(), "2".into())?;
        let mut planner = Planner::new(ctx.clone());
        let (plan, _) = planner
            .plan_sql(
                "SELECT count(DISTINCT number), count(DISTINCT number::string), median(number), \
             string_agg(number::string, '|' ORDER BY number) FROM numbers_mt(64)",
            )
            .await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let blocks = interpreter
            .execute(ctx.clone())
            .await?
            .try_collect::<Vec<DataBlock>>()
            .await?;
        let result = DataBlock::concat(&blocks)?;
        assert_eq!(result.num_rows(), 1);
        let values = result
            .columns()
            .iter()
            .map(|column| column.index(0).unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(values[..3], ["64", "64", "31.5"]);
        assert_eq!(
            result.get_by_offset(3).index(0).unwrap().to_owned(),
            databend_common_expression::Scalar::String(
                (0..64).map(|n| n.to_string()).collect::<Vec<_>>().join("|")
            )
        );
        assert!(ctx.get_aggregate_spill_progress_value().rows > 0);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn function_spill_requires_pressure_and_has_a_separate_restore_budget() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();
        for (name, value) in [
            ("force_aggregate_data_spill", "0"),
            ("aggregate_spilling_memory_ratio", "0"),
            ("max_query_memory_usage", "0"),
            ("aggregate_function_spilling_memory_threshold", "1"),
            ("aggregate_function_restore_memory_threshold", "4096"),
        ] {
            settings.set_setting(name.into(), value.into())?;
        }
        let spill = AggregateFunctionSpiller::try_create(ctx.clone())?;
        for _ in 0..4 {
            assert!(!spill.should_spill(1024 * 1024 * 1024));
        }
        assert_eq!(spill.restore_memory_limit(), 4096);

        let mut planner = Planner::new(ctx.clone());
        let (plan, _) = planner
            .plan_sql(
                "SELECT count(DISTINCT number), median(number), array_length(array_agg(number)), \
             length(string_agg(number::string, '|')) FROM numbers_mt(64)",
            )
            .await?;
        let blocks = InterpreterFactory::get(ctx.clone(), &plan)
            .await?
            .execute(ctx.clone())
            .await?
            .try_collect::<Vec<DataBlock>>()
            .await?;
        let result = DataBlock::concat(&blocks)?;
        let values = result
            .columns()
            .iter()
            .map(|column| column.index(0).unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(values, ["64", "31.5", "64", "181"]);
        assert_eq!(ctx.get_aggregate_spill_progress_value().rows, 0);

        settings.set_setting("force_aggregate_data_spill".into(), "1".into())?;
        let forced = AggregateFunctionSpiller::try_create(ctx.clone())?;
        assert!(forced.should_spill(1));
        assert_eq!(forced.restore_memory_limit(), 4096);

        // A tiny shared aggregate budget deterministically creates pressure
        // without relying on the force switch. State size must still meet the threshold.
        settings.set_setting("force_aggregate_data_spill".into(), "0".into())?;
        settings.set_setting("max_memory_usage".into(), "1".into())?;
        settings.set_setting("aggregate_spilling_memory_ratio".into(), "1".into())?;
        settings.set_setting(
            "aggregate_function_spilling_memory_threshold".into(),
            "67108864".into(),
        )?;
        let pressured = AggregateFunctionSpiller::try_create(ctx)?;
        for _ in 0..4 {
            assert!(!pressured.should_spill(4096));
            assert!(!pressured.should_spill(67108863));
            assert!(pressured.should_spill(67108864));
        }
        assert_eq!(pressured.restore_memory_limit(), 4096);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn function_spill_uses_buffered_io_and_query_file_tracking() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let spill = AggregateFunctionSpiller::try_create(ctx.clone())?;
        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 2, 3]),
            StringType::from_data_with_validity(vec!["a", "", "c"], vec![true, false, true]),
        ]);
        let types = block
            .columns()
            .iter()
            .map(|entry| entry.data_type())
            .collect::<Vec<_>>();
        let file = spill.spill(block.clone())?;
        assert!(
            ctx.get_spilled_files()
                .contains(&Location::Remote(file.location.clone()))
        );
        // A second function instance can restore a transported descriptor.
        let bytes = serde_json::to_vec(&file)?;
        let receiver = AggregateFunctionSpiller::try_create(ctx)?;
        let received = serde_json::from_slice(&bytes)?;
        let blocks = receiver
            .restore(&received, &types)?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(DataBlock::concat(&blocks)?.columns(), block.columns());

        DataOperator::instance()
            .spill_operator()
            .delete(&file.location)
            .await?;
        let restored = receiver
            .restore(&received, &types)?
            .collect::<Result<Vec<_>>>();
        assert!(restored.is_err());
        Ok(())
    }
}
