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

use databend_common_ast::ast::ExplainKind;
use databend_common_ast::ast::FormatTreeNode;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_pipeline_core::processors::PlanProfile;
use databend_common_sql::binder::ExplainConfig;
use databend_common_sql::optimizer::ColumnSet;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::MergeInto;
use databend_common_sql::plans::UpdatePlan;
use databend_common_sql::BindContext;
use databend_common_sql::MetadataRef;
use databend_common_storages_result_cache::gen_result_cache_key;
use databend_common_storages_result_cache::ResultCacheReader;
use databend_common_users::UserApiProvider;

use super::InsertMultiTableInterpreter;
use super::InterpreterFactory;
use super::UpdateInterpreter;
use crate::interpreters::interpreter_merge_into::MergeIntoInterpreter;
use crate::interpreters::Interpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline;
use crate::schedulers::Fragmenter;
use crate::schedulers::QueryFragmentsActions;
use crate::sessions::QueryContext;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PhysicalPlanBuilder;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::DeletePlan;
use crate::sql::plans::EvalScalar;
use crate::sql::plans::Filter;
use crate::sql::plans::Plan;
use crate::sql::plans::RelOperator;
use crate::sql::plans::ScalarItem;
use crate::sql::plans::Scan;
use crate::sql::ColumnBindingBuilder;
use crate::sql::ScalarExpr;
use crate::sql::Visibility;

pub struct ExplainInterpreter {
    ctx: Arc<QueryContext>,
    config: ExplainConfig,
    kind: ExplainKind,
    plan: Plan,
}

#[async_trait::async_trait]
impl Interpreter for ExplainInterpreter {
    fn name(&self) -> &str {
        "ExplainInterpreterV2"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let blocks = match &self.kind {
            ExplainKind::Raw | ExplainKind::Optimized => self.explain_plan(&self.plan)?,
            ExplainKind::Plan if self.config.logical => self.explain_plan(&self.plan)?,
            ExplainKind::Plan => match &self.plan {
                Plan::Query {
                    s_expr,
                    metadata,
                    bind_context,
                    formatted_ast,
                    ..
                } => {
                    self.explain_query(s_expr, metadata, bind_context, formatted_ast)
                        .await?
                }
                Plan::Insert(insert_plan) => insert_plan.explain(self.config.verbose).await?,
                Plan::Replace(replace_plan) => replace_plan.explain(self.config.verbose).await?,
                Plan::CreateTable(plan) => match &plan.as_select {
                    Some(box Plan::Query {
                        s_expr,
                        metadata,
                        bind_context,
                        formatted_ast,
                        ..
                    }) => {
                        let mut res =
                            vec![DataBlock::new_from_columns(vec![StringType::from_data(
                                vec!["CreateTableAsSelect:", ""],
                            )])];
                        res.extend(
                            self.explain_query(s_expr, metadata, bind_context, formatted_ast)
                                .await?,
                        );
                        vec![DataBlock::concat(&res)?]
                    }
                    _ => self.explain_plan(&self.plan)?,
                },
                Plan::InsertMultiTable(plan) => {
                    let physical_plan = InsertMultiTableInterpreter::try_create_static(
                        self.ctx.clone(),
                        *plan.clone(),
                    )?
                    .build_physical_plan()
                    .await?;
                    self.explain_physical_plan(&physical_plan, &plan.meta_data, &None)
                        .await?
                }
                Plan::MergeInto {
                    s_expr,
                    schema,
                    metadata,
                } => {
                    let merge_into: MergeInto = s_expr.plan().clone().try_into()?;
                    let interpreter = MergeIntoInterpreter::try_create(
                        self.ctx.clone(),
                        *s_expr.clone(),
                        schema.clone(),
                    )?;
                    let plan = interpreter.build_physical_plan(&merge_into).await?;
                    self.explain_physical_plan(&plan, metadata, &None).await?
                }
                Plan::Delete(plan) => self.explain_delete(plan).await?,
                _ => self.explain_plan(&self.plan)?,
            },

            ExplainKind::Join => match &self.plan {
                Plan::Query {
                    s_expr,
                    metadata,
                    bind_context,
                    ..
                } => {
                    let ctx = self.ctx.clone();
                    let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx, true);
                    let plan = builder.build(s_expr, bind_context.column_set()).await?;
                    self.explain_join_order(&plan, metadata)?
                }
                _ => Err(ErrorCode::Unimplemented(
                    "Unsupported EXPLAIN JOIN statement",
                ))?,
            },

            ExplainKind::AnalyzePlan => match &self.plan {
                Plan::Query {
                    s_expr,
                    metadata,
                    bind_context,
                    ignore_result,
                    ..
                } => {
                    self.explain_analyze(
                        s_expr,
                        metadata,
                        bind_context.column_set(),
                        *ignore_result,
                    )
                    .await?
                }
                Plan::MergeInto { s_expr, .. } => {
                    let plan: MergeInto = s_expr.plan().clone().try_into()?;
                    self.explain_analyze(
                        s_expr.child(0)?,
                        &plan.meta_data,
                        *plan.columns_set.clone(),
                        true,
                    )
                    .await?
                }
                _ => Err(ErrorCode::Unimplemented(
                    "Unsupported EXPLAIN ANALYZE statement",
                ))?,
            },

            ExplainKind::Pipeline => {
                // todo:(JackTan25), we need to make all execute2() just do `build pipeline` work,
                // don't take real actions. for now we fix #13657 like below.
                let pipeline = match &self.plan {
                    Plan::Query { .. } | Plan::MergeInto { .. } => {
                        let interpter =
                            InterpreterFactory::get(self.ctx.clone(), &self.plan).await?;
                        interpter.execute2().await?
                    }
                    _ => PipelineBuildResult::create(),
                };

                Self::format_pipeline(&pipeline)
            }

            ExplainKind::Fragments => match &self.plan {
                Plan::Query {
                    s_expr,
                    metadata,
                    bind_context,
                    ..
                } => {
                    self.explain_fragments(
                        *s_expr.clone(),
                        metadata.clone(),
                        bind_context.column_set(),
                    )
                    .await?
                }
                Plan::MergeInto { s_expr, schema, .. } => {
                    self.explain_merge_fragments(*s_expr.clone(), schema.clone())
                        .await?
                }
                Plan::Update(update) => self.explain_update_fragments(update.as_ref()).await?,
                _ => {
                    return Err(ErrorCode::Unimplemented("Unsupported EXPLAIN statement"));
                }
            },

            ExplainKind::Graph => {
                return Err(ErrorCode::Unimplemented(
                    "ExplainKind graph is unimplemented",
                ));
            }

            ExplainKind::Ast(display_string)
            | ExplainKind::Syntax(display_string)
            | ExplainKind::Memo(display_string) => {
                let line_split_result: Vec<&str> = display_string.lines().collect();
                let column = StringType::from_data(line_split_result);
                vec![DataBlock::new_from_columns(vec![column])]
            }
        };

        PipelineBuildResult::from_blocks(blocks)
    }
}

impl ExplainInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: Plan,
        kind: ExplainKind,
        config: ExplainConfig,
    ) -> Result<Self> {
        Ok(ExplainInterpreter {
            ctx,
            plan,
            kind,
            config,
        })
    }

    pub fn explain_plan(&self, plan: &Plan) -> Result<Vec<DataBlock>> {
        let result = plan.format_indent(self.config.verbose)?;
        let line_split_result: Vec<&str> = result.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    pub async fn explain_physical_plan(
        &self,
        plan: &PhysicalPlan,
        metadata: &MetadataRef,
        formatted_ast: &Option<String>,
    ) -> Result<Vec<DataBlock>> {
        if self.ctx.get_settings().get_enable_query_result_cache()? && self.ctx.get_cacheable() {
            let key = gen_result_cache_key(formatted_ast.as_ref().unwrap());
            let kv_store = UserApiProvider::instance().get_meta_store_client();
            let cache_reader = ResultCacheReader::create(
                self.ctx.clone(),
                &key,
                kv_store.clone(),
                self.ctx
                    .get_settings()
                    .get_query_result_cache_allow_inconsistent()?,
            );
            if let Some(v) = cache_reader.check_cache().await? {
                // Construct a format tree for result cache reading
                let children = vec![
                    FormatTreeNode::new(format!("SQL: {}", v.sql)),
                    FormatTreeNode::new(format!("Number of rows: {}", v.num_rows)),
                    FormatTreeNode::new(format!("Result size: {}", v.result_size)),
                ];

                let format_tree =
                    FormatTreeNode::with_children("ReadQueryResultCache".to_string(), children);

                let result = format_tree.format_pretty()?;
                let line_split_result: Vec<&str> = result.lines().collect();
                let formatted_plan = StringType::from_data(line_split_result);
                return Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])]);
            }
        }

        let result = plan
            .format(metadata.clone(), Default::default())?
            .format_pretty()?;
        let line_split_result: Vec<&str> = result.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    pub fn explain_join_order(
        &self,
        plan: &PhysicalPlan,
        metadata: &MetadataRef,
    ) -> Result<Vec<DataBlock>> {
        let result = plan.format_join(metadata)?.format_pretty()?;
        let line_split_result: Vec<&str> = result.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    fn format_pipeline(build_res: &PipelineBuildResult) -> Vec<DataBlock> {
        let mut blocks = Vec::with_capacity(1 + build_res.sources_pipelines.len());
        // Format root pipeline
        let line_split_result = format!("{}", build_res.main_pipeline.display_indent())
            .lines()
            .map(|l| l.to_string())
            .collect::<Vec<_>>();
        let column = StringType::from_data(line_split_result);
        blocks.push(DataBlock::new_from_columns(vec![column]));
        // Format child pipelines
        for pipeline in build_res.sources_pipelines.iter() {
            let line_split_result = format!("\n{}", pipeline.display_indent())
                .lines()
                .map(|l| l.to_string())
                .collect::<Vec<_>>();
            let column = StringType::from_data(line_split_result);
            blocks.push(DataBlock::new_from_columns(vec![column]));
        }
        blocks
    }

    #[async_backtrace::framed]
    async fn explain_fragments(
        &self,
        s_expr: SExpr,
        metadata: MetadataRef,
        required: ColumnSet,
    ) -> Result<Vec<DataBlock>> {
        let ctx = self.ctx.clone();
        let plan = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), true)
            .build(&s_expr, required)
            .await?;

        let root_fragment = Fragmenter::try_create(ctx.clone())?.build_fragment(&plan)?;

        let mut fragments_actions = QueryFragmentsActions::create(ctx.clone());
        root_fragment.get_actions(ctx, &mut fragments_actions)?;

        let display_string = fragments_actions.display_indent(&metadata).to_string();
        let line_split_result = display_string.lines().collect::<Vec<_>>();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    #[async_backtrace::framed]
    async fn explain_update_fragments(&self, update: &UpdatePlan) -> Result<Vec<DataBlock>> {
        let interpreter = UpdateInterpreter::try_create(self.ctx.clone(), update.clone())?;
        let display_string = if let Some(plan) = interpreter.get_physical_plan().await? {
            let root_fragment = Fragmenter::try_create(self.ctx.clone())?.build_fragment(&plan)?;

            let mut fragments_actions = QueryFragmentsActions::create(self.ctx.clone());
            root_fragment.get_actions(self.ctx.clone(), &mut fragments_actions)?;

            let ident = fragments_actions.display_indent(&update.metadata);
            ident.to_string()
        } else {
            "Nothing to update".to_string()
        };
        let line_split_result = display_string.lines().collect::<Vec<_>>();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    #[async_backtrace::framed]
    async fn explain_analyze(
        &self,
        s_expr: &SExpr,
        metadata: &MetadataRef,
        required: ColumnSet,
        ignore_result: bool,
    ) -> Result<Vec<DataBlock>> {
        let mut builder = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), true);
        let plan = builder.build(s_expr, required).await?;
        let build_res = build_query_pipeline(&self.ctx, &[], &plan, ignore_result).await?;

        // Drain the data
        let query_profiles = self.execute_and_get_profiles(build_res)?;

        let result = plan
            .format(metadata.clone(), query_profiles)?
            .format_pretty()?;
        let line_split_result: Vec<&str> = result.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    fn execute_and_get_profiles(
        &self,
        mut build_res: PipelineBuildResult,
    ) -> Result<HashMap<u32, PlanProfile>> {
        let settings = self.ctx.get_settings();
        build_res.set_max_threads(settings.get_max_threads()? as usize);
        let settings = ExecutorSettings::try_create(self.ctx.clone())?;

        match build_res.main_pipeline.is_complete_pipeline()? {
            true => {
                let mut pipelines = build_res.sources_pipelines;
                pipelines.push(build_res.main_pipeline);

                let executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
                executor.execute()?;
                self.ctx
                    .add_query_profiles(&executor.get_inner().fetch_profiling(false));
            }
            false => {
                let mut executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;
                executor.start();
                while (executor.pull_data()?).is_some() {}
                self.ctx
                    .add_query_profiles(&executor.get_inner().fetch_profiling(false));
            }
        }

        Ok(self
            .ctx
            .get_query_profiles()
            .into_iter()
            .filter(|x| x.id.is_some())
            .map(|x| (x.id.unwrap(), x))
            .collect::<HashMap<_, _>>())
    }

    async fn explain_query(
        &self,
        s_expr: &SExpr,
        metadata: &MetadataRef,
        bind_context: &BindContext,
        formatted_ast: &Option<String>,
    ) -> Result<Vec<DataBlock>> {
        let ctx = self.ctx.clone();
        // If `formatted_ast` is Some, it means we may use query result cache.
        // If we use result cache for this query,
        // we should not use `dry_run` mode to build the physical plan.
        // It's because we need to get the same partitions as the original selecting plan.
        let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx, formatted_ast.is_none());
        let plan = builder.build(s_expr, bind_context.column_set()).await?;
        self.explain_physical_plan(&plan, metadata, formatted_ast)
            .await
    }

    #[async_backtrace::framed]
    async fn explain_delete(&self, delete: &DeletePlan) -> Result<Vec<DataBlock>> {
        let table_index = delete
            .metadata
            .read()
            .get_table_index(
                Some(delete.database_name.as_str()),
                delete.table_name.as_str(),
            )
            .unwrap();

        let mut result = vec![];
        // Explain subquery.
        if let Some(subquery_desc) = &delete.subquery_desc {
            let row_id_column_binding = ColumnBindingBuilder::new(
                ROW_ID_COL_NAME.to_string(),
                subquery_desc.index,
                Box::new(DataType::Number(NumberDataType::UInt64)),
                Visibility::InVisible,
            )
            .database_name(Some(delete.database_name.clone()))
            .table_name(Some(delete.table_name.clone()))
            .table_index(Some(table_index))
            .build();
            let mut bind_context = delete.bind_context.clone();
            bind_context.columns.clear();
            bind_context.columns.push(row_id_column_binding.clone());

            let s_expr = SExpr::create_unary(
                Arc::new(RelOperator::EvalScalar(EvalScalar {
                    items: vec![ScalarItem {
                        scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: None,
                            column: row_id_column_binding,
                        }),
                        index: 0,
                    }],
                })),
                Arc::new(subquery_desc.input_expr.clone()),
            );

            let formatted_plan = StringType::from_data(vec!["DeletePlan (subquery):"]);
            result.push(DataBlock::new_from_columns(vec![formatted_plan]));
            let input = self
                .explain_query(&s_expr, &delete.metadata, &bind_context, &None)
                .await?;
            result.extend(input);
        }

        // Explain selection.
        if let Some(selection) = &delete.selection
            && !matches!(selection, ScalarExpr::SubqueryExpr(_))
        {
            let selection = if let ScalarExpr::FunctionCall(FunctionCall {
                func_name,
                arguments,
                ..
            }) = selection
                && func_name == "and"
            {
                arguments[1].clone()
            } else {
                selection.clone()
            };

            let scan = RelOperator::Scan(Scan {
                table_index,
                columns: delete.bind_context.column_set(),
                push_down_predicates: None,
                limit: None,
                order_by: None,
                prewhere: None,
                agg_index: None,
                change_type: None,
                inverted_index: None,
                statistics: Default::default(),
                update_stream_columns: false,
            });
            let scan_expr = SExpr::create_leaf(Arc::new(scan));
            let filter = RelOperator::Filter(Filter {
                predicates: vec![selection],
            });
            let s_expr = SExpr::create_unary(Arc::new(filter), Arc::new(scan_expr));

            let formatted_plan = StringType::from_data(vec!["DeletePlan (selection):"]);
            result.push(DataBlock::new_from_columns(vec![formatted_plan]));
            let input = self
                .explain_query(&s_expr, &delete.metadata, &delete.bind_context, &None)
                .await?;
            result.extend(input);
        }

        if result.is_empty() {
            let table_name = format!(
                "{}.{}.{}",
                delete.catalog_name, delete.database_name, delete.table_name
            );
            let children = vec![FormatTreeNode::new(format!("table: {table_name}"))];
            let formatted_plan = FormatTreeNode::with_children("DeletePlan:".to_string(), children)
                .format_pretty()?;
            let line_split_result: Vec<&str> = formatted_plan.lines().collect();
            let formatted_plan = StringType::from_data(line_split_result);
            result.push(DataBlock::new_from_columns(vec![formatted_plan]));
        }
        Ok(vec![DataBlock::concat(&result)?])
    }

    async fn explain_merge_fragments(
        &self,
        s_expr: SExpr,
        schema: DataSchemaRef,
    ) -> Result<Vec<DataBlock>> {
        let merge_into: databend_common_sql::plans::MergeInto = s_expr.plan().clone().try_into()?;
        let interpreter = MergeIntoInterpreter::try_create(self.ctx.clone(), s_expr, schema)?;
        let plan = interpreter.build_physical_plan(&merge_into).await?;
        let root_fragment = Fragmenter::try_create(self.ctx.clone())?.build_fragment(&plan)?;

        let mut fragments_actions = QueryFragmentsActions::create(self.ctx.clone());
        root_fragment.get_actions(self.ctx.clone(), &mut fragments_actions)?;

        let display_string = fragments_actions
            .display_indent(&merge_into.meta_data)
            .to_string();

        let line_split_result = display_string.lines().collect::<Vec<_>>();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }
}
