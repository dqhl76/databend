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

use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_sql::plans::JoinType;

use super::partitioned::PartitionedHashJoin;
use super::partitioned::PartitionedJoinState;
use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::InnerHashJoin;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::memory::AntiLeftHashJoin;
use crate::pipelines::processors::transforms::memory::AntiRightHashJoin;
use crate::pipelines::processors::transforms::memory::OuterRightHashJoin;
use crate::pipelines::processors::transforms::memory::SemiLeftHashJoin;
use crate::pipelines::processors::transforms::memory::SemiRightHashJoin;
use crate::pipelines::processors::transforms::memory::left_join::OuterLeftHashJoin;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

pub struct HashJoinFactory {
    ctx: Arc<QueryContext>,
    desc: Arc<HashJoinDesc>,
    hash_method: HashMethodKind,
    function_ctx: FunctionContext,
}

impl HashJoinFactory {
    pub fn create(
        ctx: Arc<QueryContext>,
        function_ctx: FunctionContext,
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
    ) -> Arc<HashJoinFactory> {
        Arc::new(HashJoinFactory {
            ctx,
            desc,
            function_ctx,
            hash_method: method,
        })
    }

    /// Create the state shared by all processors of one hash join recursion level.
    ///
    /// Partitioning and spilling are disabled (keeping the single-partition flat path) when:
    /// - `shared_build_chunks` is true (`CacheScan` reads `flat_basic.chunks` during probing),
    /// - there are no join keys (cross join), or
    /// - any join key or residual predicate is non-deterministic (e.g. `running_difference`,
    ///   `rand`, `nextval`), since partitioning re-evaluates keys and reorders rows across blocks.
    pub fn create_level_state(
        self: &Arc<Self>,
        level: usize,
        shared_build_chunks: bool,
    ) -> Result<Arc<PartitionedJoinState>> {
        let deterministic_exprs = self
            .desc
            .build_keys
            .iter()
            .chain(self.desc.probe_keys.iter())
            .chain(self.desc.other_predicate.iter())
            .all(|expr| expr.is_deterministic(&BUILTIN_FUNCTIONS));
        let allow_spill =
            !shared_build_chunks && !self.desc.build_keys.is_empty() && deterministic_exprs;
        PartitionedJoinState::create(self.ctx.clone(), level, self.clone(), allow_spill)
    }

    pub fn create_hash_join(
        self: &Arc<Self>,
        typ: JoinType,
        state: Arc<PartitionedJoinState>,
    ) -> Result<Box<dyn Join>> {
        Ok(Box::new(self.create_partitioned_join(typ, state)?))
    }

    /// Create an in-memory join on a single `BasicHashJoinState` (used for the flat fast path and
    /// for each resident radix partition).
    pub fn create_memory_join(
        self: &Arc<Self>,
        typ: JoinType,
        basic_state: Arc<BasicHashJoinState>,
    ) -> Result<Box<dyn Join>> {
        match typ {
            JoinType::Inner => Ok(Box::new(InnerHashJoin::create(
                &self.ctx.get_settings(),
                self.function_ctx.clone(),
                self.hash_method.clone(),
                self.desc.clone(),
                basic_state,
            )?)),
            JoinType::Left => Ok(Box::new(OuterLeftHashJoin::create(
                &self.ctx,
                self.function_ctx.clone(),
                self.hash_method.clone(),
                self.desc.clone(),
                basic_state,
            )?)),
            JoinType::LeftAnti => Ok(Box::new(AntiLeftHashJoin::create(
                &self.ctx,
                self.function_ctx.clone(),
                self.hash_method.clone(),
                self.desc.clone(),
                basic_state,
            )?)),
            JoinType::LeftSemi => Ok(Box::new(SemiLeftHashJoin::create(
                &self.ctx,
                self.function_ctx.clone(),
                self.hash_method.clone(),
                self.desc.clone(),
                basic_state,
            )?)),
            JoinType::Right => Ok(Box::new(OuterRightHashJoin::create(
                &self.ctx,
                self.function_ctx.clone(),
                self.hash_method.clone(),
                self.desc.clone(),
                basic_state,
            )?)),
            JoinType::RightSemi => Ok(Box::new(SemiRightHashJoin::create(
                &self.ctx,
                self.function_ctx.clone(),
                self.hash_method.clone(),
                self.desc.clone(),
                basic_state,
            )?)),
            JoinType::RightAnti => Ok(Box::new(AntiRightHashJoin::create(
                &self.ctx,
                self.function_ctx.clone(),
                self.hash_method.clone(),
                self.desc.clone(),
                basic_state,
            )?)),
            _ => unreachable!(),
        }
    }

    pub fn create_partitioned_join(
        self: &Arc<Self>,
        typ: JoinType,
        state: Arc<PartitionedJoinState>,
    ) -> Result<PartitionedHashJoin> {
        let flat_join = if state.can_spill() {
            None
        } else {
            Some(self.create_memory_join(typ, state.flat_basic.clone())?)
        };
        let memory_settings = MemorySettings::from_join_settings(&self.ctx)?;

        PartitionedHashJoin::create(
            self.ctx.clone(),
            self.function_ctx.clone(),
            self.hash_method.clone(),
            self.desc.clone(),
            memory_settings,
            state,
            flat_join,
            typ,
        )
    }
}
