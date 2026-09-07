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

use databend_common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

use crate::DataBlock;
use crate::types::DataType;

/// A query-owned spill run. The storage implementation defines its metadata.
/// References may travel between query workers, but must never be persisted in
/// aggregate-state tables or returned by an aggregate `_state` function.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregateSpillFile {
    pub location: String,
    pub metadata: Vec<u8>,
}

pub type AggregateSpillReader = Box<dyn Iterator<Item = Result<DataBlock>> + Send>;

/// Synchronous aggregate-function I/O backed by the query's asynchronous spill
/// buffers. Implementations own file cleanup and must make runs readable by all
/// workers participating in the query. Restoring a run must stream bounded
/// blocks, rather than materializing the entire run.
pub trait AggregateFunctionSpill: Send + Sync {
    /// Soft budget for a restored partition or external merge buffer. This is
    /// independent of the decision to spill a state during normal accumulation.
    fn restore_memory_limit(&self) -> usize;

    /// Select a state to spill under memory pressure, or when explicitly forced.
    /// State size alone must not trigger spilling, and repeated pressure must
    /// not bypass the configured minimum state size. Recursive restore uses
    /// `restore_memory_limit` instead, so forced spilling cannot prevent progress.
    fn should_spill(&self, memory_bytes: usize) -> bool;

    fn check_interrupt(&self) -> Result<()>;

    fn spill(&self, block: DataBlock) -> Result<AggregateSpillFile>;

    fn restore(
        &self,
        file: &AggregateSpillFile,
        data_types: &[DataType],
    ) -> Result<AggregateSpillReader>;
}
