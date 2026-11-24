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

use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_expression::ColumnVec;
use databend_common_expression::DataBlock;
use databend_common_expression::types::DataType;

use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::new_hash_join::common::CStyleCell;

pub struct BasicHashJoinState {
    pub mutex: Mutex<()>,
    /// True when `TransformCacheScan` shares `chunks` (requires in-place projection in
    /// `final_build` and keeping `chunks` alive during probing).
    pub shared_build_chunks: bool,
    /// When true (optimistic `Single` build that may abort mid-`BuildFinal` on `check_spill()`),
    /// `final_build` leaves `chunks` untouched in full `build_schema` so they can be repartitioned
    /// cleanly if aborted, and `finalize_chunks` projects `build_projection` and clears `chunks`.
    pub preserve_build_chunks: AtomicBool,
    pub chunks_finalized: AtomicBool,

    pub build_rows: CStyleCell<usize>,
    pub chunks: CStyleCell<Vec<DataBlock>>,
    pub columns: CStyleCell<Vec<ColumnVec>>,
    pub column_types: CStyleCell<Vec<DataType>>,
    pub build_queue: CStyleCell<VecDeque<usize>>,

    pub arenas: CStyleCell<Vec<Vec<u8>>>,
    pub hash_table: CStyleCell<HashJoinHashTable>,
    pub packets: CStyleCell<Vec<JoinRuntimeFilterPacket>>,

    pub scan_map: CStyleCell<Vec<Vec<u8>>>,
    pub scan_queue: CStyleCell<VecDeque<usize>>,
}

impl BasicHashJoinState {
    pub fn create() -> Self {
        Self::with_shared_build_chunks(false)
    }

    pub fn with_shared_build_chunks(shared_build_chunks: bool) -> Self {
        BasicHashJoinState {
            mutex: Mutex::new(()),
            shared_build_chunks,
            preserve_build_chunks: AtomicBool::new(false),
            chunks_finalized: AtomicBool::new(false),
            build_rows: CStyleCell::new(0),
            chunks: CStyleCell::new(Vec::new()),
            columns: CStyleCell::new(Vec::new()),
            column_types: CStyleCell::new(Vec::new()),
            build_queue: CStyleCell::new(VecDeque::new()),
            arenas: CStyleCell::new(Vec::new()),
            hash_table: CStyleCell::new(HashJoinHashTable::Null),
            packets: CStyleCell::new(Vec::new()),
            scan_map: CStyleCell::new(Vec::new()),
            scan_queue: CStyleCell::new(VecDeque::new()),
        }
    }

    pub(crate) fn push_chunk(&self, chunk: DataBlock) {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        *self.build_rows.as_mut() += chunk.num_rows();
        let chunk_index = self.chunks.len();
        self.chunks.as_mut().push(chunk);
        self.build_queue.as_mut().push_back(chunk_index);
        self.scan_map.as_mut().push(vec![]);
        self.scan_queue.as_mut().push_back(chunk_index);
    }

    /// Add a build chunk that is built later, when [`Self::queue_build_chunks`] queues it.
    /// Returns its chunk index.
    pub(crate) fn push_unqueued_chunk(&self, chunk: DataBlock) -> usize {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        *self.build_rows.as_mut() += chunk.num_rows();
        let chunk_index = self.chunks.len();
        self.chunks.as_mut().push(chunk);
        self.scan_map.as_mut().push(vec![]);
        self.scan_queue.as_mut().push_back(chunk_index);
        chunk_index
    }

    /// Queue chunks added by [`Self::push_unqueued_chunk`] to be built into the hash table.
    pub(crate) fn queue_build_chunks(&self, chunk_indexes: &[usize]) {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);
        self.build_queue.as_mut().extend(chunk_indexes);
    }

    /// Stop other threads from stealing any more chunks when aborting a `Single` build.
    pub(crate) fn clear_build_queue(&self) {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);
        self.build_queue.as_mut().clear();
    }

    /// Take all untouched `build_schema` chunks and reset the basic state for transition to
    /// partitioned mode (either during `Stage::Build` or when aborting `Single` in
    /// `Stage::BuildFinal` past the barrier).
    pub(crate) fn take_all_chunks_for_transition(&self) -> Vec<DataBlock> {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        let chunks = std::mem::take(self.chunks.as_mut());
        *self.build_rows.as_mut() = 0;
        self.columns.as_mut().clear();
        self.column_types.as_mut().clear();
        self.arenas.as_mut().clear();
        self.build_queue.as_mut().clear();
        self.scan_map.as_mut().clear();
        self.scan_queue.as_mut().clear();
        *self.hash_table.as_mut() = HashJoinHashTable::Null;
        self.chunks_finalized.store(false, Ordering::Release);
        self.preserve_build_chunks.store(false, Ordering::Release);
        chunks
    }

    /// Take chunks that will never be built, leaving `placeholder` (an empty block in the form
    /// built chunks have) in their place so the indexes of the other chunks stay valid.
    pub(crate) fn take_unbuilt_chunks(
        &self,
        chunk_indexes: &[usize],
        placeholder: &DataBlock,
    ) -> Vec<DataBlock> {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        let chunks = self.chunks.as_mut();
        let mut taken = Vec::with_capacity(chunk_indexes.len());
        for &chunk_index in chunk_indexes {
            let chunk = std::mem::replace(&mut chunks[chunk_index], placeholder.clone());
            *self.build_rows.as_mut() -= chunk.num_rows();
            taken.push(chunk);
        }
        taken
    }

    /// Release all build chunks, hash table, arenas, columns, and scan state.
    pub(crate) fn clear(&self) {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        self.columns.as_mut().clear();
        self.chunks.as_mut().clear();
        *self.build_rows.as_mut() = 0;
        self.column_types.as_mut().clear();
        self.arenas.as_mut().clear();
        self.build_queue.as_mut().clear();
        self.scan_map.as_mut().clear();
        self.scan_queue.as_mut().clear();
        *self.hash_table.as_mut() = HashJoinHashTable::Null;
        self.chunks_finalized.store(false, Ordering::Release);
        self.preserve_build_chunks.store(false, Ordering::Release);
    }

    pub fn steal_scan_chunk_index(&self) -> Option<(usize, usize)> {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);
        self.scan_queue.as_mut().pop_front().map(|x| (x, 0))
    }

    pub fn steal_chunk_index(&self) -> Option<usize> {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);
        self.build_queue.as_mut().pop_front()
    }

    /// Steal the next chunk index together with a cheap refcounted handle to its intact block.
    pub fn steal_chunk_with_block(&self) -> Option<(usize, DataBlock)> {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);
        let idx = self.build_queue.as_mut().pop_front()?;
        let block = self.chunks[idx].clone();
        Some((idx, block))
    }
}
