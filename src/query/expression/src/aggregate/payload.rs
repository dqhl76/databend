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

use std::alloc::Layout;
use std::alloc::alloc;
use std::alloc::dealloc;
use std::alloc::handle_alloc_error;
use std::ptr::NonNull;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::Result;
use log::info;

use super::AggrState;
use super::AggrStateLoc;
use super::AggrStateType;
use super::BATCH_SIZE;
use super::PayloadFlushState;
use super::RowID;
use super::StateAddr;
use super::StatesLayout;
use super::aggregate_function::AggregateCallRef;
use super::group_hash_route_index;
use super::payload_row::IsInArena;
use super::payload_row::rowformat_size;
use super::payload_row::serialize_column_to_rowformat;
use super::payload_row::serialize_const_column_to_rowformat;
use super::payload_row::visit_row_type;
use super::row_ptr::PageNumbering;
use super::row_ptr::RowLayout;
use super::row_ptr::RowMut;
use super::row_ptr::RowPtr;
use super::row_ptr::StatesRegion;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::ProjectedBlock;
use crate::types::DataType;

// payload layout
// A page holds its rows, then the aggregate states of its rows in the same order; see
// `StatesRegion`. A row is
// [VALIDITY][GROUPS][HASH]
// [VALIDITY] is the validity bits of the data columns (including the HASH)
// [GROUPS] is the group data, could be multiple values, fixed size, strings are elsewhere
// [HASH] is the hash data of the groups
//
// The states of a row are found from its address, so moving rows between payloads moves their
// states bitwise: a state must never keep a pointer to itself.
pub struct Payload {
    pub(super) arena: Arc<Bump>,
    pub(super) group_types: Vec<DataType>,
    pub(super) aggrs: Vec<AggregateCallRef>,
    pub(super) row_layout: RowLayout,

    pub(super) pages: Vec<Page>,
    pub(super) tuple_size: usize,
    pub(super) row_per_page: usize,
    page_bytes: usize,

    total_rows: usize,

    // Starts from 1, zero means no page allocated
    current_write_page: usize,
    /// Set to `n` when this is the partition of a table routed to `n` final aggregates, see
    /// `PartitionMask::Route`: its rows all go to the final aggregate of its partition.
    pub(super) routed_partitions: usize,
    max_page_size: usize,
}

/// The ids of the next rows of a page, reserved one by one by a probe: see `RowReserver`.
#[derive(Clone, Copy)]
pub(in crate::aggregate) struct RowCursor {
    pub(super) id: u64,
    pub(super) end: u64,
    /// The index of the page in its payload.
    pub(super) page: usize,
}

impl RowCursor {
    pub(super) const EMPTY: RowCursor = RowCursor {
        id: 0,
        end: 0,
        page: usize::MAX,
    };
}

/// Where `Payload::move_rows_into` writes the next rows of a destination: the free rows of its
/// writable page.
struct MoveCursor {
    /// The first row of the page, and the states of its first row.
    base: *mut u8,
    states: *mut u8,
    /// The index of the page in its payload, the rows it held before, and the next row and
    /// the end of its rows.
    page: usize,
    start: usize,
    row: usize,
    end: usize,
}

impl MoveCursor {
    const EMPTY: MoveCursor = MoveCursor {
        base: std::ptr::null_mut(),
        states: std::ptr::null_mut(),
        page: usize::MAX,
        start: 0,
        row: 0,
        end: 0,
    };

    /// The free rows of the writable page of `dest`, none if it has no page or a full one.
    fn open(dest: &mut Payload) -> MoveCursor {
        match dest.current_write_page.checked_sub(1) {
            Some(page) if dest.pages[page].rows < dest.row_per_page => Self::at(dest, page),
            _ => Self::EMPTY,
        }
    }

    /// The rows of the next page of `dest`, once its cursor was committed.
    fn open_next(dest: &mut Payload) -> MoveCursor {
        let (_, page) = dest.writable_page();
        Self::at(dest, page)
    }

    fn at(dest: &mut Payload, page: usize) -> MoveCursor {
        let offset = dest.row_layout.states.offset;
        let data = &mut dest.pages[page];
        let base = data.data.as_mut_ptr();
        MoveCursor {
            base,
            states: unsafe { base.add(offset) },
            page,
            start: data.rows,
            row: data.rows,
            end: dest.row_per_page,
        }
    }

    /// Count the rows written in the page, with their states, which moved with them. Committing
    /// again counts only the rows written since.
    fn commit(&mut self, dest: &mut Payload) {
        if self.page == usize::MAX {
            return;
        }
        let agg_len = dest.aggrs.len();
        let page = &mut dest.pages[self.page];
        page.rows = self.row;
        page.initialized_states = self.row * agg_len;
        dest.total_rows += self.row - self.start;
        self.start = self.row;
    }
}

/// The rows of a payload whose states a `RowMover` owns until they are moved.
enum Unmoved<'a> {
    /// Every row of the pages, in order.
    Pages(&'a [Page], usize),
    Rows(&'a [RowPtr]),
}

/// Moves rows of a payload with their states to other payloads, see `Payload::move_rows_into`
/// and `Payload::move_rows_to`. The source gives its states up to the mover, which owns the
/// states of `unmoved` from row `moved` on, and `dropped`. A row moved belongs to its destination
/// once the cursor of the destination is committed. Dropping the mover, when done or on a panic,
/// as an allocation that failed under the memory limit raises, commits the cursors and drops the
/// states it still owns: each state is dropped once, and none leaks.
struct RowMover<'a> {
    aggrs: &'a [AggregateCallRef],
    states_layout: Option<StatesLayout>,
    states: StatesRegion,
    dests: &'a mut [Payload],
    cursors: Vec<MoveCursor>,
    unmoved: Unmoved<'a>,
    moved: usize,
    dropped: &'a [StateAddr],
}

impl<'a> RowMover<'a> {
    /// A mover of the rows `unmoved` of a payload of aggregates `aggrs`, which gave up its states,
    /// `states_layout`, to the mover. The destinations have the layout of the payload, see
    /// `Payload::same_layout`.
    fn new(
        aggrs: &'a [AggregateCallRef],
        states_layout: Option<StatesLayout>,
        states: StatesRegion,
        unmoved: Unmoved<'a>,
        dests: &'a mut [Payload],
        dropped: &'a [StateAddr],
    ) -> Self {
        let mut mover = RowMover {
            aggrs,
            states_layout,
            states,
            dests,
            cursors: vec![],
            unmoved,
            moved: 0,
            dropped,
        };
        // Opening a page may fail: the mover, which now owns the states, drops them then.
        mover.cursors = mover.dests.iter_mut().map(MoveCursor::open).collect();
        mover
    }

    /// Move the row at `src`, the next row the mover owns, with its states at `src_states`, to
    /// `dests[dest]`, copying its group values at `arena_offsets` into the arena of the
    /// destination.
    ///
    /// # Safety
    ///
    /// `src` is the row of the source numbered `moved`, and the destinations have its layout.
    #[inline(always)]
    unsafe fn move_row(
        &mut self,
        dest: usize,
        src: *const u8,
        src_states: *const u8,
        arena_offsets: &[usize],
    ) {
        let tuple_size = self.dests[dest].tuple_size;
        let state_size = self.states.size;
        let cursor = &mut self.cursors[dest];
        if cursor.row == cursor.end {
            let payload = &mut self.dests[dest];
            cursor.commit(payload);
            *cursor = MoveCursor::open_next(payload);
        }
        // SAFETY: the cursor is at a free row of a page of the destination, of the row and state
        // sizes of the source. The row is counted, and its states change owner, only once every
        // allocation for it is done.
        unsafe {
            let dst = cursor.base.add(cursor.row * tuple_size);
            copy_small(src, dst, tuple_size);
            if !arena_offsets.is_empty() {
                let mut dst = RowMut::new(dst);
                let src = RowPtr::new(src);
                let arena = &self.dests[dest].arena;
                for &offset in arena_offsets {
                    dst.write_bytes(offset, arena.alloc_slice_copy(src.read_bytes(offset)));
                }
            }
            copy_small(
                src_states,
                cursor.states.add(cursor.row * state_size),
                state_size,
            );
        }
        cursor.row += 1;
        self.moved += 1;
    }
}

impl Drop for RowMover<'_> {
    fn drop(&mut self) {
        for (cursor, dest) in self.cursors.iter_mut().zip(self.dests.iter_mut()) {
            cursor.commit(dest);
        }
        let Some(states_layout) = self.states_layout.take() else {
            return;
        };
        let agg_len = self.aggrs.len();
        drop_guard(move || {
            for (idx, (aggr, loc)) in self
                .aggrs
                .iter()
                .zip(states_layout.states_loc.iter())
                .enumerate()
            {
                if !aggr.state().need_manual_drop() {
                    continue;
                }
                let drop_state = |addr: StateAddr| unsafe {
                    aggr.drop_state(AggrState::new(addr, loc));
                };
                match self.unmoved {
                    Unmoved::Pages(pages, tuple_size) => {
                        let mut skip = self.moved;
                        for page in pages {
                            let rows = page.rows;
                            let first = skip.min(rows);
                            skip -= first;
                            for row in first..rows {
                                // A row whose states were not all initialized, at a failed
                                // allocation, has only those dropped.
                                if row * agg_len + idx >= page.initialized_states {
                                    break;
                                }
                                let ptr = page.row_ptr(row, tuple_size).as_ptr();
                                drop_state(StateAddr::from(self.states.ptr(ptr)));
                            }
                        }
                    }
                    Unmoved::Rows(rows) => {
                        for row in &rows[self.moved..] {
                            drop_state(StateAddr::from(self.states.ptr(row.as_ptr())));
                        }
                    }
                }
                for addr in self.dropped {
                    drop_state(*addr);
                }
            }
        });
    }
}

#[derive(Clone, Copy)]
pub(super) enum GroupRows<'a> {
    Columns(ProjectedBlock<'a>),
    Rows(&'a [RowPtr; BATCH_SIZE]),
}

unsafe impl Send for Payload {}
unsafe impl Sync for Payload {}

pub(super) struct Page {
    // Probe and flush states hold row pointers into this buffer, and hash indexes hold row
    // ids of it. A page must not reallocate once its rows are published.
    data: PageData,
    pub(super) rows: usize,
    /// The aggregate states initialized, of the rows in order: `rows * aggregates` once every
    /// row has its states. Only these are dropped with the page.
    initialized_states: usize,
}

/// The bytes of a page, aligned to `StatesRegion::page_align`.
struct PageData {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl PageData {
    fn with_capacity(capacity: usize, align: usize) -> Self {
        let layout = Layout::from_size_align(capacity, align).expect("invalid page layout");
        let ptr = match layout.size() {
            0 => NonNull::new(align as *mut u8).expect("alignment is never zero"),
            _ => {
                NonNull::new(unsafe { alloc(layout) }).unwrap_or_else(|| handle_alloc_error(layout))
            }
        };
        PageData { ptr, layout }
    }

    fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }
}

impl Drop for PageData {
    fn drop(&mut self) {
        if self.layout.size() != 0 {
            unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
        }
    }
}

impl Page {
    fn is_partial_state(&self, agg_len: usize) -> bool {
        self.rows * agg_len != self.initialized_states
    }

    /// The address of the first row.
    pub(super) fn base(&self) -> *const u8 {
        self.data.as_ptr()
    }

    pub(super) fn row_ptr(&self, row: usize, row_size: usize) -> RowPtr {
        debug_assert!(row < self.rows);
        RowPtr::new(unsafe { self.data.as_ptr().add(row * row_size) })
    }

    fn reserve_row(&mut self, row_size: usize) -> RowMut {
        let row_offset = self.rows * row_size;
        debug_assert!(row_offset + row_size <= self.data.layout.size());
        self.rows += 1;
        let ptr = unsafe { self.data.as_mut_ptr().add(row_offset) };
        RowMut::new(ptr)
    }
}

impl Payload {
    /// A payload of pages of at most `max_page_size` bytes. A page is aligned to its size
    /// rounded up to a power of two, see `StatesRegion`: the payloads a row is copied between
    /// must have the same page size.
    pub fn new(
        arena: Arc<Bump>,
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateCallRef>,
        states_layout: Option<StatesLayout>,
        max_page_size: usize,
    ) -> Self {
        let mut tuple_size = 0;
        let mut validity_offsets = Vec::with_capacity(group_types.len());
        for x in group_types.iter() {
            if x.is_nullable() {
                validity_offsets.push(tuple_size);
                tuple_size += 1;
            } else {
                validity_offsets.push(0);
            }
        }

        let mut group_offsets = Vec::with_capacity(group_types.len());
        let mut group_sizes = Vec::with_capacity(group_types.len());

        for x in group_types.iter() {
            group_offsets.push(tuple_size);
            let size = rowformat_size(x);
            group_sizes.push(size);
            tuple_size += size;
        }

        let hash_offset = tuple_size;
        tuple_size += size_of::<u64>();

        let (state_size, state_align) = match &states_layout {
            Some(states_layout) if !aggrs.is_empty() => (
                states_layout.layout.pad_to_align().size(),
                states_layout.layout.align(),
            ),
            _ => (0, 1),
        };
        let row_per_page = (u16::MAX as usize)
            .min(max_page_size.saturating_sub(state_align) / (tuple_size + state_size))
            .max(1);
        let states_offset = (row_per_page * tuple_size).next_multiple_of(state_align);
        let page_bytes = states_offset + row_per_page * state_size;
        let states = StatesRegion::new(
            page_bytes.next_power_of_two(),
            tuple_size,
            states_offset,
            state_size,
        );

        Self {
            arena,
            pages: vec![],
            current_write_page: 0,
            group_types,
            aggrs,
            tuple_size,
            row_per_page,
            page_bytes,
            total_rows: 0,
            routed_partitions: 0,
            max_page_size,
            row_layout: RowLayout {
                hash_offset,
                states,
                validity_offsets,
                group_offsets,
                group_sizes,
                states_layout,
            },
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.total_rows
    }

    #[inline]
    pub fn memory_size(&self) -> usize {
        self.total_rows * (self.tuple_size + self.row_layout.states.size)
    }

    pub fn merge_result(&self, flush_state: &mut PayloadFlushState) -> Result<Option<DataBlock>> {
        if !self.flush(flush_state) {
            return Ok(None);
        }

        merge_flushed_states(&self.row_layout, &self.aggrs, flush_state)?;
        let mut entries = flush_state.take_aggregate_results();
        entries.extend(flush_state.take_group_columns());
        Ok(Some(DataBlock::new(entries, flush_state.row_count)))
    }

    #[inline]
    pub(super) fn writable_page(&mut self) -> (&mut Page, usize) {
        if self.current_write_page == 0
            || self.pages[self.current_write_page - 1].rows == self.row_per_page
        {
            self.current_write_page += 1;
            if self.current_write_page > self.pages.len() {
                self.pages.push(Page {
                    data: PageData::with_capacity(
                        self.page_bytes,
                        self.row_layout.states.page_align(),
                    ),
                    rows: 0,
                    initialized_states: 0,
                });
            }
        }
        let page_index = self.current_write_page - 1;
        (&mut self.pages[page_index], page_index)
    }

    pub(super) fn data_ptr(&self, page: &Page, row: usize) -> RowPtr {
        page.row_ptr(row, self.tuple_size)
    }

    /// Whether `other` holds rows of the layout of this payload: the same group types, and the
    /// states of the same aggregates, of the same fields, at the same offsets in their row, and
    /// dropped the same way. Aggregates created apart for the same call, as a partial and a final
    /// aggregate have, are the same.
    pub(super) fn same_layout(&self, other: &Payload) -> bool {
        let same_state = |a: &AggregateCallRef, b: &AggregateCallRef| {
            let (a, b) = (a.state(), b.state());
            a.need_manual_drop() == b.need_manual_drop()
                && a.state_version() == b.state_version()
                && a.fields().len() == b.fields().len()
                && a.fields().iter().zip(b.fields()).all(|pair| match pair {
                    (AggrStateType::Bool, AggrStateType::Bool) => true,
                    (AggrStateType::Custom(a), AggrStateType::Custom(b)) => a == b,
                    _ => false,
                })
        };
        let same_aggrs = self.aggrs.len() == other.aggrs.len()
            && self.aggrs.iter().zip(&other.aggrs).all(|(a, b)| {
                Arc::ptr_eq(a, b) || (a.signature() == b.signature() && same_state(a, b))
            });
        let same_locs = |a: &[AggrStateLoc], b: &[AggrStateLoc]| {
            a.len() == b.len()
                && a.iter().zip(b).all(|pair| match pair {
                    (AggrStateLoc::Bool(i, x), AggrStateLoc::Bool(j, y))
                    | (AggrStateLoc::Custom(i, x), AggrStateLoc::Custom(j, y)) => i == j && x == y,
                    _ => false,
                })
        };
        let same_states = match (
            &self.row_layout.states_layout,
            &other.row_layout.states_layout,
        ) {
            (None, None) => true,
            (Some(a), Some(b)) => {
                a.layout == b.layout
                    && a.states_loc.len() == b.states_loc.len()
                    && a.states_loc
                        .iter()
                        .zip(&b.states_loc)
                        .all(|(a, b)| same_locs(a, b))
            }
            _ => false,
        };
        self.tuple_size == other.tuple_size
            && self.row_layout.hash_offset == other.row_layout.hash_offset
            && self.row_layout.states.size == other.row_layout.states.size
            && self.group_types == other.group_types
            && same_aggrs
            && same_states
    }

    /// Move every row, with its states, to `dests[(hash & mask) >> shift]`, payloads of the
    /// layout of this one. The group values at `arena_offsets`, whose bytes are in an arena, are
    /// copied into the arena of their destination; the other rows of this payload's arena are not
    /// needed after. This payload is left holding rows whose states it does not own.
    pub(super) fn move_rows_into(
        &mut self,
        dests: &mut [Payload],
        mask: u64,
        shift: u64,
        arena_offsets: &[usize],
    ) {
        self.assert_same_layout(dests);
        let tuple_size = self.tuple_size;
        let hash_offset = self.row_layout.hash_offset;
        let src_states = self.row_layout.states;
        let mut mover = RowMover::new(
            &self.aggrs,
            self.row_layout.states_layout.take(),
            src_states,
            Unmoved::Pages(&self.pages, tuple_size),
            dests,
            &[],
        );
        for page in self.pages.iter() {
            let base = page.base();
            let states_base = unsafe { base.add(src_states.offset) };
            for row in 0..page.rows {
                // SAFETY: `row` is a row of the page, the next row the mover owns.
                unsafe {
                    let src = base.add(row * tuple_size);
                    let hash = std::ptr::read_unaligned(src.add(hash_offset).cast::<u64>());
                    let bucket = ((hash & mask) >> shift) as usize;
                    let states = states_base.add(row * src_states.size);
                    mover.move_row(bucket, src, states, arena_offsets);
                }
            }
        }
    }

    /// Move `rows`, rows of this payload, with their states, to the end of `dest`, a payload of
    /// the layout of this one, copying their group values at `arena_offsets` into the arena of
    /// `dest`; then drop `dropped`, the states of the other rows. This payload is left owning no
    /// states.
    pub(super) fn move_rows_to(
        &mut self,
        rows: &[RowPtr],
        dropped: &[StateAddr],
        dest: &mut Payload,
        arena_offsets: &[usize],
    ) {
        let dests = std::slice::from_mut(dest);
        self.assert_same_layout(dests);
        let states = self.row_layout.states;
        let mut mover = RowMover::new(
            &self.aggrs,
            self.row_layout.states_layout.take(),
            states,
            Unmoved::Rows(rows),
            dests,
            dropped,
        );
        for row in rows {
            // SAFETY: `row` is a row of this payload, the next row the mover owns.
            unsafe {
                let src = row.as_ptr();
                mover.move_row(0, src, states.ptr(src), arena_offsets);
            }
        }
    }

    /// Rows move only between payloads of the same layout: a row of another layout would be
    /// written past the rows or states of its destination, or its states dropped by another
    /// aggregate.
    fn assert_same_layout(&self, dests: &[Payload]) {
        assert!(
            dests.iter().all(|dest| dest.same_layout(self)),
            "rows moved to a payload of another layout"
        );
    }

    /// Calls `f` with the group hash of each row.
    pub(super) fn for_each_hash(&self, mut f: impl FnMut(u64)) {
        for page in &self.pages {
            for row in 0..page.rows {
                f(self.data_ptr(page, row).hash(&self.row_layout));
            }
        }
    }

    /// The rows still free in the writable page, without opening a page.
    pub(super) fn current_cursor(
        &mut self,
        ids: &mut PageNumbering,
        partition: usize,
    ) -> RowCursor {
        match self.current_write_page.checked_sub(1) {
            Some(page) if self.pages[page].rows < self.row_per_page => {
                self.cursor_at(ids, partition, page)
            }
            _ => RowCursor::EMPTY,
        }
    }

    /// The rows of the writable page, opening one if the current one is full.
    pub(super) fn open_cursor(&mut self, ids: &mut PageNumbering, partition: usize) -> RowCursor {
        let (_, page) = self.writable_page();
        self.cursor_at(ids, partition, page)
    }

    fn cursor_at(
        &mut self,
        ids: &mut PageNumbering,
        partition: usize,
        page_index: usize,
    ) -> RowCursor {
        let page_id = ids.number_pages(partition, &self.pages, page_index);
        let page = &self.pages[page_index];
        RowCursor {
            id: page_id | page.rows as u64,
            end: page_id | self.row_per_page as u64,
            page: page_index,
        }
    }

    /// Count the rows reserved through `cursor` in its page.
    pub(super) fn commit_cursor(&mut self, cursor: &RowCursor) {
        if cursor.page == usize::MAX {
            return;
        }
        let page = &mut self.pages[cursor.page];
        let rows = self.row_per_page - (cursor.end - cursor.id) as usize;
        debug_assert!(rows >= page.rows);
        self.total_rows += rows - page.rows;
        page.rows = rows;
    }

    /// Write the groups and the states of the rows of `select_vector`, reserved at `address`.
    pub(super) fn write_rows(
        &mut self,
        select_vector: &[RowID],
        group_hashes: &[u64; BATCH_SIZE],
        address: &mut [RowPtr; BATCH_SIZE],
        page_index: &mut [usize],
        groups: GroupRows,
        source_states: Option<&[StateAddr; BATCH_SIZE]>,
    ) {
        let address = unsafe {
            // SAFETY: RowPtr and RowMut are repr(transparent) wrappers over raw
            // u8 pointers. Raw pointer mutability does not change layout, and
            // row_ptr.rs has compile-time size/alignment assertions for these
            // two wrappers.
            std::mem::transmute::<&mut [RowPtr; BATCH_SIZE], &mut [RowMut; BATCH_SIZE]>(address)
        };
        match groups {
            GroupRows::Columns(group_columns) => {
                self.append_rows(select_vector, group_hashes, address, group_columns)
            }
            GroupRows::Rows(rows) => self.append_copied_rows(select_vector, rows, address),
        }
        self.append_states(select_vector, address, page_index, source_states);
    }

    fn append_copied_rows(
        &mut self,
        select_vector: &[RowID],
        rows: &[RowPtr; BATCH_SIZE],
        address: &mut [RowMut; BATCH_SIZE],
    ) {
        for row in select_vector {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    rows[*row].as_ptr(),
                    address[*row].as_mut_ptr(),
                    self.tuple_size,
                )
            };
        }

        for (idx, data_type) in self.group_types.iter().enumerate() {
            if visit_row_type(&data_type.remove_nullable(), IsInArena) {
                let offset = self.row_layout.group_offsets[idx];
                for row in select_vector {
                    unsafe {
                        let data = self.arena.alloc_slice_copy(rows[*row].read_bytes(offset));
                        address[*row].write_bytes(offset, data);
                    }
                }
            }
        }
    }

    fn append_rows(
        &mut self,
        select_vector: &[RowID],
        group_hashes: &[u64; BATCH_SIZE],
        address: &mut [RowMut; BATCH_SIZE],
        group_columns: ProjectedBlock,
    ) {
        let mut write_offset = 0;
        // write validity
        for entry in group_columns.iter() {
            match entry {
                BlockEntry::Const(scalar, DataType::Nullable(_), _) => {
                    let val = if scalar.is_null() { 0 } else { 1 };
                    for row in select_vector {
                        unsafe {
                            address[*row].write_u8(write_offset, val);
                        }
                    }
                    write_offset += 1;
                }
                BlockEntry::Column(Column::Nullable(deref!(c))) => {
                    let bitmap = c.validity();
                    if bitmap.null_count() == 0 || bitmap.null_count() == bitmap.len() {
                        let val: u8 = if bitmap.null_count() == 0 { 1 } else { 0 };
                        // faster path
                        for row in select_vector {
                            unsafe {
                                address[*row].write_u8(write_offset, val);
                            }
                        }
                    } else {
                        for row in select_vector {
                            unsafe {
                                address[*row]
                                    .write_u8(write_offset, bitmap.get_bit(row.to_usize()) as u8);
                            }
                        }
                    }
                    write_offset += 1;
                }
                _ => (),
            }
        }

        let mut scratch = vec![];
        for (idx, entry) in group_columns.iter().enumerate() {
            let offset = self.row_layout.group_offsets[idx];
            assert!(write_offset == offset);

            match entry {
                BlockEntry::Const(scalar, data_type, _) => unsafe {
                    serialize_const_column_to_rowformat(
                        &self.arena,
                        scalar,
                        data_type,
                        select_vector,
                        address,
                        offset,
                        &mut scratch,
                    )
                },
                BlockEntry::Column(column) => unsafe {
                    serialize_column_to_rowformat(
                        &self.arena,
                        column,
                        select_vector,
                        address,
                        offset,
                        &mut scratch,
                    );
                },
            }

            write_offset += self.row_layout.group_sizes[idx];
        }

        // write group hashes
        debug_assert!(write_offset == self.row_layout.hash_offset);
        for row in select_vector {
            address[*row].set_hash(&self.row_layout, group_hashes[*row]);
        }
    }

    fn append_states(
        &mut self,
        select_vector: &[RowID],
        address: &mut [RowMut; BATCH_SIZE],
        page_index: &mut [usize],
        source_states: Option<&[StateAddr; BATCH_SIZE]>,
    ) {
        if let Some(states_layout) = self.row_layout.states_layout.as_ref() {
            let state_size = states_layout.layout.size();
            // write states, in the states region of the page
            for row in select_vector {
                let place = StateAddr::from(
                    self.row_layout
                        .states
                        .ptr(address[*row].as_mut_ptr().cast_const()),
                );
                let page = &mut self.pages[page_index[*row]];
                // Copying a state that needs no manual drop equals initializing it and merging
                // the source state into it. The caller merges the other states.
                if let Some(source_states) = source_states {
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            source_states[*row].as_ptr(),
                            place.as_ptr(),
                            state_size,
                        )
                    };
                }
                for (aggr, loc) in self.aggrs.iter().zip(states_layout.states_loc.iter()) {
                    if source_states.is_none() || aggr.state().need_manual_drop() {
                        aggr.init_state(AggrState::new(place, loc));
                    }
                    page.initialized_states += 1;
                }
            }

            #[cfg(debug_assertions)]
            {
                for page in self.pages.iter() {
                    assert_eq!(page.rows * self.aggrs.len(), page.initialized_states);
                }
            }
        }

        debug_assert_eq!(
            self.total_rows,
            self.pages.iter().map(|x| x.rows).sum::<usize>()
        );
    }

    /// Move the pages into payloads of at most `max_rows` rows and `max_bytes` bytes each,
    /// without copying rows; a page over a limit makes a payload of its own. The payloads share
    /// the arena of the states, which is freed with the last of them.
    pub fn split_pages(mut self, max_rows: usize, max_bytes: usize) -> Vec<Payload> {
        if self.total_rows <= max_rows && self.memory_size() <= max_bytes {
            return vec![self];
        }

        let mut payloads = vec![];
        let mut current = self.empty_like();
        for page in std::mem::take(&mut self.pages) {
            if page.rows == 0 {
                continue;
            }
            let rows = current.total_rows + page.rows;
            let row_bytes = self.tuple_size + self.row_layout.states.size;
            if current.total_rows != 0 && (rows > max_rows || rows * row_bytes > max_bytes) {
                payloads.push(std::mem::replace(&mut current, self.empty_like()));
            }
            current.total_rows += page.rows;
            current.pages.push(page);
            current.current_write_page = current.pages.len();
        }
        if current.total_rows != 0 {
            payloads.push(current);
        }
        self.total_rows = 0;
        self.current_write_page = 0;
        payloads
    }

    pub(super) fn empty_like(&self) -> Payload {
        let mut payload = Payload::new(
            self.arena.clone(),
            self.group_types.clone(),
            self.aggrs.clone(),
            self.row_layout.states_layout.clone(),
            self.max_page_size,
        );
        payload.routed_partitions = self.routed_partitions;
        payload
    }

    /// The number of final aggregates the rows of this payload were routed among when it is a
    /// partition of a routed table, else 0.
    pub fn routed_partitions(&self) -> usize {
        self.routed_partitions
    }

    /// Append copies of the rows of `select_vector` in `row_refs`, with their states. The states
    /// are not counted as initialized in the pages: see `scatter_into_buckets`.
    fn copy_rows_with_states(&mut self, select_vector: &[RowID], row_refs: &[RowPtr; BATCH_SIZE]) {
        let tuple_size = self.tuple_size;
        let states = self.row_layout.states;
        let row_per_page = self.row_per_page;
        let (mut page, _) = self.writable_page();

        for index in select_vector {
            debug_assert!(page.rows < row_per_page);
            let mut row_mut = page.reserve_row(tuple_size);
            unsafe {
                let source = row_refs[*index].as_ptr();
                std::ptr::copy_nonoverlapping(source, row_mut.as_mut_ptr(), tuple_size);
                std::ptr::copy_nonoverlapping(
                    states.ptr(source),
                    states.ptr(row_mut.as_mut_ptr().cast_const()),
                    states.size,
                );
            }

            if page.rows == row_per_page {
                (page, _) = self.writable_page();
            }
        }

        self.total_rows += select_vector.len();

        debug_assert_eq!(
            self.total_rows,
            self.pages.iter().map(|x| x.rows).sum::<usize>()
        );
    }

    /// Scan the next rows into `flush_state` and select them by partition, of `partition_count`,
    /// in its probe state. Returns whether there were rows left.
    fn scan_hash_partitions(
        &self,
        flush_state: &mut PayloadFlushState,
        partition_count: usize,
    ) -> bool {
        debug_assert!(partition_count > 0);

        loop {
            let Some(page) = self.pages.get(flush_state.flush_page) else {
                return false;
            };

            if flush_state.flush_page_row >= page.rows {
                flush_state.flush_page += 1;
                flush_state.flush_page_row = 0;
                flush_state.row_count = 0;
                continue;
            }

            let end = (flush_state.flush_page_row + BATCH_SIZE).min(page.rows);
            let rows = end - flush_state.flush_page_row;
            flush_state.row_count = rows;

            {
                let state = &mut *flush_state.probe_state;
                state.reset_partitions(partition_count);

                for (idx, row_ptr) in flush_state.addresses[..rows].iter_mut().enumerate() {
                    *row_ptr = self.data_ptr(page, idx + flush_state.flush_page_row);
                    let hash = row_ptr.hash(&self.row_layout);
                    let partition_idx =
                        group_hash_route_index(hash, partition_count as u64) as usize;

                    let (count, sel) = &mut state.partition_entries[partition_idx];
                    sel[*count as usize] = idx.into();
                    *count += 1;
                }
            }

            flush_state.flush_page_row = end;
            return true;
        }
    }

    /// Split the rows into `buckets` payloads by `group_hash_route_index` of their group hash, the
    /// routing of raw rows with the same group keys.
    pub fn scatter_into_buckets(mut self, buckets: usize) -> Vec<Payload> {
        if buckets == 0 {
            return vec![];
        }

        let mut bucket_payloads = (0..buckets)
            .map(|_| {
                // Copying a row locates its states by the layout of the target.
                let mut payload = self.empty_like();
                payload.routed_partitions = 0;
                payload
            })
            .collect::<Vec<_>>();
        if self.len() == 0 {
            return bucket_payloads;
        }

        let mut flush_state = PayloadFlushState::default();
        while self.scan_hash_partitions(&mut flush_state, buckets) {
            let partitions = &flush_state.probe_state.partition_entries;
            for (payload, (count, sel)) in bucket_payloads.iter_mut().zip(partitions) {
                if *count > 0 {
                    payload.copy_rows_with_states(&sel[..*count as usize], &flush_state.addresses);
                }
            }
        }

        // The states were copied with the rows, so they move to the bucket payloads, which only
        // hold copied rows. The source gives up dropping them before the buckets count them as
        // initialized: a panic in between drops no state twice.
        self.row_layout.states_layout = None;
        let agg_len = self.aggrs.len();
        for payload in &mut bucket_payloads {
            for page in &mut payload.pages {
                page.initialized_states = page.rows * agg_len;
            }
        }
        bucket_payloads
    }

    /// A block of no rows with the columns of `aggregate_flush`.
    pub(super) fn empty_block(&self) -> DataBlock {
        assert_eq!(
            self.aggrs.is_empty(),
            self.row_layout.states_layout.is_none()
        );
        let entries = self
            .row_layout
            .states_layout
            .as_ref()
            .iter()
            .flat_map(|layout| layout.serialize_type.iter())
            .map(|serde_type| {
                ColumnBuilder::with_capacity(&serde_type.data_type(), 0)
                    .build()
                    .into()
            })
            .chain(
                self.group_types
                    .iter()
                    .map(|t| ColumnBuilder::with_capacity(t, 0).build().into()),
            )
            .collect();
        DataBlock::new(entries, 0)
    }
}

/// Copy `len` bytes, with fixed-size moves for the sizes of common rows and states.
#[inline(always)]
unsafe fn copy_small(src: *const u8, dst: *mut u8, len: usize) {
    unsafe {
        match len {
            0 => {}
            8 => std::ptr::copy_nonoverlapping(src, dst, 8),
            16 => std::ptr::copy_nonoverlapping(src, dst, 16),
            24 => std::ptr::copy_nonoverlapping(src, dst, 24),
            32 => std::ptr::copy_nonoverlapping(src, dst, 32),
            _ => std::ptr::copy_nonoverlapping(src, dst, len),
        }
    }
}

/// Finalize the states of the rows a flush left in `flush_state` into its aggregate results.
pub(super) fn merge_flushed_states(
    row_layout: &RowLayout,
    aggrs: &[AggregateCallRef],
    flush_state: &mut PayloadFlushState,
) -> Result<()> {
    let row_count = flush_state.row_count;
    flush_state.aggregate_results.clear();
    if let Some(states_layout) = row_layout.states_layout.as_ref() {
        for (aggr, loc) in aggrs.iter().zip(states_layout.states_loc.iter().cloned()) {
            let return_type = aggr.signature().return_type.clone();
            // The capacity is in rows: one result per row.
            let mut builder = ColumnBuilder::with_capacity(&return_type, row_count);
            for place in &flush_state.state_places.as_slice()[0..row_count] {
                aggr.merge_result(AggrState::new(*place, &loc), &mut builder)?;
            }
            flush_state.aggregate_results.push(builder.build().into());
        }
    }
    Ok(())
}

impl Drop for Payload {
    fn drop(&mut self) {
        drop_guard(move || {
            let Some(states_layout) = self.row_layout.states_layout.as_ref() else {
                return;
            };

            'FOR: for (idx, (aggr, loc)) in self
                .aggrs
                .iter()
                .zip(states_layout.states_loc.iter())
                .enumerate()
            {
                if !aggr.state().need_manual_drop() {
                    continue;
                }

                for page in self.pages.iter() {
                    let is_partial_state = page.is_partial_state(self.aggrs.len());

                    if is_partial_state && idx == 0 {
                        info!(
                            "Cleaning partial page, state_offsets: {}, row: {}, agg length: {}",
                            page.initialized_states,
                            page.rows,
                            self.aggrs.len()
                        );
                    }
                    for row in 0..page.initialized_states.div_ceil(self.aggrs.len()) {
                        // When OOM, some states are not initialized, we don't need to destroy them
                        if is_partial_state
                            && row * self.aggrs.len() + idx >= page.initialized_states
                        {
                            continue 'FOR;
                        }
                        let addr = self.data_ptr(page, row).state_addr(&self.row_layout);
                        unsafe {
                            aggr.drop_state(AggrState::new(addr, loc));
                        }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::Layout;
    use std::cell::Cell;
    use std::fmt;
    use std::sync::LazyLock;

    use databend_common_base::runtime::catch_unwind;

    use super::*;
    use crate::AggregateHashTable;
    use crate::BlockEntry;
    use crate::ColumnBuilder;
    use crate::FromData;
    use crate::HashTableConfig;
    use crate::ProbeState;
    use crate::StateSerdeItem;
    use crate::aggregate::AggrStateType;
    use crate::aggregate::AggregateCall;
    use crate::aggregate::MAX_PAGE_SIZE;
    use crate::aggregate::aggregate_function::get_states_layout;
    use crate::aggregate::aggregate_function::*;
    use crate::types::ArgType;
    use crate::types::DataType;
    use crate::types::StringType;
    use crate::types::UInt64Type;

    thread_local! {
        /// The states of `LiveAggregate` alive on this thread.
        static LIVE: Cell<isize> = const { Cell::new(0) };
    }

    fn live() -> isize {
        LIVE.with(Cell::get)
    }

    /// A state on the heap that counts itself in `LIVE`: a state dropped twice leaves the count
    /// below zero, a state never dropped above.
    struct LiveState(#[allow(dead_code)] Box<u64>);

    impl Drop for LiveState {
        fn drop(&mut self) {
            LIVE.with(|live| live.set(live.get() - 1));
        }
    }

    /// An aggregate of no arguments whose states need a drop, see `LiveState`; or, without
    /// `manual_drop`, the same call with states of the same size that are said not to.
    struct LiveAggregate {
        manual_drop: bool,
    }

    impl fmt::Display for LiveAggregate {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "live")
        }
    }

    impl AggregateCall for LiveAggregate {
        fn signature(&self) -> &AggregateSignature {
            static SIGNATURE: LazyLock<AggregateSignature> = LazyLock::new(|| AggregateSignature {
                name: "live".to_string(),
                params: vec![],
                args_type: vec![],
                distinct: false,
                order_by: vec![],
                return_type: UInt64Type::data_type(),
            });
            &SIGNATURE
        }

        fn features(&self) -> &AggregateFeatures {
            static FEATURES: AggregateFeatures = AggregateFeatures {
                eager_aggregation: EagerAggregation::Unsupported,
                supports_filter: false,
                supports_state: false,
                sort_policy: SortPolicy::Unsupported,
                distinct_policy: DistinctPolicy::Unsupported,
                hide_doc: false,
                category: "",
                description: "",
                definition: "",
                example: "",
            };
            &FEATURES
        }

        fn input_layout(&self) -> &FunctionInputLayout {
            &FunctionInputLayout::Identity
        }

        fn state(&self) -> &AggregateStateDescription {
            fn state(manual_drop: bool) -> AggregateStateDescription {
                AggregateStateDescription::new(
                    vec![AggrStateType::Custom(Layout::new::<LiveState>())],
                    vec![StateSerdeItem::DataType(UInt64Type::data_type())],
                )
                .with_manual_drop(manual_drop)
            }
            static DROPPED: LazyLock<AggregateStateDescription> = LazyLock::new(|| state(true));
            static KEPT: LazyLock<AggregateStateDescription> = LazyLock::new(|| state(false));
            match self.manual_drop {
                true => &DROPPED,
                false => &KEPT,
            }
        }

        fn init_state(&self, state: AggrState<'_>) {
            LIVE.with(|live| live.set(live.get() + 1));
            state.write(|| LiveState(Box::new(0)));
        }

        fn accumulate(&self, _: AggrState<'_>, _: ProjectedBlock<'_>) -> Result<()> {
            Ok(())
        }

        fn accumulate_keys(&self, _: AggregateStateSet<'_>, _: ProjectedBlock<'_>) -> Result<()> {
            Ok(())
        }

        fn accumulate_row(&self, _: AggrState<'_>, _: ProjectedBlock<'_>, _: usize) -> Result<()> {
            Ok(())
        }

        fn accumulate_row_count(&self, _: AggrState<'_>, _: usize) -> Result<()> {
            Ok(())
        }

        fn serialize(&self, _: AggregateStateSet<'_>, _: &mut [ColumnBuilder]) -> Result<()> {
            unimplemented!()
        }

        fn merge_serialized(&self, _: AggregateStateSet<'_>, _: &BlockEntry) -> Result<()> {
            unimplemented!()
        }

        fn merge_states(&self, _: AggrState<'_>, _: AggrState<'_>) -> Result<()> {
            Ok(())
        }

        fn merge_result(&self, _: AggrState<'_>, _: &mut ColumnBuilder) -> Result<()> {
            unimplemented!()
        }

        fn merge_result_read_only(&self, _: AggrState<'_>, _: &mut ColumnBuilder) -> Result<()> {
            unimplemented!()
        }

        unsafe fn drop_state(&self, state: AggrState<'_>) {
            unsafe { std::ptr::drop_in_place(state.get::<LiveState>()) };
        }
    }

    /// A payload of `rows` groups of string keys, each with a live state.
    fn source(rows: usize) -> Payload {
        let aggrs: Vec<AggregateCallRef> = vec![Arc::new(LiveAggregate { manual_drop: true })];
        let mut table = AggregateHashTable::new(
            vec![DataType::String],
            aggrs,
            HashTableConfig::default().with_initial_radix_bits(0),
            Arc::new(Bump::new()),
        );
        let keys = (0..rows)
            .map(|i| format!("a group key long enough for the arena {i}"))
            .collect::<Vec<_>>();
        let keys: Vec<BlockEntry> = vec![StringType::from_data(keys).into()];
        let params: Vec<ProjectedBlock> = vec![(&[]).into()];
        table
            .add_groups(
                &mut ProbeState::default(),
                (&keys).into(),
                &params,
                (&[]).into(),
                rows,
            )
            .unwrap();
        table.into_payloads().pop().unwrap()
    }

    /// An empty payload of the layout of `source`, whose arena takes at most `limit` bytes.
    fn dest(source: &Payload, limit: Option<usize>) -> Payload {
        let arena = Bump::new();
        arena.set_allocation_limit(limit);
        Payload::new(
            Arc::new(arena),
            source.group_types.clone(),
            source.aggrs.clone(),
            Some(get_states_layout(&source.aggrs).unwrap()),
            MAX_PAGE_SIZE,
        )
    }

    fn rows_of(payload: &Payload) -> Vec<RowPtr> {
        payload
            .pages
            .iter()
            .flat_map(|page| (0..page.rows).map(|row| payload.data_ptr(page, row)))
            .collect()
    }

    // Moving every row: the states are dropped once, by the destinations, also when the arena
    // of a destination runs out midway and the move panics.
    #[test]
    fn test_move_rows_into_drops_each_state_once() {
        for limit in [None, Some(4096)] {
            {
                let mut source = source(2000);
                let offsets = [source.row_layout.group_offsets[0]];
                assert_eq!(live(), 2000);
                let mut dests = [dest(&source, None), dest(&source, limit)];
                let moved =
                    catch_unwind(|| source.move_rows_into(&mut dests, 1 << 47, 47, &offsets));
                let held = dests[0].len() + dests[1].len();
                match limit {
                    None => assert!(moved.is_ok() && held == 2000),
                    Some(_) => assert!(moved.is_err() && 0 < held && held < 2000),
                }
                // The states of the rows not moved were dropped at the panic.
                assert_eq!(live(), held as isize);
            }
            assert_eq!(live(), 0, "limit {limit:?}");
        }
    }

    // Moving some rows and dropping the others: each state is dropped once, also when the move
    // panics midway.
    #[test]
    fn test_move_rows_to_drops_each_state_once() {
        for limit in [None, Some(4096)] {
            {
                let mut source = source(2000);
                let offsets = [source.row_layout.group_offsets[0]];
                let rows = rows_of(&source);
                let kept = rows.iter().step_by(2).copied().collect::<Vec<_>>();
                let dropped = rows
                    .iter()
                    .skip(1)
                    .step_by(2)
                    .map(|row| row.state_addr(&source.row_layout))
                    .collect::<Vec<_>>();
                let mut dest = dest(&source, limit);
                let moved =
                    catch_unwind(|| source.move_rows_to(&kept, &dropped, &mut dest, &offsets));
                match limit {
                    None => assert!(moved.is_ok() && dest.len() == 1000),
                    Some(_) => assert!(moved.is_err() && 0 < dest.len() && dest.len() < 1000),
                }
                assert_eq!(live(), dest.len() as isize);
            }
            assert_eq!(live(), 0, "limit {limit:?}");
        }
    }

    // Rows move only to payloads of the same layout.
    #[test]
    #[should_panic(expected = "rows moved to a payload of another layout")]
    fn test_move_rows_into_another_layout() {
        let mut source = source(10);
        let mut other = Payload::new(
            Arc::new(Bump::new()),
            vec![DataType::String],
            vec![],
            None,
            MAX_PAGE_SIZE,
        );
        source.move_rows_into(std::slice::from_mut(&mut other), 0, 0, &[]);
    }

    // An aggregate of the same call and state size, whose states are not dropped the same way,
    // has another layout.
    #[test]
    #[should_panic(expected = "rows moved to a payload of another layout")]
    fn test_move_rows_into_another_state() {
        let mut source = source(10);
        let aggrs: Vec<AggregateCallRef> = vec![Arc::new(LiveAggregate { manual_drop: false })];
        let mut other = Payload::new(
            Arc::new(Bump::new()),
            source.group_types.clone(),
            aggrs.clone(),
            Some(get_states_layout(&aggrs).unwrap()),
            MAX_PAGE_SIZE,
        );
        assert_eq!(other.tuple_size, source.tuple_size);
        source.move_rows_into(std::slice::from_mut(&mut other), 0, 0, &[]);
    }
}
