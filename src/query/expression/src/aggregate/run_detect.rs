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

//! Runs of equal group keys: consecutive rows whose group keys are all equal.
//!
//! Input sorted by a cluster key, or correlated with it, often comes in runs, and a partial
//! aggregate then needs only the first row of each run to find the group of the others.

use databend_common_column::bitmap::Bitmap;

use crate::BlockEntry;
use crate::Column;
use crate::ProjectedBlock;
use crate::types::DecimalColumn;
use crate::types::NumberColumn;
use crate::types::encoded::NarrowBuffer;

/// Sets `starts[r]` to 1 when the group keys of row `r` may differ from those of row `r - 1`,
/// and to 0 when they are equal; `starts[0]` is 1. Returns false, with `starts` unspecified,
/// when a group key has a type this does not compare.
///
/// A 0 is exact: the keys of the two rows are the same group. A 1 may be a false difference
/// (floats are not compared, nor values that encode differently), which only splits a run.
pub(super) fn mark_run_starts(group_columns: ProjectedBlock, starts: &mut [u8]) -> bool {
    debug_assert!(!starts.is_empty());
    for (i, entry) in group_columns.iter().enumerate() {
        let first = i == 0;
        let marked = match entry {
            // A constant column repeats one value.
            BlockEntry::Const(..) => {
                if first {
                    starts.fill(0);
                }
                true
            }
            BlockEntry::Column(column) => mark_column(column, starts, first),
        };
        if !marked {
            return false;
        }
    }
    starts[0] = 1;
    true
}

/// The number of rows that start a run.
pub(super) fn count_run_starts(starts: &[u8]) -> usize {
    starts.iter().map(|start| *start as u32).sum::<u32>() as usize
}

fn mark_column(column: &Column, starts: &mut [u8], first: bool) -> bool {
    match column {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {
            if first {
                starts.fill(0);
            }
        }
        Column::Number(column) => match column {
            NumberColumn::UInt8(v) => mark_values(v.as_slice(), starts, first),
            NumberColumn::UInt16(v) => mark_values(v.as_slice(), starts, first),
            NumberColumn::UInt32(v) => mark_values(v.as_slice(), starts, first),
            NumberColumn::UInt64(v) => mark_values(v.as_slice(), starts, first),
            NumberColumn::Int8(v) => mark_values(v.as_slice(), starts, first),
            NumberColumn::Int16(v) => mark_values(v.as_slice(), starts, first),
            NumberColumn::Int32(v) => mark_values(v.as_slice(), starts, first),
            NumberColumn::Int64(v) => mark_values(v.as_slice(), starts, first),
            // Equal floats need not be the same group (-0.0 and 0.0, NaNs): left out.
            NumberColumn::Float32(_) | NumberColumn::Float64(_) => return false,
        },
        Column::Decimal(column) => match column {
            DecimalColumn::Decimal64(v, _) => mark_values(v.as_slice(), starts, first),
            DecimalColumn::Decimal128(v, _) => mark_values(v.as_slice(), starts, first),
            DecimalColumn::Decimal256(v, _) => mark_values(v.as_slice(), starts, first),
        },
        Column::Date(v) => mark_values(v.as_slice(), starts, first),
        Column::Timestamp(v) => mark_values(v.as_slice(), starts, first),
        Column::Boolean(v) => mark_bits(v, starts, first),
        Column::String(v) => mark_rows(starts, first, |row| {
            // SAFETY: `row` is a row of the batch, and `starts` has a byte per row.
            unsafe { v.index_unchecked(row) != v.index_unchecked(row - 1) }
        }),
        Column::Binary(v) => mark_rows(starts, first, |row| {
            // SAFETY: as above.
            unsafe { v.index_unchecked(row) != v.index_unchecked(row - 1) }
        }),
        // Equal codes are equal values; different codes may still be equal values.
        Column::Encoded(column) => mark_codes(column.codes().0, starts, first),
        Column::Nullable(column) => {
            // Rows of different validity differ, and the values under a null mark at most a
            // false difference between two nulls.
            if !mark_column(column.column(), starts, first) {
                return false;
            }
            mark_bits(column.validity(), starts, false);
        }
        _ => return false,
    }
    true
}

fn mark_codes(codes: &NarrowBuffer, starts: &mut [u8], first: bool) {
    match codes {
        NarrowBuffer::U8(v) => mark_values(v.as_slice(), starts, first),
        NarrowBuffer::U16(v) => mark_values(v.as_slice(), starts, first),
        NarrowBuffer::U32(v) => mark_values(v.as_slice(), starts, first),
        NarrowBuffer::U64(v) => mark_values(v.as_slice(), starts, first),
    }
}

/// The compares of fixed-width values, written over zipped slices without bounds checks so that
/// they vectorize.
#[inline(always)]
fn mark_values<T: PartialEq + Copy>(values: &[T], starts: &mut [u8], first: bool) {
    let rows = starts.len();
    let (previous, current) = (&values[..rows - 1], &values[1..rows]);
    let pairs = starts[1..].iter_mut().zip(current.iter().zip(previous));
    if first {
        for (start, (value, before)) in pairs {
            *start = (value != before) as u8;
        }
    } else {
        for (start, (value, before)) in pairs {
            *start |= (value != before) as u8;
        }
    }
}

fn mark_bits(bits: &Bitmap, starts: &mut [u8], first: bool) {
    mark_rows(starts, first, |row| {
        // SAFETY: `row` is a row of the batch, and `starts` has a byte per row.
        unsafe { bits.get_bit_unchecked(row) != bits.get_bit_unchecked(row - 1) }
    })
}

#[inline(always)]
fn mark_rows(starts: &mut [u8], first: bool, differs: impl Fn(usize) -> bool) {
    for (row, start) in starts.iter_mut().enumerate().skip(1) {
        let differs = differs(row) as u8;
        *start = if first { differs } else { *start | differs };
    }
}
