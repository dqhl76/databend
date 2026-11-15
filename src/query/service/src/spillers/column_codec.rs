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

//! A typed encoding of a block of spilled rows, whose types the reader knows.
//!
//! Spilled aggregate states and group keys are mostly integers in a small range per block
//! (keys, dates, decimals, counts), flags, and short strings. Each column is encoded by its
//! type, without a schema or per-page metadata:
//!
//! - integers, dates, timestamps and 64-bit decimals: frame of reference and bit packing per
//!   [`CHUNK`] values;
//! - validity and booleans: one byte when all set, else a bitmap;
//! - variable-size values: their lengths, bit packed, and their bytes appended to one area
//!   of the block that is LZ4 compressed at the end;
//! - nested types: their parts, recursively;
//! - floats and wide decimals: their bytes as they are; the few remaining types: the generic
//!   column serialization.
//!
//! Encoding and decoding cost little more than a pass over the values, and the result is
//! smaller than Parquet with LZ4 pages, as the bytes of fixed-size values are packed by
//! their actual range instead of compressed by a general-purpose codec.

use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::BinaryColumn;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::i256;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::utils::arrow::deserialize_column;
use databend_common_expression::utils::arrow::serialize_column;

/// Values packed with one frame of reference and bit width.
const CHUNK: usize = 1024;

/// Encodes `blocks`, which share their columns, as one block of all their rows into `out`: the
/// row count, the variable-size area, then the columns. The blocks are not concatenated first:
/// each column is encoded from its parts in the blocks.
pub fn encode_blocks(blocks: &[DataBlock], out: &mut Vec<u8>) -> Result<()> {
    let num_rows = blocks.iter().map(DataBlock::num_rows).sum::<usize>();
    let num_columns = blocks.first().map_or(0, DataBlock::num_columns);
    let mut columns =
        Vec::with_capacity(blocks.iter().map(DataBlock::memory_size).sum::<usize>() / 4);
    let mut encoder = Encoder {
        out: &mut columns,
        var: vec![],
        values: vec![],
    };
    for index in 0..num_columns {
        let parts = blocks
            .iter()
            .map(|block| match block.get_by_offset(index) {
                BlockEntry::Column(Column::Encoded(encoded)) => encoded.decode(),
                entry => entry.to_column(),
            })
            .collect::<Vec<_>>();
        encoder.column(&parts)?;
    }
    let compressed = lz4_flex::block::compress(&encoder.var);
    put_u64(out, num_rows as u64);
    put_u64(out, encoder.var.len() as u64);
    put_u64(out, compressed.len() as u64);
    out.extend_from_slice(&compressed);
    out.extend_from_slice(&columns);
    Ok(())
}

/// Decodes a block encoded by [`encode_blocks`] with columns of `types`.
pub fn decode_block(bytes: &[u8], types: &[DataType]) -> Result<DataBlock> {
    let mut reader = Reader { bytes, pos: 0 };
    let num_rows = reader.u64()? as usize;
    let raw_len = reader.u64()? as usize;
    let compressed_len = reader.u64()? as usize;
    let var = lz4_flex::block::decompress(reader.take(compressed_len)?, raw_len)
        .map_err(|err| ErrorCode::Internal(format!("corrupted spilled block: {err}")))?;

    let mut decoder = Decoder {
        reader: &mut reader,
        var: &var,
        var_pos: 0,
        values: vec![],
    };
    let mut columns = Vec::with_capacity(types.len());
    for data_type in types {
        columns.push(decoder.column(data_type, num_rows)?);
    }
    Ok(DataBlock::new_from_columns(columns))
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

/// Maps signed integers to unsigned ones of the same order, so that the frame of reference of
/// a chunk is its minimum.
#[inline(always)]
fn order_preserving(value: i64) -> u64 {
    (value as u64) ^ (1 << 63)
}

#[inline(always)]
fn from_order_preserving(value: u64) -> i64 {
    (value ^ (1 << 63)) as i64
}

/// Packs `values` by chunks of [`CHUNK`]: the minimum, the bit width, then the differences to
/// the minimum, followed by 8 bytes of padding so that a reader can load whole words.
fn pack(values: &[u64], out: &mut Vec<u8>) {
    for chunk in values.chunks(CHUNK) {
        let (mut min, mut max) = (u64::MAX, 0u64);
        for &value in chunk {
            min = min.min(value);
            max = max.max(value);
        }
        let bits = 64 - (max - min).leading_zeros() as usize;
        put_u64(out, min);
        out.push(bits as u8);
        if bits == 0 {
            continue;
        }
        let start = out.len();
        let len = (bits * chunk.len()).div_ceil(8);
        out.resize(start + len + 8, 0);
        let mut acc: u64 = 0;
        let mut acc_bits = 0usize;
        let mut pos = start;
        for &value in chunk {
            let delta = value - min;
            acc |= delta << acc_bits;
            let total = acc_bits + bits;
            if total >= 64 {
                out[pos..pos + 8].copy_from_slice(&acc.to_le_bytes());
                pos += 8;
                acc = if acc_bits == 0 {
                    0
                } else {
                    delta >> (64 - acc_bits)
                };
                acc_bits = total - 64;
            } else {
                acc_bits = total;
            }
        }
        if acc_bits > 0 {
            out[pos..pos + 8].copy_from_slice(&acc.to_le_bytes());
        }
    }
}

struct Encoder<'a> {
    out: &'a mut Vec<u8>,
    var: Vec<u8>,
    /// Scratch for the values of a column.
    values: Vec<u64>,
}

/// The parts of a nested column, one per part of the column.
fn parts_of<'a>(parts: &'a [Column], part: impl Fn(&'a Column) -> &'a Column) -> Vec<Column> {
    parts.iter().map(|column| part(column).clone()).collect()
}

/// Encodes the columns of the blocks of a segment: each column is given as its parts, one per
/// block, which have the same type and are encoded as one column of all their rows.
impl Encoder<'_> {
    fn integers<'a, T: Copy + 'a>(
        &mut self,
        parts: impl Iterator<Item = &'a [T]>,
        to_u64: impl Fn(T) -> u64,
    ) {
        self.values.clear();
        for values in parts {
            self.values
                .extend(values.iter().map(|value| to_u64(*value)));
        }
        pack(&self.values, self.out);
    }

    fn raw<'a, T: Copy + 'a>(&mut self, parts: impl Iterator<Item = &'a [T]>) {
        for values in parts {
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    values.as_ptr() as *const u8,
                    std::mem::size_of_val(values),
                )
            };
            self.out.extend_from_slice(bytes);
        }
    }

    fn bitmap<'a>(&mut self, parts: impl Iterator<Item = &'a Bitmap> + Clone) {
        if parts.clone().all(|bitmap| bitmap.null_count() == 0) {
            self.out.push(1);
            return;
        }
        self.out.push(0);
        let len = parts.clone().map(Bitmap::len).sum::<usize>();
        let start = self.out.len();
        self.out.resize(start + len.div_ceil(8), 0);
        let packed = &mut self.out[start..];
        let mut i = 0;
        for bitmap in parts {
            for set in bitmap.iter() {
                packed[i / 8] |= (set as u8) << (i % 8);
                i += 1;
            }
        }
    }

    fn lengths(&mut self, lengths: impl Iterator<Item = usize>) {
        self.values.clear();
        self.values.extend(lengths.map(|len| len as u64));
        pack(&self.values, self.out);
    }

    fn binary<'a>(&mut self, parts: impl Iterator<Item = &'a BinaryColumn> + Clone) {
        self.lengths(
            parts
                .clone()
                .flat_map(|column| column.iter().map(|value| value.len())),
        );
        for column in parts {
            let offsets = column.offsets();
            let (start, end) = (offsets[0] as usize, offsets[column.len()] as usize);
            self.var.extend_from_slice(&column.data()[start..end]);
        }
    }

    fn column(&mut self, parts: &[Column]) -> Result<()> {
        let Some(first) = parts.first() else {
            return Ok(());
        };
        macro_rules! parts {
            ($pattern:pat => $value:expr) => {
                parts.iter().map(|column| match column {
                    $pattern => $value,
                    _ => unreachable!("the parts of a spilled column differ in type"),
                })
            };
        }
        if parts.iter().any(
            |column| matches!(column, Column::Decimal(decimal) if !decimal.is_strict_decimal()),
        ) {
            // The reader decodes a decimal by the width of its precision: encode it with that
            // width, whatever width it is carried with.
            let strict = parts!(Column::Decimal(decimal) => Column::Decimal(decimal.clone().strict_decimal()))
                .collect::<Vec<_>>();
            return self.column(&strict);
        }
        match first {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {}
            Column::Number(number) => match number {
                NumberColumn::UInt8(_) => self.integers(
                    parts!(Column::Number(NumberColumn::UInt8(v)) => v.as_slice()),
                    |x| x as u64,
                ),
                NumberColumn::UInt16(_) => self.integers(
                    parts!(Column::Number(NumberColumn::UInt16(v)) => v.as_slice()),
                    |x| x as u64,
                ),
                NumberColumn::UInt32(_) => self.integers(
                    parts!(Column::Number(NumberColumn::UInt32(v)) => v.as_slice()),
                    |x| x as u64,
                ),
                NumberColumn::UInt64(_) => self.integers(
                    parts!(Column::Number(NumberColumn::UInt64(v)) => v.as_slice()),
                    |x| x,
                ),
                NumberColumn::Int8(_) => self.integers(
                    parts!(Column::Number(NumberColumn::Int8(v)) => v.as_slice()),
                    |x| order_preserving(x as i64),
                ),
                NumberColumn::Int16(_) => self.integers(
                    parts!(Column::Number(NumberColumn::Int16(v)) => v.as_slice()),
                    |x| order_preserving(x as i64),
                ),
                NumberColumn::Int32(_) => self.integers(
                    parts!(Column::Number(NumberColumn::Int32(v)) => v.as_slice()),
                    |x| order_preserving(x as i64),
                ),
                NumberColumn::Int64(_) => self.integers(
                    parts!(Column::Number(NumberColumn::Int64(v)) => v.as_slice()),
                    order_preserving,
                ),
                NumberColumn::Float32(_) => {
                    self.raw(parts!(Column::Number(NumberColumn::Float32(v)) => v.as_slice()))
                }
                NumberColumn::Float64(_) => {
                    self.raw(parts!(Column::Number(NumberColumn::Float64(v)) => v.as_slice()))
                }
            },
            Column::Decimal(decimal) => match decimal {
                DecimalColumn::Decimal64(..) => self.integers(
                    parts!(Column::Decimal(DecimalColumn::Decimal64(v, _)) => v.as_slice()),
                    order_preserving,
                ),
                DecimalColumn::Decimal128(..) => self
                    .raw(parts!(Column::Decimal(DecimalColumn::Decimal128(v, _)) => v.as_slice())),
                DecimalColumn::Decimal256(..) => self
                    .raw(parts!(Column::Decimal(DecimalColumn::Decimal256(v, _)) => v.as_slice())),
            },
            Column::Date(_) => self.integers(parts!(Column::Date(v) => v.as_slice()), |x| {
                order_preserving(x as i64)
            }),
            Column::Timestamp(_) => self.integers(
                parts!(Column::Timestamp(v) => v.as_slice()),
                order_preserving,
            ),
            Column::Boolean(_) => self.bitmap(parts!(Column::Boolean(bitmap) => bitmap)),
            Column::String(_) => {
                let strings = parts!(Column::String(column) => column);
                self.lengths(
                    strings
                        .clone()
                        .flat_map(|column| column.len_iter().map(|len| len as usize)),
                );
                for column in strings {
                    for value in column.iter() {
                        self.var.extend_from_slice(value.as_bytes());
                    }
                }
            }
            Column::Binary(_) | Column::Bitmap(_) | Column::Variant(_) | Column::Geometry(_) => {
                self.binary(parts!(
                    Column::Binary(column)
                    | Column::Bitmap(column)
                    | Column::Variant(column)
                    | Column::Geometry(column) => column
                ))
            }
            Column::Nullable(_) => {
                self.bitmap(parts!(Column::Nullable(nullable) => nullable.validity()));
                self.column(&parts_of(parts, |column| match column {
                    Column::Nullable(nullable) => nullable.column(),
                    _ => unreachable!("the parts of a spilled column differ in type"),
                }))?;
            }
            Column::Tuple(fields) => {
                for index in 0..fields.len() {
                    self.column(&parts_of(parts, |column| match column {
                        Column::Tuple(fields) => &fields[index],
                        _ => unreachable!("the parts of a spilled column differ in type"),
                    }))?;
                }
            }
            Column::Array(_) | Column::Map(_) => {
                let arrays = parts!(Column::Array(array) | Column::Map(array) => array);
                self.lengths(
                    arrays.clone().flat_map(|array| {
                        array.offsets().windows(2).map(|w| (w[1] - w[0]) as usize)
                    }),
                );
                let values = arrays
                    .map(|array| {
                        let offsets = array.offsets();
                        let (start, end) =
                            (offsets[0] as usize, offsets[offsets.len() - 1] as usize);
                        array.values().slice(start..end)
                    })
                    .collect::<Vec<_>>();
                self.column(&values)?;
            }
            Column::Encoded(_) => {
                let decoded =
                    parts!(Column::Encoded(encoded) => encoded.decode()).collect::<Vec<_>>();
                self.column(&decoded)?;
            }
            Column::TimestampTz(_)
            | Column::Interval(_)
            | Column::Geography(_)
            | Column::Vector(_)
            | Column::Opaque(_) => {
                let column = Column::concat_columns(parts.to_vec())?;
                let bytes = serialize_column(&column);
                put_u64(self.out, bytes.len() as u64);
                self.out.extend_from_slice(&bytes);
            }
        }
        Ok(())
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.pos + len;
        if end > self.bytes.len() {
            return Err(ErrorCode::Internal("corrupted spilled block: truncated"));
        }
        let bytes = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

struct Decoder<'a, 'b> {
    reader: &'b mut Reader<'a>,
    var: &'b [u8],
    var_pos: usize,
    values: Vec<u64>,
}

impl Decoder<'_, '_> {
    fn unpack(&mut self, n: usize) -> Result<()> {
        self.values.clear();
        self.values.reserve(n);
        let mut remaining = n;
        while remaining > 0 {
            let count = CHUNK.min(remaining);
            let min = self.reader.u64()?;
            let bits = self.reader.u8()? as usize;
            if bits == 0 {
                self.values.extend(std::iter::repeat_n(min, count));
            } else {
                let len = (bits * count).div_ceil(8);
                let data = self.reader.take(len + 8)?;
                {
                    let read =
                        |pos: usize| u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                    if bits <= 56 {
                        let mask = (1u64 << bits) - 1;
                        let mut bit = 0usize;
                        for _ in 0..count {
                            self.values
                                .push(min + ((read(bit / 8) >> (bit % 8)) & mask));
                            bit += bits;
                        }
                    } else {
                        let mask = if bits == 64 {
                            u64::MAX
                        } else {
                            (1u64 << bits) - 1
                        };
                        let mut bit = 0usize;
                        for _ in 0..count {
                            let (word, shift) = (bit / 8, bit % 8);
                            let wide = read(word) as u128 | ((data[word + 8] as u128) << 64);
                            self.values
                                .push(min.wrapping_add(((wide >> shift) as u64) & mask));
                            bit += bits;
                        }
                    }
                }
            }
            remaining -= count;
        }
        Ok(())
    }

    fn integers<T>(&mut self, n: usize, from_u64: impl Fn(u64) -> T) -> Result<Buffer<T>> {
        self.unpack(n)?;
        Ok(self
            .values
            .iter()
            .map(|value| from_u64(*value))
            .collect::<Vec<_>>()
            .into())
    }

    fn raw<T: Copy + Default>(&mut self, n: usize) -> Result<Buffer<T>> {
        let bytes = self.reader.take(n * std::mem::size_of::<T>())?;
        let mut values = vec![T::default(); n];
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                values.as_mut_ptr() as *mut u8,
                bytes.len(),
            );
        }
        Ok(values.into())
    }

    fn bitmap(&mut self, n: usize) -> Result<Bitmap> {
        if self.reader.u8()? == 1 {
            return Ok(Bitmap::new_constant(true, n));
        }
        let bytes = self.reader.take(n.div_ceil(8))?;
        Ok(Bitmap::from_u8_vec(bytes.to_vec(), n))
    }

    fn var_bytes(&mut self, len: usize) -> Result<&[u8]> {
        let end = self.var_pos + len;
        if end > self.var.len() {
            return Err(ErrorCode::Internal(
                "corrupted spilled block: variable-size area",
            ));
        }
        let bytes = &self.var[self.var_pos..end];
        self.var_pos = end;
        Ok(bytes)
    }

    /// The lengths of `n` values, then their offsets.
    fn offsets(&mut self, n: usize) -> Result<Buffer<u64>> {
        self.unpack(n)?;
        let mut offsets = Vec::with_capacity(n + 1);
        let mut offset = 0u64;
        offsets.push(0);
        for len in &self.values {
            offset += len;
            offsets.push(offset);
        }
        Ok(offsets.into())
    }

    fn binary(&mut self, n: usize) -> Result<BinaryColumn> {
        let offsets = self.offsets(n)?;
        let data = self.var_bytes(offsets[n] as usize)?.to_vec();
        Ok(BinaryColumn::new(data.into(), offsets))
    }

    fn column(&mut self, data_type: &DataType, n: usize) -> Result<Column> {
        let column = match data_type {
            DataType::Null => Column::Null { len: n },
            DataType::EmptyArray => Column::EmptyArray { len: n },
            DataType::EmptyMap => Column::EmptyMap { len: n },
            DataType::Number(number) => Column::Number(match number {
                NumberDataType::UInt8 => NumberColumn::UInt8(self.integers(n, |x| x as u8)?),
                NumberDataType::UInt16 => NumberColumn::UInt16(self.integers(n, |x| x as u16)?),
                NumberDataType::UInt32 => NumberColumn::UInt32(self.integers(n, |x| x as u32)?),
                NumberDataType::UInt64 => NumberColumn::UInt64(self.integers(n, |x| x)?),
                NumberDataType::Int8 => {
                    NumberColumn::Int8(self.integers(n, |x| from_order_preserving(x) as i8)?)
                }
                NumberDataType::Int16 => {
                    NumberColumn::Int16(self.integers(n, |x| from_order_preserving(x) as i16)?)
                }
                NumberDataType::Int32 => {
                    NumberColumn::Int32(self.integers(n, |x| from_order_preserving(x) as i32)?)
                }
                NumberDataType::Int64 => {
                    NumberColumn::Int64(self.integers(n, from_order_preserving)?)
                }
                NumberDataType::Float32 => NumberColumn::Float32(self.raw::<F32>(n)?),
                NumberDataType::Float64 => NumberColumn::Float64(self.raw::<F64>(n)?),
            }),
            DataType::Decimal(size) => Column::Decimal(match size.data_kind() {
                DecimalDataKind::Decimal64 => {
                    DecimalColumn::Decimal64(self.integers(n, from_order_preserving)?, *size)
                }
                DecimalDataKind::Decimal128 => {
                    DecimalColumn::Decimal128(self.raw::<i128>(n)?, *size)
                }
                DecimalDataKind::Decimal256 => {
                    DecimalColumn::Decimal256(self.raw::<i256>(n)?, *size)
                }
            }),
            DataType::Date => Column::Date(self.integers(n, |x| from_order_preserving(x) as i32)?),
            DataType::Timestamp => Column::Timestamp(self.integers(n, from_order_preserving)?),
            DataType::Boolean => Column::Boolean(self.bitmap(n)?),
            DataType::String => {
                let offsets = self.offsets(n)?;
                let bytes = self.var_bytes(offsets[n] as usize)?;
                let mut builder = StringColumnBuilder::with_capacity(n);
                for w in offsets.windows(2) {
                    let value = &bytes[w[0] as usize..w[1] as usize];
                    // The bytes were written from a string column.
                    builder.put_and_commit(unsafe { std::str::from_utf8_unchecked(value) });
                }
                Column::String(builder.build())
            }
            DataType::Binary => Column::Binary(self.binary(n)?),
            DataType::Bitmap => Column::Bitmap(self.binary(n)?),
            DataType::Variant => Column::Variant(self.binary(n)?),
            DataType::Geometry => Column::Geometry(self.binary(n)?),
            DataType::Nullable(inner) => {
                let validity = self.bitmap(n)?;
                self.column(inner, n)?.wrap_nullable(Some(validity))
            }
            DataType::Tuple(fields) => {
                let mut columns = Vec::with_capacity(fields.len());
                for field in fields {
                    columns.push(self.column(field, n)?);
                }
                Column::Tuple(columns)
            }
            DataType::Array(inner) | DataType::Map(inner) => {
                let offsets = self.offsets(n)?;
                let values = self.column(inner, offsets[n] as usize)?;
                let array = Box::new(ArrayColumn::new(values, offsets));
                match data_type {
                    DataType::Array(_) => Column::Array(array),
                    _ => Column::Map(array),
                }
            }
            DataType::TimestampTz
            | DataType::Interval
            | DataType::Geography
            | DataType::Vector(_)
            | DataType::Opaque(_) => {
                let len = self.reader.u64()? as usize;
                deserialize_column(self.reader.take(len)?)?
            }
            // A column of states has the layout of their physical type.
            DataType::AggregateState(state) => self.column(state.physical_type(), n)?,
            DataType::Generic(_) | DataType::StageLocation => {
                return Err(ErrorCode::Internal(format!(
                    "cannot decode a spilled column of type {data_type}"
                )));
            }
        };
        Ok(column)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::RandomOptions;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::DecimalDataType;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::NumberDataType;

    use super::decode_block;
    use super::encode_blocks;

    fn types() -> Vec<DataType> {
        let numbers = [
            NumberDataType::UInt8,
            NumberDataType::UInt16,
            NumberDataType::UInt32,
            NumberDataType::UInt64,
            NumberDataType::Int8,
            NumberDataType::Int16,
            NumberDataType::Int32,
            NumberDataType::Int64,
            NumberDataType::Float32,
            NumberDataType::Float64,
        ];
        let mut types: Vec<DataType> = numbers.into_iter().map(DataType::Number).collect();
        for precision in [15, 38, 76] {
            let size = DecimalSize::new_unchecked(precision, 2);
            types.push(DataType::Decimal(DecimalDataType::from(size).size()));
        }
        types.extend([
            DataType::Null,
            DataType::EmptyArray,
            DataType::Boolean,
            DataType::String,
            DataType::Binary,
            DataType::Variant,
            DataType::Date,
            DataType::Timestamp,
            DataType::Interval,
            DataType::Nullable(Box::new(DataType::String)),
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
            DataType::Tuple(vec![
                DataType::Number(NumberDataType::UInt64),
                DataType::Nullable(Box::new(DataType::String)),
            ]),
            DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::String)))),
            DataType::Array(Box::new(DataType::Number(NumberDataType::Int32))),
        ]);
        types
    }

    fn round_trip(columns: Vec<Column>) -> Result<()> {
        let block = DataBlock::new_from_columns(columns);
        let rows = block.num_rows();
        round_trip_blocks(&[block.clone()])?;
        // The same rows in uneven parts are encoded as one block of all of them.
        let cuts = [0, 1, rows / 3, rows / 3, rows / 2 + 7, rows];
        let parts = cuts
            .windows(2)
            .map(|w| block.slice(w[0]..w[1]))
            .collect::<Vec<_>>();
        round_trip_blocks(&parts)
    }

    fn round_trip_blocks(blocks: &[DataBlock]) -> Result<()> {
        let types = blocks[0]
            .columns()
            .iter()
            .map(|entry| entry.data_type())
            .collect::<Vec<_>>();
        let expected = DataBlock::concat(blocks)?;
        let mut bytes = vec![];
        encode_blocks(blocks, &mut bytes)?;
        let decoded = decode_block(&bytes, &types)?;
        assert_eq!(decoded.num_rows(), expected.num_rows());
        for (index, (expected, actual)) in
            expected.columns().iter().zip(decoded.columns()).enumerate()
        {
            assert_eq!(
                expected.to_column(),
                actual.to_column(),
                "column {index} of type {}",
                types[index]
            );
        }
        Ok(())
    }

    // Every type the spilled states and keys can have survives an encoding, over several
    // chunks of packed values.
    #[test]
    fn test_round_trip_every_type() -> Result<()> {
        let options = RandomOptions {
            seed: Some(7),
            min_string_len: 0,
            max_string_len: 40,
            max_array_len: 4,
        };
        let columns = types()
            .iter()
            .map(|data_type| Column::random(data_type, 3000, Some(options.clone())))
            .collect();
        round_trip(columns)
    }

    // Sliced columns start in the middle of their bitmaps, offsets and values.
    #[test]
    fn test_round_trip_sliced_columns() -> Result<()> {
        let options = RandomOptions {
            seed: Some(11),
            min_string_len: 0,
            max_string_len: 20,
            max_array_len: 3,
        };
        let columns = types()
            .iter()
            .map(|data_type| Column::random(data_type, 2500, Some(options.clone())).slice(3..2203))
            .collect();
        round_trip(columns)
    }

    // A decimal carried with a wider width than its precision needs reads back with its own.
    #[test]
    fn test_round_trip_non_strict_decimal() -> Result<()> {
        use databend_common_expression::types::DecimalColumn;
        use databend_common_expression::types::DecimalSize;

        let size = DecimalSize::new_unchecked(10, 2);
        let wide = Column::Decimal(DecimalColumn::Decimal128((-1500..1500).collect(), size));
        let strict = Column::Decimal(DecimalColumn::Decimal64((-1500..1500).collect(), size));
        let mut bytes = vec![];
        encode_blocks(&[DataBlock::new_from_columns(vec![wide])], &mut bytes)?;
        let decoded = decode_block(&bytes, &[DataType::Decimal(size)])?;
        assert_eq!(decoded.get_by_offset(0).to_column(), strict);
        Ok(())
    }

    // Values that need all 64 bits, the extremes of signed integers, and constant chunks.
    #[test]
    fn test_round_trip_extreme_integers() -> Result<()> {
        use databend_common_expression::FromData;
        use databend_common_expression::types::Int64Type;
        use databend_common_expression::types::UInt64Type;

        let signed = Int64Type::from_data(
            (0..3000)
                .map(|i| match i % 3 {
                    0 => i64::MIN,
                    1 => i64::MAX,
                    _ => i as i64 - 1500,
                })
                .collect::<Vec<_>>(),
        );
        let unsigned = UInt64Type::from_data(
            (0..3000u64)
                .map(|i| if i % 2 == 0 { u64::MAX } else { i })
                .collect::<Vec<_>>(),
        );
        let constant = UInt64Type::from_data(vec![42u64; 3000]);
        round_trip(vec![signed, unsigned, constant])
    }
}
