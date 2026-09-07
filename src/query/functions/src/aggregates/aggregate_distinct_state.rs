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

use std::collections::HashSet;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::marker::Send;
use std::sync::Arc;

use borsh::BorshSerialize;
use bumpalo::Bump;
use databend_common_column::binary::BinaryColumnBuilder;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::DataBlock;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::simple_type::SimpleType;
use databend_common_expression::types::simple_type::SimpleValueType;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::*;
use databend_common_hashtable::HashSet as CommonHashSet;
use databend_common_hashtable::HashtableKeyable;
use databend_common_hashtable::HashtableLike;
use databend_common_hashtable::ShortStringHashSet;
use databend_common_hashtable::StackHashSet;
use databend_common_io::prelude::*;
use siphasher::sip128::Hasher128;
use siphasher::sip128::SipHasher24;

use super::SerializeInfo;
use super::StateAddr;
use super::StateSerde;
use super::aggregate_spill::SPILL_BATCH_ROWS;
use super::batch_merge1;
use super::batch_serialize1;
use super::borsh_partial_deserialize;

pub(super) trait DistinctStateFunc: Sized + Send + StateSerde + 'static {
    fn new() -> Self;
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
    fn memory_size(&self) -> usize;
    fn add(&mut self, columns: ProjectedBlock, row: usize) -> Result<()>;
    fn batch_add(
        &mut self,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()>;
    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn build_entries(&mut self, types: &[DataType]) -> Result<Vec<BlockEntry>>;

    fn spill_types(types: &[DataType]) -> Vec<DataType> {
        types.to_vec()
    }

    fn spill_entries(&mut self, types: &[DataType]) -> Result<Vec<BlockEntry>> {
        self.build_entries(types)
    }

    fn merge_spilled(&mut self, columns: ProjectedBlock, rows: usize) -> Result<()> {
        self.batch_add(columns, None, rows)
    }

    fn visit_serialized(
        state: &BlockEntry,
        mut consume: impl FnMut(DataBlock) -> Result<()>,
    ) -> Result<()> {
        let Column::Tuple(fields) = state.to_column() else {
            return Err(ErrorCode::BadBytes("Invalid serialized DISTINCT state"));
        };
        let Some(ScalarRef::Array(values)) = fields[0].index(0) else {
            return Err(ErrorCode::BadBytes(
                "DISTINCT state must contain an array of keys",
            ));
        };
        for start in (0..values.len()).step_by(SPILL_BATCH_ROWS) {
            let end = (start + SPILL_BATCH_ROWS).min(values.len());
            consume(DataBlock::new_from_columns(vec![values.slice(start..end)]))?;
        }
        Ok(())
    }
}

pub trait SimpleAccessType: AccessType {
    type Simple: SimpleType<Scalar = <Self as AccessType>::Scalar>;
}

impl<T: SimpleType> SimpleAccessType for SimpleValueType<T> {
    type Simple = T;
}

pub trait DistinctAdapter: Send + 'static
where <Self::Access as AccessType>::Scalar: Copy + Send + HashtableKeyable
{
    type Access: SimpleAccessType + ArgType;

    fn downcast_column(columns: &ProjectedBlock) -> ColumnView<Self::Access>;
    fn upcast_column(values: Buffer<<Self::Access as AccessType>::Scalar>) -> BlockEntry;
}

pub struct NumberAdapter<T>(PhantomData<T>);

impl<T> DistinctAdapter for NumberAdapter<T>
where T: Number + HashtableKeyable + Copy + Send
{
    type Access = NumberType<T>;

    fn downcast_column(columns: &ProjectedBlock) -> ColumnView<Self::Access> {
        columns[0].downcast::<Self::Access>().unwrap()
    }

    fn upcast_column(values: Buffer<<Self::Access as AccessType>::Scalar>) -> BlockEntry {
        NumberType::<T>::upcast_column(values).into()
    }
}

pub struct TimestampAdapter;

impl DistinctAdapter for TimestampAdapter {
    type Access = TimestampType;

    fn downcast_column(columns: &ProjectedBlock) -> ColumnView<Self::Access> {
        columns[0].downcast::<Self::Access>().unwrap()
    }

    fn upcast_column(values: Buffer<<Self::Access as AccessType>::Scalar>) -> BlockEntry {
        TimestampType::upcast_column(values).into()
    }
}

pub struct DateAdapter;

impl DistinctAdapter for DateAdapter {
    type Access = DateType;

    fn downcast_column(columns: &ProjectedBlock) -> ColumnView<Self::Access> {
        columns[0].downcast::<Self::Access>().unwrap()
    }

    fn upcast_column(values: Buffer<<Self::Access as AccessType>::Scalar>) -> BlockEntry {
        DateType::upcast_column(values).into()
    }
}

pub struct AggregateDistinctAdapterState<A: DistinctAdapter>
where <A::Access as AccessType>::Scalar: Copy + Send + HashtableKeyable
{
    set: CommonHashSet<<A::Access as AccessType>::Scalar>,
    _adapter: PhantomData<A>,
}

impl<A> DistinctStateFunc for AggregateDistinctAdapterState<A>
where
    A: DistinctAdapter,
    <A::Access as AccessType>::Scalar: Copy + Send + HashtableKeyable,
{
    fn new() -> Self {
        AggregateDistinctAdapterState {
            set: CommonHashSet::with_capacity(4),
            _adapter: PhantomData,
        }
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn memory_size(&self) -> usize {
        std::mem::size_of_val(&self.set)
            + self.set.capacity() * std::mem::size_of::<<A::Access as AccessType>::Scalar>()
    }

    fn add(&mut self, columns: ProjectedBlock, row: usize) -> Result<()> {
        let view = A::downcast_column(&columns);
        let v = unsafe { view.index_unchecked(row) };
        let key: <A::Access as AccessType>::Scalar = A::Access::to_owned_scalar(v);
        let _ = self.set.set_insert(key).is_ok();
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let view = A::downcast_column(&columns);
        match validity {
            Some(bitmap) => {
                for (t, v) in view.iter().zip(bitmap.iter()) {
                    if v {
                        let key: <A::Access as AccessType>::Scalar = A::Access::to_owned_scalar(t);
                        let _ = self.set.set_insert(key).is_ok();
                    }
                }
            }
            None => {
                for row in 0..input_rows {
                    let v = unsafe { view.index_unchecked(row) };
                    let key: <A::Access as AccessType>::Scalar = A::Access::to_owned_scalar(v);
                    let _ = self.set.set_insert(key).is_ok();
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.set_merge(&rhs.set);
        Ok(())
    }

    fn build_entries(&mut self, _types: &[DataType]) -> Result<Vec<BlockEntry>> {
        let values: Buffer<<A::Access as AccessType>::Scalar> =
            self.set.iter().map(|e| *e.key()).collect();
        Ok(vec![A::upcast_column(values)])
    }
}

impl<A> StateSerde for AggregateDistinctAdapterState<A>
where
    A: DistinctAdapter,
    <A::Access as AccessType>::Scalar: Copy + Send + HashtableKeyable,
{
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::Array(Box::new(A::Access::data_type())).into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<ArrayType<A::Access>, Self, _>(
            places,
            loc,
            builders,
            |state, builder| {
                for v in state.set.iter() {
                    builder.put_item(A::Access::to_scalar_ref(v.key()));
                }
                builder.commit_row();
                Ok(())
            },
        )
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<ArrayType<A::Access>, Self, _>(places, loc, state, filter, |state, data| {
            for v in A::Access::iter_column(&data) {
                let key: <A::Access as AccessType>::Scalar = A::Access::to_owned_scalar(v);
                let _ = state.set.set_insert(key).is_ok();
            }
            Ok(())
        })
    }
}

pub type AggregateDistinctNumberState<T> = AggregateDistinctAdapterState<NumberAdapter<T>>;
pub type AggregateDistinctTimestampState = AggregateDistinctAdapterState<TimestampAdapter>;
pub type AggregateDistinctDateState = AggregateDistinctAdapterState<DateAdapter>;

pub struct AggregateDistinctState {
    set: HashSet<Vec<u8>>,
    bytes: usize,
}

impl DistinctStateFunc for AggregateDistinctState {
    fn spill_types(_: &[DataType]) -> Vec<DataType> {
        vec![DataType::Binary]
    }

    fn spill_entries(&mut self, _: &[DataType]) -> Result<Vec<BlockEntry>> {
        let mut builder = BinaryColumnBuilder::with_capacity(self.len(), self.bytes);
        for key in &self.set {
            builder.put_slice(key);
            builder.commit_row();
        }
        Ok(vec![Column::Binary(builder.build()).into()])
    }

    fn merge_spilled(&mut self, columns: ProjectedBlock, _: usize) -> Result<()> {
        for key in columns[0].downcast::<BinaryType>().unwrap().iter() {
            self.insert(key.to_vec());
        }
        Ok(())
    }

    fn new() -> Self {
        AggregateDistinctState {
            set: HashSet::new(),
            bytes: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn memory_size(&self) -> usize {
        self.bytes + self.set.capacity() * (size_of::<Vec<u8>>() + 1)
    }

    fn add(&mut self, columns: ProjectedBlock, row: usize) -> Result<()> {
        let values = columns
            .iter()
            .map(|entry| unsafe { entry.index_unchecked(row) }.to_owned())
            .collect::<Vec<_>>();

        let mut buffer = Vec::with_capacity(values.len() * std::mem::size_of::<Scalar>());
        values.serialize(&mut buffer)?;
        self.insert(buffer);
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        match validity {
            Some(validity) => {
                for (row, b) in (0..input_rows).zip(validity) {
                    if !b {
                        continue;
                    }
                    self.add(columns, row)?;
                }
            }
            None => {
                for row in 0..input_rows {
                    self.add(columns, row)?;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        for value in &rhs.set {
            self.insert(value.clone());
        }
        Ok(())
    }

    fn build_entries(&mut self, types: &[DataType]) -> Result<Vec<BlockEntry>> {
        let mut builders: Vec<ColumnBuilder> = types
            .iter()
            .map(|ty| ColumnBuilder::with_capacity(ty, self.set.len()))
            .collect();

        for data in self.set.iter() {
            let mut slice = data.as_slice();
            let scalars: Vec<Scalar> = borsh_partial_deserialize(&mut slice)?;
            scalars.iter().enumerate().for_each(|(idx, group_value)| {
                builders[idx].push(group_value.as_ref());
            });
        }

        Ok(builders.into_iter().map(|b| b.build().into()).collect())
    }
}

impl AggregateDistinctState {
    fn insert(&mut self, value: Vec<u8>) {
        let bytes = value.capacity();
        if self.set.insert(value) {
            self.bytes += bytes;
        }
    }
}

impl StateSerde for AggregateDistinctState {
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::Array(Box::new(DataType::Binary)).into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<ArrayType<BinaryType>, Self, _>(
            places,
            loc,
            builders,
            |state, builder| {
                for v in state.set.iter() {
                    builder.put_item(v);
                }
                builder.commit_row();
                Ok(())
            },
        )
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<ArrayType<BinaryType>, Self, _>(places, loc, state, filter, |state, data| {
            for v in data.iter() {
                state.insert(v.to_vec());
            }
            Ok(())
        })
    }
}

pub struct AggregateDistinctStringState {
    set: ShortStringHashSet<[u8]>,
}

impl DistinctStateFunc for AggregateDistinctStringState {
    fn spill_types(_: &[DataType]) -> Vec<DataType> {
        vec![DataType::Binary]
    }

    fn spill_entries(&mut self, _: &[DataType]) -> Result<Vec<BlockEntry>> {
        let mut builder = BinaryColumnBuilder::with_capacity(self.len(), 0);
        for key in self.set.iter() {
            builder.put_slice(key.key());
            builder.commit_row();
        }
        Ok(vec![Column::Binary(builder.build()).into()])
    }

    fn merge_spilled(&mut self, columns: ProjectedBlock, _: usize) -> Result<()> {
        for key in columns[0].downcast::<BinaryType>().unwrap().iter() {
            let _ = self.set.set_insert(key);
        }
        Ok(())
    }

    fn new() -> Self {
        #![allow(clippy::arc_with_non_send_sync)]
        AggregateDistinctStringState {
            set: ShortStringHashSet::<[u8]>::with_capacity(4, Arc::new(Bump::new())),
        }
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn memory_size(&self) -> usize {
        self.set.bytes_len(false)
    }

    fn add(&mut self, columns: ProjectedBlock, row: usize) -> Result<()> {
        let view = columns[0].downcast::<StringType>().unwrap();
        let data = unsafe { view.index_unchecked(row) };
        let _ = self.set.set_insert(data.as_bytes());
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let view = columns[0].downcast::<StringType>().unwrap();
        match validity {
            Some(v) => {
                for row in 0..input_rows {
                    if v.get_bit(row) {
                        let data = unsafe { view.index_unchecked(row) };
                        let _ = self.set.set_insert(data.as_bytes());
                    }
                }
            }
            None => {
                for row in 0..input_rows {
                    let data = unsafe { view.index_unchecked(row) };
                    let _ = self.set.set_insert(data.as_bytes());
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.set_merge(&rhs.set);
        Ok(())
    }

    fn build_entries(&mut self, _types: &[DataType]) -> Result<Vec<BlockEntry>> {
        let mut builder = StringColumnBuilder::with_capacity(self.set.len());
        for key in self.set.iter() {
            builder.put_and_commit(unsafe { std::str::from_utf8_unchecked(key.key()) });
        }
        Ok(vec![Column::String(builder.build()).into()])
    }
}

impl StateSerde for AggregateDistinctStringState {
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::Array(Box::new(DataType::Binary)).into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<ArrayType<BinaryType>, Self, _>(
            places,
            loc,
            builders,
            |state, builder| {
                for v in state.set.iter() {
                    builder.put_item(v.key());
                }
                builder.commit_row();
                Ok(())
            },
        )
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<ArrayType<BinaryType>, Self, _>(places, loc, state, filter, |state, data| {
            for v in data.iter() {
                let _ = state.set.set_insert(v);
            }
            Ok(())
        })
    }
}

// For count(distinct string) and uniq(string)
pub struct AggregateUniqStringState {
    set: StackHashSet<u128>,
}

impl DistinctStateFunc for AggregateUniqStringState {
    fn visit_serialized(
        state: &BlockEntry,
        mut consume: impl FnMut(DataBlock) -> Result<()>,
    ) -> Result<()> {
        let Column::Tuple(fields) = state.to_column() else {
            return Err(ErrorCode::BadBytes(
                "Invalid serialized string DISTINCT state",
            ));
        };
        let column = fields[0]
            .as_binary()
            .ok_or_else(|| ErrorCode::BadBytes("String DISTINCT state must be binary"))?;
        let mut data = column.index(0).unwrap();
        let count = data.read_uvarint()?;
        if count.checked_mul(16) != Some(data.len() as u64) {
            return Err(ErrorCode::BadBytes(
                "Invalid serialized string DISTINCT key count",
            ));
        }
        for chunk in data.chunks(SPILL_BATCH_ROWS * 16) {
            let mut builder = BinaryColumnBuilder::with_capacity(chunk.len() / 16, chunk.len());
            for key in chunk.chunks_exact(16) {
                builder.put_slice(key);
                builder.commit_row();
            }
            consume(DataBlock::new_from_columns(vec![Column::Binary(
                builder.build(),
            )]))?;
        }
        Ok(())
    }

    fn new() -> Self {
        AggregateUniqStringState {
            set: StackHashSet::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn len(&self) -> usize {
        self.set.len()
    }

    fn memory_size(&self) -> usize {
        self.set.capacity() * size_of::<u128>()
    }

    fn add(&mut self, columns: ProjectedBlock, row: usize) -> Result<()> {
        let view = columns[0].downcast::<StringType>().unwrap();
        let data = unsafe { view.index_unchecked(row) }.as_bytes();
        let mut hasher = SipHasher24::new();
        hasher.write(data);
        let hash128 = hasher.finish128();
        let _ = self.set.set_insert(hash128.into()).is_ok();
        Ok(())
    }

    fn batch_add(
        &mut self,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let view = columns[0].downcast::<StringType>().unwrap();
        match validity {
            Some(v) => {
                for (t, v) in view.iter().zip(v.iter()) {
                    if v {
                        let mut hasher = SipHasher24::new();
                        hasher.write(t.as_bytes());
                        let hash128 = hasher.finish128();
                        let _ = self.set.set_insert(hash128.into()).is_ok();
                    }
                }
            }
            _ => {
                for row in 0..input_rows {
                    let data = unsafe { view.index_unchecked(row) };
                    let mut hasher = SipHasher24::new();
                    hasher.write(data.as_bytes());
                    let hash128 = hasher.finish128();
                    let _ = self.set.set_insert(hash128.into()).is_ok();
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.set.set_merge(&rhs.set);
        Ok(())
    }

    // This method won't be called.
    fn build_entries(&mut self, _types: &[DataType]) -> Result<Vec<BlockEntry>> {
        Ok(vec![])
    }

    fn spill_types(_: &[DataType]) -> Vec<DataType> {
        vec![DataType::Binary]
    }

    fn spill_entries(&mut self, _: &[DataType]) -> Result<Vec<BlockEntry>> {
        let mut builder = BinaryColumnBuilder::with_capacity(self.len(), self.len() * 16);
        for key in self.set.iter() {
            builder.put_slice(&key.key().to_le_bytes());
            builder.commit_row();
        }
        Ok(vec![Column::Binary(builder.build()).into()])
    }

    fn merge_spilled(&mut self, columns: ProjectedBlock, _: usize) -> Result<()> {
        let view = columns[0].downcast::<BinaryType>().unwrap();
        for bytes in view.iter() {
            let bytes = bytes.try_into().map_err(|_| {
                ErrorCode::BadBytes("Invalid spilled COUNT DISTINCT string fingerprint")
            })?;
            let _ = self.set.set_insert(u128::from_le_bytes(bytes));
        }
        Ok(())
    }
}

impl AggregateUniqStringState {
    pub fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        writer.write_uvarint(self.set.len() as u64)?;
        for value in self.set.iter() {
            value.key().serialize(writer)?;
        }
        Ok(())
    }

    pub fn deserialize(reader: &mut &[u8]) -> Result<Self> {
        let size = reader.read_uvarint()?;
        let mut set = StackHashSet::with_capacity(size as usize);
        for _ in 0..size {
            let e = borsh_partial_deserialize(reader)?;
            let _ = set.set_insert(e).is_ok();
        }
        Ok(Self { set })
    }
}

impl StateSerde for AggregateUniqStringState {
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let binary_builder = builders[0].as_binary_mut().unwrap();
        for place in places {
            let state: &mut Self = AggrState::new(*place, loc).get();
            state.serialize(&mut binary_builder.data)?;
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<BinaryType, Self, _>(places, loc, state, filter, |state, mut data| {
            let rhs = Self::deserialize(&mut data)?;
            state.merge(&rhs)
        })
    }
}
