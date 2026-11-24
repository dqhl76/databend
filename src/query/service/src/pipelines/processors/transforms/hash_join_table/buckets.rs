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

use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::hashjoin_hashtable::hash_bits;

/// Number of partitions a partitioned hash join splits its build rows into at each level.
pub const NUM_PARTITIONS: usize = 16;

/// Partition id of a row at the recursion level whose partition bits start at `shift_bits`.
///
/// The hash table of a partition hashes the same keys with the same hash: the bucket index is the
/// top bits (`hash >> hash_shift`) and the early-filtering tag is the low 4 bits
/// (`hash & 0b1111`). Taking partition bits directly from either end would leave them constant
/// inside a partition and reduce the reachable buckets or tags.
///
/// Each level takes the parity of the tag nibble and its own nibble
/// `[4 + shift_bits, 8 + shift_bits)`. Over GF(2) these parities are independent of each other and
/// of the tag bits, keeping bucket indices and tags uniformly spread inside every partition at
/// every level.
#[inline(always)]
pub fn partition_id(hash: u64, shift_bits: usize) -> u64 {
    debug_assert!(shift_bits + 8 <= hash_bits() as usize);
    ((hash >> (4 + shift_bits)) ^ hash) & 0b1111
}

/// The bucket headers of a hash join hash table.
///
/// `Single` stores one contiguous zero-initialized bucket array for unspilled/flat builds,
/// enabling direct `pointers[hash >> hash_shift]` lookups with zero indirection during probing.
///
/// `Partitioned` stores one lazily-allocated segment per radix partition (chosen by
/// [`partition_id`]): a partition's segment is allocated only when that partition is built into
/// memory, and never for partitions that spill.
pub enum Buckets {
    Single {
        pointers: Box<[AtomicU64]>,
        hash_shift: usize,
    },
    Partitioned {
        segments: [AtomicPtr<AtomicU64>; NUM_PARTITIONS],
        segment_capacity: usize,
        hash_shift: usize,
        partition_shift: usize,
    },
}

unsafe impl Send for Buckets {}

unsafe impl Sync for Buckets {}

impl Buckets {
    /// Total bucket count for `row_num` build rows.
    pub fn capacity(row_num: usize) -> usize {
        std::cmp::max((row_num * 2).next_power_of_two(), 1 << 10)
    }

    /// One contiguous bucket array for `row_num` build rows.
    pub fn with_build_row_num(row_num: usize) -> Self {
        let capacity = Self::capacity(row_num);
        // Safety: `AtomicU64` has the same representation as `u64`, and all-zero bytes are valid.
        let pointers: Box<[AtomicU64]> = unsafe { Box::new_zeroed_slice(capacity).assume_init() };
        Buckets::Single {
            pointers,
            hash_shift: (hash_bits() - capacity.trailing_zeros()) as usize,
        }
    }

    /// One segment for each partition of `row_num` build rows at the level whose partition bits
    /// start at `shift_bits`. No segment is allocated until [`Buckets::allocate`].
    pub fn partitioned(row_num: usize, shift_bits: usize) -> Self {
        let segment_capacity = Self::capacity(row_num) / NUM_PARTITIONS;
        Buckets::Partitioned {
            segments: std::array::from_fn(|_| AtomicPtr::new(std::ptr::null_mut())),
            segment_capacity,
            hash_shift: (hash_bits() - segment_capacity.trailing_zeros()) as usize,
            partition_shift: 4 + shift_bits,
        }
    }

    /// Number of buckets in one segment (or total buckets for `Single`).
    pub fn segment_capacity(&self) -> usize {
        match self {
            Buckets::Single { pointers, .. } => pointers.len(),
            Buckets::Partitioned {
                segment_capacity, ..
            } => *segment_capacity,
        }
    }

    /// Allocate the segment of `partition`. It must happen before any row of the partition is
    /// inserted or probed, ordered by the synchronization that hands the partition to the
    /// threads building or probing it. No-op for `Single`.
    pub fn allocate(&self, partition: usize) {
        let Buckets::Partitioned {
            segments,
            segment_capacity,
            ..
        } = self
        else {
            return;
        };

        let segment = &segments[partition];
        if !segment.load(Ordering::Acquire).is_null() {
            return;
        }

        let pointers: Box<[u64]> =
            unsafe { Box::new_zeroed_slice(*segment_capacity).assume_init() };
        let pointers = Box::into_raw(pointers) as *mut AtomicU64;
        if segment
            .compare_exchange(
                std::ptr::null_mut(),
                pointers,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            // Safety: `pointers` was just leaked from a box of `segment_capacity` buckets and was
            // not published.
            drop(unsafe { Self::boxed_segment(pointers, *segment_capacity) });
        }
    }

    /// Number of allocated buckets.
    pub fn allocated_buckets(&self) -> usize {
        match self {
            Buckets::Single { pointers, .. } => pointers.len(),
            Buckets::Partitioned {
                segments,
                segment_capacity,
                ..
            } => {
                segments
                    .iter()
                    .filter(|segment| !segment.load(Ordering::Relaxed).is_null())
                    .count()
                    * *segment_capacity
            }
        }
    }

    /// Lookup a bucket inside a `Partitioned` segment table.
    #[inline(always)]
    pub(crate) fn partitioned_bucket(
        segments: &[AtomicPtr<AtomicU64>; NUM_PARTITIONS],
        hash_shift: usize,
        partition_shift: usize,
        hash: u64,
    ) -> &AtomicU64 {
        let partition = (((hash >> partition_shift) ^ hash) & (NUM_PARTITIONS as u64 - 1)) as usize;
        let segment = segments[partition].load(Ordering::Relaxed);
        debug_assert!(
            !segment.is_null(),
            "bucket segment {partition} is not allocated"
        );
        // Safety: the segment holds `segment_capacity` buckets, and `hash >> hash_shift` is less
        // than `segment_capacity` because the hash has `hash_bits()` bits.
        unsafe { &*segment.add((hash >> hash_shift) as usize) }
    }

    /// The bucket of `hash`. Its segment must be allocated.
    #[inline(always)]
    pub fn bucket(&self, hash: u64) -> &AtomicU64 {
        match self {
            Buckets::Single {
                pointers,
                hash_shift,
            } => {
                let index = (hash >> *hash_shift) as usize;
                debug_assert!(index < pointers.len());
                // Safety: `hash >> hash_shift` is less than `pointers.len()` because the hash has
                // `hash_bits()` bits.
                unsafe { pointers.get_unchecked(index) }
            }
            Buckets::Partitioned {
                segments,
                hash_shift,
                partition_shift,
                ..
            } => Self::partitioned_bucket(segments, *hash_shift, *partition_shift, hash),
        }
    }

    /// The bucket header of `hash`. Its segment must be allocated.
    #[inline(always)]
    pub fn header(&self, hash: u64) -> u64 {
        self.bucket(hash).load(Ordering::Relaxed)
    }

    /// # Safety
    /// `pointers` must come from a leaked box of `segment_capacity` buckets.
    unsafe fn boxed_segment(pointers: *mut AtomicU64, segment_capacity: usize) -> Box<[u64]> {
        unsafe {
            Box::from_raw(std::ptr::slice_from_raw_parts_mut(
                pointers as *mut u64,
                segment_capacity,
            ))
        }
    }
}

impl Drop for Buckets {
    fn drop(&mut self) {
        if let Buckets::Partitioned {
            segments,
            segment_capacity,
            ..
        } = self
        {
            for segment in segments {
                let pointers = segment.load(Ordering::Acquire);
                if !pointers.is_null() {
                    // Safety: every published segment is a leaked box of `segment_capacity`
                    // buckets.
                    drop(unsafe { Self::boxed_segment(pointers, *segment_capacity) });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::Ordering;

    use databend_common_hashtable::FastHash;
    use databend_common_hashtable::hash_join_fast_string_hash;

    use super::Buckets;
    use super::NUM_PARTITIONS;
    use super::hash_bits;
    use super::partition_id;

    fn hash(key: u64, width: u32) -> u64 {
        let mut h = key;
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccd);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
        h ^= h >> 33;
        h >> (64 - width)
    }

    /// Share of a segment's buckets, and number of tags, that the rows of one partition reach
    /// at the last of `levels` levels.
    fn partition_coverage(width: u32, levels: usize, build_rows: usize) -> (f64, usize) {
        let capacity = Buckets::partitioned(build_rows, 0).segment_capacity();
        let hash_shift = width - capacity.trailing_zeros();

        let mut buckets = HashSet::new();
        let mut tags = HashSet::new();
        let mut sampled = 0;
        let mut key = 0;
        while sampled < capacity * 8 {
            key += 1;
            let h = hash(key, width);
            if (0..levels).all(|level| partition_id(h, level * 4) == 5) {
                buckets.insert(h >> hash_shift);
                tags.insert(h & 0b1111);
                sampled += 1;
            }
        }
        (buckets.len() as f64 / capacity as f64, tags.len())
    }

    #[test]
    fn test_partition_keeps_all_buckets_reachable() {
        for width in [32, 64] {
            for levels in [1, 2] {
                let (bucket_coverage, tags) = partition_coverage(width, levels, 1 << 14);
                assert!(
                    bucket_coverage > 0.99,
                    "width {width}, levels {levels}: only {bucket_coverage} of buckets reachable"
                );
                assert_eq!(tags, NUM_PARTITIONS, "width {width}, levels {levels}");
            }
        }
    }

    #[test]
    fn test_partition_is_balanced() {
        for width in [32, 64] {
            for shift_bits in [0, 4, 24] {
                let mut counts = [0usize; NUM_PARTITIONS];
                for key in 0..(1 << 16) {
                    counts[partition_id(hash(key, width), shift_bits) as usize] += 1;
                }
                let expected = (1 << 16) / NUM_PARTITIONS;
                for count in counts {
                    assert!(
                        count.abs_diff(expected) < expected / 10,
                        "width {width}, shift {shift_bits}: {counts:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_segments_are_allocated_per_partition() {
        let rows = 1 << 20;
        let single = Buckets::with_build_row_num(rows);
        let partitioned = Buckets::partitioned(rows, 4);
        assert_eq!(partitioned.allocated_buckets(), 0);
        assert_eq!(single.allocated_buckets(), Buckets::capacity(rows));

        partitioned.allocate(3);
        partitioned.allocate(3);
        assert_eq!(
            partitioned.allocated_buckets(),
            single.allocated_buckets() / NUM_PARTITIONS
        );

        for pid in 0..NUM_PARTITIONS {
            partitioned.allocate(pid);
        }
        assert_eq!(partitioned.allocated_buckets(), single.allocated_buckets());
    }

    #[test]
    fn test_bucket_lies_in_its_partition_segment() {
        let width = hash_bits();
        for shift_bits in [0, 4, 8] {
            let buckets = Buckets::partitioned(1 << 12, shift_bits);
            for pid in 0..NUM_PARTITIONS {
                buckets.allocate(pid);
            }

            let Buckets::Partitioned {
                ref segments,
                segment_capacity,
                ..
            } = buckets
            else {
                unreachable!()
            };

            for key in 0..(1 << 12) {
                let h = hash(key, width);
                let pid = partition_id(h, shift_bits) as usize;
                let segment = segments[pid].load(Ordering::Relaxed);
                let offset = (buckets.bucket(h) as *const _ as usize - segment as usize)
                    / std::mem::size_of::<u64>();
                assert!(offset < segment_capacity, "key {key}, shift {shift_bits}");
            }
        }
    }

    #[test]
    fn test_single_bucket_read_write() {
        let width = hash_bits();
        let buckets = Buckets::with_build_row_num(1 << 10);
        for key in 0..(1 << 10) {
            let h = hash(key, width);
            buckets.bucket(h).store(key + 1, Ordering::Relaxed);
        }
        for key in 0..(1 << 10) {
            let h = hash(key, width);
            assert_ne!(buckets.header(h), 0);
        }
    }

    /// Build rows are routed to partitions by the `FastHash` of their keys, while the string
    /// hash table places a key by `hash_join_fast_string_hash`. The two differ only for the empty
    /// key under SSE4.2, and must still agree on its partition at every level.
    #[test]
    fn test_string_keys_route_to_their_table_segment() {
        for key in [&b""[..], b"k", b"a longer key than eight bytes"] {
            for shift_bits in (0..=hash_bits() as usize - 8).step_by(4) {
                assert_eq!(
                    partition_id(key.fast_hash(), shift_bits),
                    partition_id(hash_join_fast_string_hash(key), shift_bits),
                    "key {key:?}, shift {shift_bits}"
                );
            }
        }
    }

    #[test]
    fn test_single_and_partitioned_hashtable_probe_equivalence() {
        use super::super::HashJoinHashTable;
        use super::super::HashJoinHashtableLike;
        use super::super::RawEntry;
        use super::super::RowPtr;

        let build_rows = 2048usize;
        let single = HashJoinHashTable::<u64>::with_build_row_num(build_rows);
        let partitioned_buckets = Buckets::partitioned(build_rows, 0);
        for pid in 0..NUM_PARTITIONS {
            partitioned_buckets.allocate(pid);
        }
        let partitioned = HashJoinHashTable::<u64>::with_buckets(partitioned_buckets);

        let mut entries_s: Vec<RawEntry<u64>> = (0..build_rows)
            .map(|i| RawEntry {
                row_ptr: RowPtr {
                    chunk_index: 0,
                    row_index: i as u32,
                },
                key: (i as u64) * 3,
                next: 0,
            })
            .collect();
        let mut entries_p = entries_s
            .iter()
            .map(|e| RawEntry {
                row_ptr: e.row_ptr,
                key: e.key,
                next: 0,
            })
            .collect::<Vec<_>>();

        for (es, ep) in entries_s.iter_mut().zip(entries_p.iter_mut()) {
            single.insert(es.key, es as *mut _);
            partitioned.insert(ep.key, ep as *mut _);
        }

        let probe_keys: Vec<u64> = (0..(build_rows * 2) as u64).collect();
        let mut h_s: Vec<u64> = probe_keys.iter().map(|k| k.fast_hash()).collect();
        let mut h_p = h_s.clone();

        let _ = single.probe(&mut h_s, None);
        let _ = partitioned.probe(&mut h_p, None);

        for (i, key) in probe_keys.iter().enumerate() {
            assert_eq!(
                single.next_contains(key, h_s[i]),
                partitioned.next_contains(key, h_p[i]),
                "probe mismatch for key {key}"
            );
            assert_eq!(
                single.next_contains(key, h_s[i]),
                key % 3 == 0 && (*key as usize) < build_rows * 3,
                "unexpected containment for key {key}"
            );
        }
    }
}
