# Function-owned aggregate spilling

Outer aggregation partitions SQL `GROUP BY` keys. It cannot subdivide one large
aggregate state: a global `COUNT(DISTINCT x)` has one group, and several different
DISTINCT expressions retain several independent sets. The single-state final
processor must also merge incoming partial blocks promptly rather than retain
all serialized partial states until finalization.

Function-owned spilling handles this state growth without teaching the grouped
aggregate processors about each aggregate's internal representation.

## Execution contract

`AggregateFunction::with_spill` creates a query-scoped function instance. The
pipeline builder supplies an `AggregateFunctionSpill` implementation; functions
can spill during accumulation, state merging, serialization, and finalization.
Functions without a spill strategy return `None` and retain their existing
implementation. Ordinary fixed-size reducers do not need to spill internally.

The expression crate defines the synchronous I/O contract and file descriptor.
The service implementation uses `SpillsBufferPool` and the synchronous
`SpillsDataWriter::write/close` and `SpillsDataReader::read` interfaces. The pool's
workers perform the asynchronous storage operations. Aggregate functions do not
own a runtime or block on an async pipeline processor.

Function-owned spilling reuses the outer aggregate pressure and force settings,
with separate state-selection and restore budgets:

| Setting | Default | Meaning |
| --- | --- | --- |
| `aggregate_spilling_memory_ratio` | 60 | Shared with outer aggregate spilling. Global tracked memory pressure starts at this percentage of `max_memory_usage`; zero disables this trigger. |
| `aggregate_function_spilling_memory_threshold` | 64 MiB | Minimum native state size required to trigger spilling under memory pressure. Size alone never triggers spilling. |
| `aggregate_function_restore_memory_threshold` | 64 MiB | Independent soft budget for restored partitions and external merge buffers. |
| `force_aggregate_data_spill` | 0 | Shared test switch: force both outer and function-owned spilling, independently of pressure and state size. |

Query and workload-group memory constraints are still shared resource limits.
Query pressure applies when `query_out_of_memory_behavior = 'spilling'` and
`max_query_memory_usage` is nonzero. Both layers use the same `MemorySettings`
construction, but function spilling can execute without an outer spill.

Without memory pressure, normal accumulation and merging stay in memory even
when a state is larger than the selection threshold or restore budget. Under
pressure, only nonempty states at least as large as the selection threshold
trigger spilling. Repeated pressure checks do not relax this threshold.
Checks do not reserve a global memory budget or elect a single worker, so
concurrent workers can spill together.

Accumulation checks pressure at input-batch boundaries (up to 2048 rows), including
duplicate rows in grouped DISTINCT states. Existing spilled runs still require
flushing resident tails during serialization/finalization, and streaming merges
flush tails before appending incoming runs to preserve order.
Restore uses its independent budget, not pressure or forced spilling, so it can
bound partition merging after pressure clears and avoid infinite forced spilling.

## Restore strategies

| State family | Spill and final restore |
| --- | --- |
| DISTINCT / `uniq` | Partition canonical set keys into 4 hash buckets. Restore one bucket, deduplicate across its runs, and feed unique values into a fresh nested reducer. COUNT sums the disjoint bucket sizes. |
| `ARRAY_AGG`, `STRING_AGG` | Store native runs. Merge them sequentially into the result builder, retaining duplicates and run order. STRING_AGG removes the final delimiter once, including when a durable state was split within a multibyte delimiter. |
| Aggregate `ORDER BY` adaptor | Externally sort runs and perform pairwise merge passes. Feed the final ordered stream into the nested function. At most two run readers are open during a merge. |
| `MEDIAN`, `QUANTILE_CONT` | Externally merge the values and retain the ranks needed by the requested quantiles. Apply the existing float/decimal interpolation semantics; never merge per-run quantile results. |

DISTINCT retains its existing typed sets and string fingerprints. Partition keys
must preserve those sets' equality semantics: numeric keys use their native
representation, encoded multi-argument keys remain encoded, and the string COUNT
fingerprints are partitioned directly instead of being hashed as input strings
again. A restore bucket exceeding the limit is spilled into another 4 buckets
using the next hash bits. If all 64 bits are exhausted, restore returns an
explicit memory error. A single indivisible value may exceed the soft limit.

The streaming adaptor owns the run lifecycle. Its function-specific result
strategy supplies the final reduction and can split durable serialized inputs
before merging. Array and DISTINCT inputs are consumed in batches; STRING_AGG
inputs are split on UTF-8 boundaries. The older ORDER BY binary state format
still requires decoding its column vector once before feeding bounded pieces.

## Serialization and ownership

Factory-created functions keep their durable `serialize_type`. This format is
used by typed aggregate-state tables, indexes, and `_state` results.
`spill_serialize_type` advertises the ephemeral execution layout; a specialized
function's actual `serialize_type` must match it. The partial physical plan uses
that execution schema for exchange. Specialized functions accept both durable
input states and execution states containing an additional binary run manifest.

NULL, OR-NULL, IF, and ORDER BY adaptors propagate query specialization. Adaptors
that expose durable states deliberately do not propagate it. Temporary file
references must never escape into persisted aggregate states. This is an
internal exchange format change and assumes query workers run the same version.

Files are immutable and query-owned. Serializing or merging a state shares file
references; dropping a partial state must not delete files that a final worker
or repeated read-only finalization still needs. Files are registered before
opening the writer, including failed writes, and use the existing query spill
progress and vacuum lifecycle. This inherits that lifecycle's configuration and
crash-retention behavior; there is no per-state eager deletion.

Distributed function spilling requires shared remote spill storage. A local
spill backend is supported for a single node; a multi-node query attempting a
function spill with that backend receives an explicit unsupported error before
writing an inaccessible reference.

## Bounds and extension points

The selection threshold is a minimum size for triggering a spill under pressure,
not an upper bound on state memory.
The restore budget is a soft per-partition/buffer limit, not a total query-memory ceiling.
Because normal accumulation is pressure-driven, the first spilled native run can
exceed that budget. Serializing or reading that run can still require transient
memory proportional to its size; the budget governs repartitioning and merge
buffers, not a hard cap on every allocation or input run.
Input batches, serialization/sort scratch space, reader prefetch, writer buffers,
run manifests, and output columns also consume memory. Manifests grow with run
count. Multi-pass sorting trades additional temporary storage and I/O for a
constant number of active readers. ARRAY_AGG and STRING_AGG necessarily allocate
memory proportional to their final SQL value.

Support is semantic and opt-in. A DISTINCT wrapper can spill its set around any
nested reducer, but bounding the entire function also requires that reducer to
have bounded state or its own strategy. The ORDER BY adaptor has the same
requirement for the function consuming its ordered stream. Other growing native
states, including MODE, HISTOGRAM, QUANTILE_DISC, JSON aggregates, bitmap
aggregates, and UDAFs, retain their existing behavior until they implement a
suitable restore strategy. Persisted `_state` producers are also unchanged.
Serializing an opaque state and restoring every run into one large state is not
a bounded restore strategy.

To add a function, define its state-memory estimate and restore algebra, return
a specialization from `with_spill`, and advertise its execution serialization
layout. Reuse the streaming lifecycle when native runs can be consumed in order;
reuse internal hash partitioning or external sorting when the reduction requires
co-locating equal keys or globally ordered values. Keep the durable state format
unchanged. No aggregate processor changes are needed for additional functions.

## Regression coverage and validation

`aggregate_spill/tests.rs` covers multi-stage serialized and direct state merges,
recursive repartitioning, native/durable state mixing, floating-point keys,
NULL/empty/filter semantics, ordered runs, exact quantiles, repeated read-only
finalization, reader bounds, and I/O/cancellation errors. It also checks that
large states stay in memory without pressure and duplicate-only grouped input
still polls pressure. Service tests exercise the real async buffer adapter,
global aggregate queries, pressure gating, and the independent restore budget.
Policy tests cover repeated pressure without bypassing the size threshold, the
threshold boundary, absence of pressure, and forced spilling.
The SQL regression suite includes the six-prefix DISTINCT workload, grouped
outer spilling, streaming and ordered results, decimal quantiles, and the
shared aggregate memory pressure in a global aggregation without forced spilling.

When Rust compilation is permitted, the focused validation commands are:

```sh
cargo test -p databend-common-functions --lib aggregates::aggregate_spill::tests
cargo test -p databend-query --lib spillers::aggregate_function::tests
cargo run -p databend-sqllogictests --bin databend-sqllogictests -- \
  --handlers http --run_dir 03_common \
  --run_file 03_0039_spill_aggregate_function.test
```

The SQL command requires a running query service. Run the existing aggregate
function and outer-spill suites as well before submitting the change.
