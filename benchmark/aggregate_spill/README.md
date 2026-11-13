# Aggregate Spill Benchmark

`run.py` runs in-memory and spilling `GROUP BY` queries on TPC-H `lineitem` at scale factor 8.
For each query it reports the server time, the peak memory and the aggregate spill size.

The queries follow the grouping benchmark of "Robust External Hash Aggregation in the Solid
State Age" (Kuiper et al., ICDE 2024, Section VI).

The SQL lives in files next to `run.py`:

- `load.sql` creates and loads the table.
- `queries/` holds one file per case, named after the case. To add a case, add a file.
  - `mem_*` cases run with default settings and stay in memory on a machine with 32 GB.
  - `spill_*` cases spill under a fixed `max_query_memory_usage`, set by their `SETTINGS` clause.

## Run

Start the standalone deployment, then run the benchmark. Run both from the repository root:

```bash
make run
pip install tpchgen-cli
./benchmark/aggregate_spill/run.py --max-threads 8 --output result.json
```

- `make run` builds release binaries and starts `scripts/ci/deploy/databend-query-standalone.sh`.
- The metrics are read from the deployment's query log under `.databend`.
- The first run loads `lineitem_sf8`, and later runs reuse it.
- The benchmark takes about five minutes on 8 cores.

`--help` lists the options.
