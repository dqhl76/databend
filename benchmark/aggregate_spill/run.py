#!/usr/bin/env python3
"""Aggregation benchmark: in-memory and spilling GROUP BY on TPC-H lineitem.

Derived from the grouping benchmark of "Robust External Hash Aggregation in the Solid State
Age" (Kuiper et al., ICDE 2024, Section VI): a query groups lineitem by a set of keys, selects
the other columns with any_value in its wide variant, and ends with OFFSET N - 1 so that it
returns exactly one row whatever the system, after aggregating every group.

It runs a small subset at scale factor 8 that covers the in-memory path with default settings
and the spill path under a fixed query memory limit, and finishes within about five minutes on
8 cores. For every case it records the server time, the peak memory and the aggregate spill
size, and writes them to a JSON result file.

    run.py --bin-dir <dir with databend-query and databend-meta> --work-dir <dir> \
        --output result.json

Requires tpchgen-cli (`pip install tpchgen-cli`) on PATH or given with --tpchgen. The data is
generated once into the work dir and reused while the table holds the expected rows.
"""

import argparse
import base64
import glob
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
import urllib.request

SF = 8
TABLE = f"lineitem_sf{SF}"
EXPECTED_ROWS = 47989007
COLS = [
    "l_orderkey",
    "l_partkey",
    "l_suppkey",
    "l_linenumber",
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
    "l_returnflag",
    "l_linestatus",
    "l_shipdate",
    "l_commitdate",
    "l_receiptdate",
    "l_shipinstruct",
    "l_shipmode",
    "l_comment",
]
# Groupings of Table I of the paper used here, with their number of groups at SF 8.
GROUPINGS = {
    1: (["l_returnflag", "l_linestatus"], 4),
    2: (["l_partkey"], 1600000),
    5: (["l_orderkey"], 12000000),
    10: (["l_suppkey", "l_partkey", "l_shipinstruct", "l_shipmode"], 42457938),
    12: (["l_orderkey", "l_suppkey"], 47987877),
    13: (["l_suppkey", "l_partkey", "l_orderkey"], 47988996),
}
GIB = 1 << 30
SPILLING = {"query_out_of_memory_behavior": "spilling"}
# (name, grouping, variant, extra settings). In-memory cases stay far below the default
# spill threshold of any machine with 32 GB; spill cases spill under a fixed query limit, so
# that they spill the same way on any machine that has the memory to run them.
CASES = [
    ("mem_thin_g1", 1, "thin", {}),
    ("mem_thin_g5", 5, "thin", {}),
    ("mem_thin_g13", 13, "thin", {}),
    ("mem_wide_g1", 1, "wide", {}),
    ("mem_wide_g2", 2, "wide", {}),
    ("mem_wide_g5", 5, "wide", {}),
    (
        "spill_thin_g13",
        13,
        "thin",
        {**SPILLING, "max_query_memory_usage": str(GIB // 2)},
    ),
    ("spill_wide_g5", 5, "wide", {**SPILLING, "max_query_memory_usage": str(2 * GIB)}),
    (
        "spill_wide_g12",
        12,
        "wide",
        {**SPILLING, "max_query_memory_usage": str(4 * GIB)},
    ),
]
QUERY_TIMEOUT_SECS = 300


def sql_of(grouping, variant):
    keys, groups = GROUPINGS[grouping]
    select = ", ".join(keys)
    if variant == "wide":
        select += ", " + ", ".join(f"any_value({c})" for c in COLS if c not in keys)
    return (
        f"SELECT {select} FROM {TABLE} GROUP BY {', '.join(keys)} OFFSET {groups - 1}"
    )


class Server:
    def __init__(self, args):
        self.args = args
        self.work = os.path.abspath(args.work_dir)
        self.port = args.port
        # Settings of every query, e.g. max_threads to pin the parallelism of a machine.
        self.settings = {"max_execution_time": str(QUERY_TIMEOUT_SECS)}
        if args.max_threads:
            self.settings["max_threads"] = str(args.max_threads)
        self.procs = []
        self.auth = "Basic " + base64.b64encode(b"root:").decode()

    def path(self, *parts):
        return os.path.join(self.work, *parts)

    def start(self):
        p = self.port
        os.makedirs(self.path("logs"), exist_ok=True)
        meta = f"""admin_api_address = "127.0.0.1:{p + 4}"
grpc_api_address = "127.0.0.1:{p + 5}"
grpc_api_advertise_host = "127.0.0.1"
[log.file]
level = "WARN"
dir = "{self.path("logs", "meta")}"
[raft_config]
id = 0
raft_dir = "{self.path("meta")}"
raft_api_port = {p + 6}
raft_listen_host = "127.0.0.1"
raft_advertise_host = "127.0.0.1"
single = true
"""
        query = f"""[query]
max_active_sessions = 16
flight_api_address = "127.0.0.1:{p + 1}"
admin_api_address = "127.0.0.1:{p + 2}"
metric_api_address = "127.0.0.1:{p + 3}"
mysql_handler_host = "127.0.0.1"
mysql_handler_port = {p + 7}
http_handler_host = "127.0.0.1"
http_handler_port = {p}
flight_sql_handler_host = "127.0.0.1"
flight_sql_handler_port = {p + 8}
tenant_id = "aggregate_spill"
cluster_id = "aggregate_spill"
[[query.users]]
name = "root"
auth_type = "no_password"
[log.file]
level = "INFO"
dir = "{self.path("logs", "query")}"
[log.query]
on = true
[meta]
endpoints = ["127.0.0.1:{p + 5}"]
username = "root"
password = "root"
client_timeout_in_second = 60
[storage]
type = "fs"
[storage.fs]
data_path = "{self.path("data")}"
[cache]
data_cache_storage = "none"
"""
        for name, text in (("meta.toml", meta), ("query.toml", query)):
            with open(self.path(name), "w") as f:
                f.write(text)
        for port in (p, p + 5):
            if socket.socket().connect_ex(("127.0.0.1", port)) == 0:
                sys.exit(f"port {port} is in use; pass another --port")
        bin_dir = os.path.abspath(self.args.bin_dir)
        self.procs.append(
            self.spawn(
                [os.path.join(bin_dir, "databend-meta"), "-c", self.path("meta.toml")],
                "meta.out",
            )
        )
        self.wait_port(p + 5)
        self.procs.append(
            self.spawn(
                [
                    os.path.join(bin_dir, "databend-query"),
                    "-c",
                    self.path("query.toml"),
                ],
                "query.out",
            )
        )
        self.wait_port(p)

    def spawn(self, cmd, out):
        return subprocess.Popen(
            cmd, stdout=open(self.path("logs", out), "w"), stderr=subprocess.STDOUT
        )

    def wait_port(self, port, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if any(proc.poll() is not None for proc in self.procs):
                sys.exit(f"a server exited while starting; see {self.path('logs')}")
            if socket.socket().connect_ex(("127.0.0.1", port)) == 0:
                return
            time.sleep(0.5)
        sys.exit(f"port {port} not listening after {timeout}s")

    def stop(self):
        for proc in reversed(self.procs):
            proc.terminate()
            try:
                proc.wait(30)
            except subprocess.TimeoutExpired:
                proc.kill()

    def request(self, url, body=None):
        req = urllib.request.Request(
            url,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "Authorization": self.auth},
        )
        return json.load(urllib.request.urlopen(req, timeout=3600))

    def query(self, sql, settings=None):
        """Runs `sql`; returns (query id, rows, error)."""
        settings = {**self.settings, **(settings or {})}
        base = f"http://127.0.0.1:{self.port}"
        r = self.request(
            base + "/v1/query",
            {
                "sql": sql,
                "session": {"settings": settings},
                "pagination": {"wait_time_secs": 10},
            },
        )
        qid, rows = r.get("id"), list(r.get("data") or [])
        while r.get("error") is None and r.get("next_uri"):
            r = self.request(base + r["next_uri"])
            rows += r.get("data") or []
        return qid, rows, r.get("error")

    def must(self, sql):
        _, rows, err = self.query(sql)
        if err:
            sys.exit(f"{sql}: {err}")
        return rows

    def details(self, qid):
        """The finish record of `qid` in the query log."""
        for _ in range(100):
            for name in sorted(
                glob.glob(self.path("logs", "query", "query-details", "*"))
            ):
                with open(name) as f:
                    for line in f:
                        if qid not in line:
                            continue
                        record = json.loads(line[line.index("{") :])
                        if (
                            record.get("query_id") == qid
                            and record.get("log_type_name") != "Start"
                        ):
                            return record
            time.sleep(0.1)
        return {}

    def clear_spill(self):
        shutil.rmtree(self.path("data", "_query_spill"), ignore_errors=True)


def prepare_data(server, tpchgen):
    rows = server.must(f"SELECT count(*) FROM system.tables WHERE name = '{TABLE}'")
    if (
        int(rows[0][0])
        and int(server.must(f"SELECT count(*) FROM {TABLE}")[0][0]) == EXPECTED_ROWS
    ):
        print(f"reusing {TABLE}")
        return
    start = time.time()
    out = server.path("tpch")
    shutil.rmtree(out, ignore_errors=True)
    subprocess.run(
        [
            tpchgen,
            "parquet",
            "-s",
            str(SF),
            "-T",
            "lineitem",
            "--parts",
            str(2 * SF),
            "-o",
            out,
            "--no-progress",
        ],
        check=True,
    )
    server.must(f"CREATE OR REPLACE STAGE aggregate_spill_tpch URL = 'fs://{out}/'")
    server.must(
        f"CREATE OR REPLACE TABLE {TABLE} (l_orderkey BIGINT NOT NULL, l_partkey BIGINT NOT NULL, "
        "l_suppkey BIGINT NOT NULL, l_linenumber INT NOT NULL, l_quantity DECIMAL(15,2) NOT NULL, "
        "l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, "
        "l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR NOT NULL, l_linestatus VARCHAR NOT NULL, "
        "l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, "
        "l_shipinstruct VARCHAR NOT NULL, l_shipmode VARCHAR NOT NULL, l_comment VARCHAR NOT NULL)"
    )
    server.must(
        f"COPY INTO {TABLE} FROM @aggregate_spill_tpch/lineitem/ PATTERN = '.*[.]parquet' "
        "FILE_FORMAT = (TYPE = PARQUET)"
    )
    loaded = int(server.must(f"SELECT count(*) FROM {TABLE}")[0][0])
    shutil.rmtree(out, ignore_errors=True)
    if loaded != EXPECTED_ROWS:
        sys.exit(f"{TABLE} has {loaded} rows, expected {EXPECTED_ROWS}")
    print(f"loaded {TABLE} in {time.time() - start:.0f}s")


def run_cases(server, rounds):
    runs = {name: [] for name, *_ in CASES}
    for r in range(rounds):
        for name, grouping, variant, settings in CASES:
            qid, rows, err = server.query(sql_of(grouping, variant), settings)
            record = server.details(qid) if qid else {}
            server.clear_spill()
            run = {
                "ok": err is None and len(rows) == 1,
                "error": None if err is None else str(err.get("message", err))[:200],
                "rows": len(rows),
                "secs": record.get("query_duration_ms", 0) / 1000,
                "peak_mib": max((record.get("peek_memory_usage") or {}).values() or [0])
                / 2**20,
                "spill_mib": (
                    record.get("agg_spilled_bytes", 0)
                    + record.get("group_by_spilled_bytes", 0)
                )
                / 2**20,
            }
            runs[name].append(run)
            status = (
                "ok" if run["ok"] else f"FAILED rows={run['rows']} {run['error'] or ''}"
            )
            print(
                f"round {r} {name:<16} {run['secs']:8.2f}s peak {run['peak_mib']:8.0f} MiB "
                f"spill {run['spill_mib']:8.0f} MiB {status}",
                flush=True,
            )
    result = {}
    for name, grouping, variant, settings in CASES:
        ok = [run for run in runs[name] if run["ok"]]
        result[name] = {
            "grouping": grouping,
            "variant": variant,
            "settings": settings,
            "ok": len(ok) == len(runs[name]),
            # The fastest round: the noise of a shared machine only adds time.
            "secs": min((run["secs"] for run in ok), default=None),
            "peak_mib": max((run["peak_mib"] for run in ok), default=None),
            "spill_mib": max((run["spill_mib"] for run in ok), default=None),
            "errors": [run["error"] for run in runs[name] if not run["ok"]],
        }
    return result


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--bin-dir",
        required=True,
        help="directory with databend-query and databend-meta",
    )
    parser.add_argument(
        "--work-dir",
        required=True,
        help="configs, logs and data; data is reused across runs",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=18000,
        help="HTTP port; the next 8 ports are used too",
    )
    parser.add_argument(
        "--tpchgen", default=shutil.which("tpchgen-cli") or "tpchgen-cli"
    )
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument(
        "--max-threads",
        type=int,
        help="max_threads of every query; defaults to the server's",
    )
    parser.add_argument("--output", default="result.json", help="JSON result file")
    args = parser.parse_args()

    started = time.time()
    server = Server(args)
    server.start()
    try:
        version = server.must("SELECT version()")[0][0]
        print(version)
        prepare_data(server, args.tpchgen)
        cases = run_cases(server, args.rounds)
    finally:
        server.stop()
    result = {
        "version": version,
        "date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "machine": {
            "cpus": os.cpu_count(),
            "memory_gib": round(
                os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30
            ),
            "platform": platform.platform(),
        },
        "scale_factor": SF,
        "rounds": args.rounds,
        "max_threads": args.max_threads,
        "cases": cases,
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\n{'case':<16} {'secs':>8} {'peak MiB':>9} {'spill MiB':>9}")
    for name, case in cases.items():
        if case["ok"]:
            print(
                f"{name:<16} {case['secs']:8.2f} {case['peak_mib']:9.0f} {case['spill_mib']:9.0f}"
            )
        else:
            print(f"{name:<16} FAILED {case['errors']}")
    print(f"\nwrote {args.output}, finished in {time.time() - started:.0f}s")
    if any(not case["ok"] for case in cases.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
