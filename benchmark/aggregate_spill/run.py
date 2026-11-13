#!/usr/bin/env python3
"""Aggregation benchmark: in-memory and spilling GROUP BY on TPC-H lineitem.

Runs against the standalone deployment of scripts/ci/deploy/databend-query-standalone.sh;
see README.md in this directory.
"""

import argparse
import base64
import glob
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
SF = 8
# The table that load.sql creates and queries/*.sql read.
TABLE = "lineitem_sf8"
EXPECTED_ROWS = 47989007
QUERY_TIMEOUT_SECS = 300
# Paths under the working directory of the standalone deployment, as set by
# scripts/ci/deploy/config/databend-query-node-1.toml.
QUERY_DETAILS_DIR = os.path.join("logs_1", "query-details")
REMOTE_SPILL_DIR = os.path.join("stateless_test_data", "_query_spill")


def read_sql(path, **params):
    """The statements of the SQL file `path`, which ends each with `;`, without its comment
    lines and with each `{name}` replaced by `params[name]`."""
    with open(path) as f:
        text = "".join(line for line in f if not line.lstrip().startswith("--"))
    for name, value in params.items():
        text = text.replace("{" + name + "}", value)
    return [sql.strip() for sql in text.split(";") if sql.strip()]


def natural_key(path):
    """Orders the names by their numbers, so that g5 comes before g13."""
    name = os.path.basename(path)
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", name)]


def read_cases():
    """(name, SQL) of each file of queries/, named after the file."""
    cases = []
    for path in sorted(
        glob.glob(os.path.join(HERE, "queries", "*.sql")), key=natural_key
    ):
        statements = read_sql(path)
        if len(statements) != 1:
            sys.exit(f"{path} has {len(statements)} statements, expected one")
        cases.append((os.path.splitext(os.path.basename(path))[0], statements[0]))
    return cases


class Client:
    def __init__(self, args):
        self.base = f"http://127.0.0.1:{args.port}"
        self.databend_dir = os.path.abspath(args.databend_dir)
        # Settings of every query, e.g. max_threads to pin the parallelism of a machine.
        self.settings = {"max_execution_time": str(QUERY_TIMEOUT_SECS)}
        if args.max_threads:
            self.settings["max_threads"] = str(args.max_threads)
        self.auth = "Basic " + base64.b64encode(b"root:").decode()

    def path(self, *parts):
        return os.path.join(self.databend_dir, *parts)

    def request(self, url, body=None):
        req = urllib.request.Request(
            url,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "Authorization": self.auth},
        )
        return json.load(urllib.request.urlopen(req, timeout=3600))

    def query(self, sql):
        """Runs `sql`; returns (query id, rows, error)."""
        r = self.request(
            self.base + "/v1/query",
            {
                "sql": sql,
                "session": {"settings": self.settings},
                "pagination": {"wait_time_secs": 10},
            },
        )
        qid, rows = r.get("id"), list(r.get("data") or [])
        while r.get("error") is None and r.get("next_uri"):
            r = self.request(self.base + r["next_uri"])
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
            for name in sorted(glob.glob(self.path(QUERY_DETAILS_DIR, "*"))):
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
        # The spill beyond the local spill disk quota goes to the storage and is kept after
        # the query for vacuum.
        shutil.rmtree(self.path(REMOTE_SPILL_DIR), ignore_errors=True)


def prepare_data(client, tpchgen):
    rows = client.must(f"SELECT count(*) FROM system.tables WHERE name = '{TABLE}'")
    if (
        int(rows[0][0])
        and int(client.must(f"SELECT count(*) FROM {TABLE}")[0][0]) == EXPECTED_ROWS
    ):
        print(f"reusing {TABLE}")
        return
    start = time.time()
    with tempfile.TemporaryDirectory(prefix="aggregate_spill_tpch") as out:
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
        for sql in read_sql(os.path.join(HERE, "load.sql"), tpch_dir=out):
            client.must(sql)
    loaded = int(client.must(f"SELECT count(*) FROM {TABLE}")[0][0])
    if loaded != EXPECTED_ROWS:
        sys.exit(f"{TABLE} has {loaded} rows, expected {EXPECTED_ROWS}")
    print(f"loaded {TABLE} in {time.time() - start:.0f}s")


def run_cases(client, cases, rounds):
    runs = {name: [] for name, _ in cases}
    for r in range(rounds):
        for name, sql in cases:
            qid, rows, err = client.query(sql)
            record = client.details(qid) if qid else {}
            client.clear_spill()
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
    for name, _ in cases:
        ok = [run for run in runs[name] if run["ok"]]
        result[name] = {
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
        "--port", type=int, default=8000, help="HTTP port of databend-query"
    )
    parser.add_argument(
        "--databend-dir",
        default=".databend",
        help="working directory of the standalone deployment, for its query log",
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
    cases = read_cases()
    client = Client(args)
    qid, rows, err = client.query("SELECT version()")
    if err:
        sys.exit(f"SELECT version(): {err}")
    if not client.details(qid):
        sys.exit(
            f"query {qid} is not in {client.path(QUERY_DETAILS_DIR)}; "
            "run from the repository root of the deployment or pass --databend-dir"
        )
    version = rows[0][0]
    print(version)
    prepare_data(client, args.tpchgen)
    results = run_cases(client, cases, args.rounds)
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
        "cases": results,
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\n{'case':<16} {'secs':>8} {'peak MiB':>9} {'spill MiB':>9}")
    for name, case in results.items():
        if case["ok"]:
            print(
                f"{name:<16} {case['secs']:8.2f} {case['peak_mib']:9.0f} {case['spill_mib']:9.0f}"
            )
        else:
            print(f"{name:<16} FAILED {case['errors']}")
    print(f"\nwrote {args.output}, finished in {time.time() - started:.0f}s")
    if any(not case["ok"] for case in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
