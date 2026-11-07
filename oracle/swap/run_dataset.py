#!/usr/bin/env python3
"""Runs the M4 load dataset and the readback queries through the swap cluster's FE.

    run_dataset.py load <script.sql> <data dir>    statements, and stream loads via curl in the FE container
    run_dataset.py query <queries.sql> <out dir>   one reply per query, <out dir>/<name>.tsv

A stream load is a comment line in the script:
    -- stream_load <db>.<table> <file> <header>=<value> ...
Its body goes on stdin to curl inside the FE container, which follows the FE's redirect to the
backend, so the host never needs to reach the backend's address.
"""
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

FE = "doris-oracle-fe-only"
MYSQL = ["docker", "exec", "-i", FE, "mysql", "-uroot", "-h127.0.0.1", "-P9030", "--batch"]


@dataclass
class Step:
    kind: str  # "sql" or "stream_load"
    sql: str = ""
    file: str = ""
    argv: list = field(default_factory=list)


def parse_line(line: str, n: int) -> Step:
    """A `-- stream_load` line; `n` makes its label unique within the script."""
    parts = line.split()
    assert parts[:2] == ["--", "stream_load"], line
    target, file, headers = parts[2], parts[3], parts[4:]
    db, table = target.split(".")
    argv = ["curl", "-sS", "--location-trusted", "-u", "root:", "-H", "Expect:100-continue",
            "-H", f"label:{db}_{table}_{n}"]
    for h in headers:
        k, v = h.split("=", 1)
        argv += ["-H", f"{k}:{v}"]
    argv += ["-T", "-", f"http://127.0.0.1:8030/api/{db}/{table}/_stream_load"]
    return Step("stream_load", file=file, argv=argv)


def parse_script(text: str) -> list:
    steps, buf, quote = [], [], None
    for n, line in enumerate(text.splitlines(), 1):
        if not "".join(buf).strip():
            buf = []
        if not buf and line.strip().startswith("-- stream_load"):
            steps.append(parse_line(line.strip(), n))
            continue
        if not buf and (not line.strip() or line.strip().startswith("--")):
            continue
        for ch in line + "\n":
            if quote:
                buf.append(ch)
                if ch == quote:
                    quote = None
            elif ch in "'\"":
                quote = ch
                buf.append(ch)
            elif ch == ";":
                steps.append(Step("sql", sql=" ".join("".join(buf).split())))
                buf = []
            else:
                buf.append(ch)
    assert not "".join(buf).strip(), "unterminated statement"
    return steps


def parse_queries(text: str) -> list:
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, q = line.split(":", 1)
        out.append((name.strip(), q.strip()))
    return out


def load(script: Path, data: Path) -> None:
    for step in parse_script(script.read_text()):
        if step.kind == "sql":
            subprocess.run(MYSQL + ["-e", step.sql], check=True)
            continue
        body = (data / step.file).read_bytes()
        out = subprocess.run(["docker", "exec", "-i", FE] + step.argv, input=body, capture_output=True, check=True)
        reply = out.stdout.decode()
        if '"Status": "Success"' not in reply:
            sys.exit(f"stream load of {step.file} failed: {reply}")


def query(queries: Path, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    for name, q in parse_queries(queries.read_text()):
        res = subprocess.run(MYSQL + ["-e", q], capture_output=True, check=True)
        (out / f"{name}.tsv").write_bytes(res.stdout)


if __name__ == "__main__":
    mode, a, b = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
    {"load": load, "query": query}[mode](a, b)
