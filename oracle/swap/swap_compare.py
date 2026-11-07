#!/usr/bin/env python3
"""Compares two swap-check snapshots: what the FE showed before a swap and after it.

A snapshot directory holds `tablets-<db.table>.tsv` (SHOW TABLETS, with a header row),
`backends.tsv` (SHOW BACKENDS, with a header row) and `tasks.log` (the incoming BE's log since
it started, from which the task types it received are read). Usage:
    swap_compare.py BEFORE_DIR AFTER_DIR
prints each difference and exits 1 if there is one."""
import csv
import os
import re
import sys

# Clocks and counters that move on their own.
TABLET_IGNORED = {"LstFailedTime", "LstConsistencyCheckTime", "QueryHits"}
# Clocks, the binary's version, and the deployment facts of clusters/rust.facts.json (each
# binary sees the host's CPUs, memory and disk its own way).
BACKEND_IGNORED = {
    "LastStartTime", "LastHeartbeat", "LastUpdateTime", "LastReportTime", "LiveSince", "Version", "CpuCores",
    "Memory", "TotalCapacity", "AvailCapacity", "DataUsedCapacity", "TrashUsedCapacity", "UsedPct",
    "MaxDiskUsedPct", "RemoteUsedCapacity", "Status",
}
# Tasks the FE sends to repair a backend or move its tablets.
REPAIRS = ("DROP", "CLONE")

RS_TASK = re.compile(r"doris-rs be: task (\w+) signature=")
CPP_TASK = re.compile(r"successfully submit task\|type=(\w+)\|")


def tasks_in_log(text):
    out = []
    for line in text.splitlines():
        m = RS_TASK.search(line) or CPP_TASK.search(line)
        if m:
            out.append(m.group(1))
    return out


def read_tsv(path):
    with open(path, encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def load(directory):
    tablets = {}
    for name in sorted(os.listdir(directory)):
        if name.startswith("tablets-") and name.endswith(".tsv"):
            tablets[name[len("tablets-"):-len(".tsv")]] = read_tsv(os.path.join(directory, name))
    backends = read_tsv(os.path.join(directory, "backends.tsv"))
    log = os.path.join(directory, "tasks.log")
    tasks = tasks_in_log(open(log, encoding="utf-8", errors="replace").read()) if os.path.exists(log) else []
    return {"tablets": tablets, "backend": backends[0] if backends else {}, "tasks": tasks}


def differences(before, after):
    out = []
    for table in sorted(set(before["tablets"]) | set(after["tablets"])):
        a = {r["TabletId"]: r for r in before["tablets"].get(table, [])}
        b = {r["TabletId"]: r for r in after["tablets"].get(table, [])}
        for tid in sorted(set(a) | set(b)):
            if tid not in a or tid not in b:
                out.append(f"{table}: TabletId {tid} {'appeared' if tid not in a else 'is gone'}")
                continue
            for field in sorted(set(a[tid]) | set(b[tid])):
                if field in TABLET_IGNORED or a[tid].get(field) == b[tid].get(field):
                    continue
                out.append(f"{table}: tablet {tid} {field}: {a[tid].get(field)!r} -> {b[tid].get(field)!r}")
    for field in sorted(set(before["backend"]) | set(after["backend"])):
        if field in BACKEND_IGNORED or before["backend"].get(field) == after["backend"].get(field):
            continue
        out.append(f"backend {field}: {before['backend'].get(field)!r} -> {after['backend'].get(field)!r}")
    for task in after["tasks"]:
        if task in REPAIRS:
            out.append(f"the FE sent a {task} task")
    return out


def main(argv):
    diffs = differences(load(argv[1]), load(argv[2]))
    for d in diffs:
        print(d)
    return 1 if diffs else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
