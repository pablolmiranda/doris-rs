"""Derive the reference products from recorded reference runs.

    derive.py OUT_DIR RUN_DIR [RUN_DIR...] [--scope epic1.scope.json] [--needs index.json]

Run 1 is the anchor: its replies are the expected replies. Writes into OUT_DIR:
  suites.txt      suites in scope that passed in every run (the gate list)
  flaky.txt       suites in scope that passed in some runs but not all, with the runs
  baseline.json   run 1's results for the gate suites
  replies/        <suite>.jsonl.gz, run 1's canonical exchanges
  marks.json      probe marks per suite, with the runs they came from
  traffic/        <suite>.bin.gz, run 1's FE<->BE messages per suite, and manifest.json
  reaches_be.txt  gate suites that sent the BE work (plan fragments, tasks, loads)
  ranking.json    unsupported features by the gate suites they block (with --needs)
"""
import argparse
import json
import os
import sys

import hashlib

import marks as probe_marks
import records
import replies


def passed_everywhere(runs, in_scope):
    keys = sorted(k for k in runs[0] if in_scope(k))
    gate, flaky = [], {}
    for k in keys:
        statuses = [r.get(k, {}).get("status") for r in runs]
        if all(s == "passed" for s in statuses):
            gate.append(k)
        elif any(s == "passed" for s in statuses):
            flaky[k] = [i + 1 for i, s in enumerate(statuses) if s != "passed"]
    return gate, flaky


BACKGROUND_METHODS = {"heartbeat", "report", "publish_topic_info", "get_dictionary_status"}
BE_WORK = ("exec_plan_fragment", "submit_tasks", "tablet_writer", "stream_load", "http")


def split_traffic(messages, wins):
    """{suite: [(header, data)]} plus a background count per method: messages outside every
    window, and the FE's periodic calls, belong to no suite."""
    by_suite, background = {}, {}
    for header, data in messages:
        suite = replies.owner(header.get("time_ms"), wins)
        if header.get("method") in BACKGROUND_METHODS or suite == replies.UNATTRIBUTED:
            background[header.get("method")] = background.get(header.get("method"), 0) + 1
        else:
            by_suite.setdefault(suite, []).append((header, data))
    return by_suite, background


def reaches_be(items):
    return any(str(h.get("method", "")).startswith(BE_WORK) or h.get("protocol") == "http" for h, _ in items)


def write_traffic(run_dir, out_dir, gate):
    """Per-suite corpus from the anchor run's message file; returns the suites reaching the BE."""
    path = os.path.join(run_dir, "traffic.bin.gz")
    if not os.path.exists(path):
        return []
    wins = replies.windows(replies.results(run_dir))
    by_suite, background = split_traffic(records.read(path), wins)
    corpus = os.path.join(out_dir, "traffic")
    os.makedirs(corpus, exist_ok=True)
    holes_path = os.path.join(run_dir, "traffic-holes.json")
    holes = replies.load_json(holes_path) if os.path.exists(holes_path) else []
    # Capture holes: messages after a hole on that connection direction are missing from the
    # corpus, so the needs of the suites running then may be incomplete.
    manifest = {"source": os.path.basename(os.path.normpath(run_dir)), "background": background,
                "capture_holes": holes, "suites": {}}
    for k in gate:
        items = by_suite.get(k, [])
        if not items:
            continue
        target = os.path.join(corpus, f"{k}.bin.gz")
        records.write(target, items)
        with open(target, "rb") as fh:
            manifest["suites"][k] = {"messages": len(items), "sha256": hashlib.sha256(fh.read()).hexdigest()}
    with open(os.path.join(corpus, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=1, sort_keys=True)
    return [k for k in gate if reaches_be(by_suite.get(k, []))]


def directory_loads(suites):
    """{suite: [load suites of its directory]}: a directory's `load` suites create the tables
    its other suites query, so their needs are the suite's needs too."""
    loads = {}
    for k in suites:
        directory, _, name = k.rpartition(".")
        if name == "load":
            loads.setdefault(directory.rpartition(".")[0], []).append(k)
    return {k: [l for l in loads.get(k.rpartition(".")[0].rpartition(".")[0], []) if l != k] for k in suites}


def rank(index, supported, suites):
    """Unsupported features, ranked by the suites each one alone blocks, then by all it blocks."""
    loads = directory_loads(suites)
    blockers = {}
    for k in suites:
        needs = set()
        for source in [k, *loads[k]]:
            needs |= {f"{kind}:{v}" for kind, values in index.get(source, {}).items() for v in values}
        blockers[k] = needs - supported
    features = {f for b in blockers.values() for f in b}
    out = [{"feature": f, "alone": sum(1 for b in blockers.values() if b == {f}),
            "total": sum(1 for b in blockers.values() if f in b)} for f in features]
    return sorted(out, key=lambda r: (-r["alone"], -r["total"], r["feature"]))


def write_lines(path, lines):
    with open(path, "w", encoding="utf-8") as fh:
        for line in lines:
            fh.write(line + "\n")


def derive(run_dirs, out_dir, scope=None, needs_index=None, supported=frozenset()):
    os.makedirs(out_dir, exist_ok=True)
    runs = [replies.results(d) for d in run_dirs]

    def in_scope(key):
        return scope is None or not scope.get(key, {"reasons": ["unknown"]})["reasons"]

    gate, flaky = passed_everywhere(runs, in_scope)
    write_lines(os.path.join(out_dir, "suites.txt"), gate)
    write_lines(os.path.join(out_dir, "flaky.txt"),
                [f"{k}  # flaky on reference: failed in runs {', '.join(map(str, r))}" for k, r in flaky.items()])
    with open(os.path.join(out_dir, "baseline.json"), "w", encoding="utf-8") as fh:
        json.dump({"meta": {"runs": [os.path.basename(os.path.normpath(d)) for d in run_dirs]},
                   "suites": {k: runs[0][k] for k in gate}}, fh, indent=2, sort_keys=True)

    exchanges = [replies.run_exchanges(d) for d in run_dirs]
    marks = {}
    for k in gate:
        replies.write_suite(os.path.join(out_dir, "replies", f"{k}.jsonl.gz"), exchanges[0].get(k, []))
        marks[k] = probe_marks.derive_marks([e.get(k, []) for e in exchanges])
    with open(os.path.join(out_dir, "marks.json"), "w", encoding="utf-8") as fh:
        json.dump({"runs": [os.path.basename(os.path.normpath(d)) for d in run_dirs], "suites": marks},
                  fh, indent=1, sort_keys=True)
    be = write_traffic(run_dirs[0], out_dir, gate)
    write_lines(os.path.join(out_dir, "reaches_be.txt"), be)
    if needs_index is not None:
        with open(os.path.join(out_dir, "ranking.json"), "w", encoding="utf-8") as fh:
            json.dump(rank(needs_index, set(supported), gate), fh, indent=1)
    return {"gate": len(gate), "flaky": len(flaky), "reaches_be": len(be)}


def main(argv):
    parser = argparse.ArgumentParser(description="Derive the Epic 1 reference products from reference runs.")
    parser.add_argument("out_dir")
    parser.add_argument("run_dirs", nargs="+")
    parser.add_argument("--scope")
    parser.add_argument("--needs")
    parser.add_argument("--supported", help="JSON {\"features\": [...]} of features doris-rs supports")
    args = parser.parse_args(argv[1:])
    scope = replies.load_json(args.scope) if args.scope else None
    needs = replies.load_json(args.needs) if args.needs else None
    supported = set(replies.load_json(args.supported)["features"]) if args.supported else set()
    print(json.dumps(derive(args.run_dirs, args.out_dir, scope, needs, supported)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
