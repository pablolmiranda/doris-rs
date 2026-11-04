"""Grade one recorded candidate run against the derived reference.

    grade.py RUN_DIR REFERENCE_DIR SCORES_DIR [--json OUT]

Two gates over the reference's gate suites: the suite's own result (.out), and byte-equal MySQL
replies under the reference's probe marks (strict; only for suites that passed .out). The
ratchet files in SCORES_DIR (epic1-out.txt, epic1-strict.txt) list suites that must keep
passing; the exit status is 1 when one of them does not.
"""
import argparse
import json
import os
import sys

import replies

GATES = ("out", "strict")


def read_list(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return [line.split("#")[0].strip() for line in fh if line.split("#")[0].strip()]


def numbers(passing, gate, reaches_be):
    passing, be = set(passing), set(reaches_be) & set(gate)
    return {"passing": len(passing), "total": len(gate),
            "passing_be": len(passing & be), "total_be": len(be)}


def grade(run_dir, reference_dir, scores_dir):
    gate = read_list(os.path.join(reference_dir, "suites.txt"))
    reaches_be = read_list(os.path.join(reference_dir, "reaches_be.txt"))
    marks = replies.load_json(os.path.join(reference_dir, "marks.json"))["suites"]
    results = replies.results(run_dir)

    out_passing = [k for k in gate if results.get(k, {}).get("status") == "passed"]
    actual = replies.run_exchanges(run_dir)
    strict_passing, differences, not_compared = [], {}, {}
    for k in out_passing:
        mark = marks.get(k, {"status": "no_replies", "exchanges": {}})
        if mark["status"] != "stable":
            not_compared.setdefault(mark["status"], []).append(k)
            continue
        expected = replies.read_suite(os.path.join(reference_dir, "replies", f"{k}.jsonl.gz"))
        diff = replies.compare(expected, mark, actual.get(k, []))
        if diff is None:
            strict_passing.append(k)
        else:
            differences[k] = diff

    passing = {"out": out_passing, "strict": strict_passing}
    ratchet = {"lost": {}, "new": {}}
    for g in GATES:
        listed = read_list(os.path.join(scores_dir, f"epic1-{g}.txt"))
        ratchet["lost"][g] = [k for k in listed if k not in passing[g]]
        ratchet["new"][g] = [k for k in passing[g] if k not in listed]
    return {
        "out": {"passing": out_passing, "numbers": numbers(out_passing, gate, reaches_be)},
        "strict": {"passing": strict_passing, "differences": differences, "not_compared": not_compared,
                   "numbers": numbers(strict_passing, gate, reaches_be)},
        "ratchet": ratchet,
        "ok": not any(ratchet["lost"].values()),
    }


def report(verdict):
    lines = []
    for g in GATES:
        n = verdict[g]["numbers"]
        lines.append(f"{g:6}: passing {n['passing']} of {n['total']} epic suites; "
                     f"{n['passing_be']} of {n['total_be']} that reach the BE")
    for k, d in sorted(verdict["strict"]["differences"].items()):
        lines.append(f"strict difference in {k} at exchange {d['index']} ({d['field']}): {d['statement']}")
        lines.append(f"    expected: {json.dumps(d['expected'])[:300]}")
        lines.append(f"    actual:   {json.dumps(d['actual'])[:300]}")
    for status, suites in sorted(verdict["strict"]["not_compared"].items()):
        lines.append(f"not compared ({status} on the reference): {len(suites)} suites")
    for g in GATES:
        for k in verdict["ratchet"]["lost"][g]:
            lines.append(f"RATCHET {g}: {k} is listed but no longer passes")
        if verdict["ratchet"]["new"][g]:
            lines.append(f"new {g} passes not yet in scores/epic1-{g}.txt: {len(verdict['ratchet']['new'][g])}")
    return "\n".join(lines)


def main(argv):
    parser = argparse.ArgumentParser(description="Grade a recorded run on the suite and strict gates.")
    parser.add_argument("run_dir")
    parser.add_argument("reference_dir")
    parser.add_argument("scores_dir")
    parser.add_argument("--json")
    args = parser.parse_args(argv[1:])
    verdict = grade(args.run_dir, args.reference_dir, args.scores_dir)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(verdict, fh, indent=1, sort_keys=True)
    print(report(verdict))
    return 0 if verdict["ok"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
