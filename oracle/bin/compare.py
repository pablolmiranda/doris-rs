#!/usr/bin/env python3
"""Compare a candidate results JSON against a baseline and decide pass or fail.

Classification per suite:
    regression   baseline passed, candidate failed or incomplete
    missing      baseline passed, candidate did not run it
    fixed        baseline failed, candidate passed
    known        failed in both
    new          only in candidate
    same         passed in both

Exit status is 1 when there is at least one regression, or a missing suite unless
--allow-missing is given. Names listed in an --ignore file are reported but never fail the gate
(use it for suites known to be flaky on the reference itself).
"""
import argparse
import json
import sys


def load(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)["suites"]


def classify(baseline, candidate):
    report = {k: [] for k in ("regression", "missing", "fixed", "known", "new", "same")}
    for name, base in baseline.items():
        cand = candidate.get(name)
        base_ok = base["status"] == "passed"
        if cand is None:
            report["missing" if base_ok else "known"].append(name)
            continue
        cand_ok = cand["status"] == "passed"
        if base_ok and cand_ok:
            report["same"].append(name)
        elif base_ok and not cand_ok:
            report["regression"].append(name)
        elif not base_ok and cand_ok:
            report["fixed"].append(name)
        else:
            report["known"].append(name)
    for name in candidate:
        if name not in baseline:
            report["new"].append(name)
    for bucket in report.values():
        bucket.sort()
    return report


def gate(report, ignore, allow_missing):
    failing = [n for n in report["regression"] if n not in ignore]
    if not allow_missing:
        failing += [n for n in report["missing"] if n not in ignore]
    return failing


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline")
    parser.add_argument("candidate")
    parser.add_argument("--ignore", help="file with one suite name per line to never fail on")
    parser.add_argument("--allow-missing", action="store_true")
    parser.add_argument("--json", help="write the full report here")
    parser.add_argument("--max-list", type=int, default=50)
    args = parser.parse_args(argv[1:])

    ignore = set()
    if args.ignore:
        with open(args.ignore, encoding="utf-8") as fh:
            ignore = {l.split("#")[0].strip() for l in fh if l.split("#")[0].strip()}

    baseline, candidate = load(args.baseline), load(args.candidate)
    report = classify(baseline, candidate)
    failing = gate(report, ignore, args.allow_missing)

    for bucket in ("regression", "missing", "fixed", "new", "known", "same"):
        names = report[bucket]
        print(f"{bucket:11s} {len(names)}")
        if bucket in ("regression", "missing", "fixed", "new"):
            for name in names[: args.max_list]:
                mark = " (ignored)" if name in ignore else ""
                detail = ""
                if bucket == "regression":
                    msg = (candidate.get(name) or {}).get("message") or ""
                    detail = "  " + msg.splitlines()[0][:160] if msg else ""
                print(f"  {name}{mark}{detail}")
            if len(names) > args.max_list:
                print(f"  ... {len(names) - args.max_list} more")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump({"report": report, "failing": failing, "ignored": sorted(ignore)}, fh, indent=2)

    if failing:
        print(f"\nGATE FAILED: {len(failing)} suite(s) regressed or went missing")
        return 1
    print("\nGATE PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
