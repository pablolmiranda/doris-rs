"""Rank the features doris-rs lacks by the Epic 1 suites they block.

    rank.py REFERENCE_DIR [--supported scores/epic1-supported.json] [--top N]

Reads REFERENCE_DIR/needs.json (doris-needs over the traffic corpus) and suites.txt, writes
ranking.json and prints the top of it. "alone" counts suites blocked by that feature only;
"total" every suite that needs it. A suite's needs include its directory's load suites'.
"""
import argparse
import json
import os
import sys

import derive
import grade


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("reference_dir")
    parser.add_argument("--supported", default=os.path.join(os.path.dirname(__file__), "..", "scores", "epic1-supported.json"))
    parser.add_argument("--top", type=int, default=15)
    args = parser.parse_args(argv[1:])
    with open(os.path.join(args.reference_dir, "needs.json"), encoding="utf-8") as fh:
        index = json.load(fh)
    with open(args.supported, encoding="utf-8") as fh:
        supported = set(json.load(fh)["features"])
    suites = grade.read_list(os.path.join(args.reference_dir, "suites.txt"))
    ranking = derive.rank(index, supported, suites)
    with open(os.path.join(args.reference_dir, "ranking.json"), "w", encoding="utf-8") as fh:
        json.dump(ranking, fh, indent=1)
    print(f"{'feature':45} {'alone':>6} {'total':>6}")
    for r in ranking[:args.top]:
        print(f"{r['feature']:45} {r['alone']:6} {r['total']:6}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
