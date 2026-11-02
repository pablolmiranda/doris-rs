#!/usr/bin/env python3
"""Summarize failures in a results.json: counts per top-level suite directory and the most
common server error phrases, so a baseline can be sanity-checked without reading the raw log.

usage: triage.py RESULTS.json [--top N]
"""
import argparse
import collections
import json
import re
import sys

ERROR_PATTERNS = [
    r"errCode = \d+, detailMessage = [^\n]{0,110}",
    r"Connection refused[^\n]{0,40}",
    r"Communications link failure[^\n]{0,40}",
    r"Unknown (?:table|database|column)[^\n]{0,60}",
    r"[Tt]imeout[^\n]{0,60}",
    r"The OSS Access Key Id you provide[^\n]{0,40}",
    r"Access Denied[^\n]{0,60}",
    r"java\.[a-zA-Z.]*Exception[^\n]{0,80}",
]
ERRORS = re.compile("|".join(f"(?:{p})" for p in ERROR_PATTERNS))


def normalize(text):
    text = re.sub(r"\b[0-9a-f]{12,}\b", "<hex>", text)
    text = re.sub(r"\d+", "N", text)
    return text.strip()


def triage(suites, top):
    by_dir_total = collections.Counter()
    by_dir_failed = collections.Counter()
    phrases = collections.Counter()
    for name, entry in suites.items():
        top_dir = name.split(".")[0]
        by_dir_total[top_dir] += 1
        if entry["status"] != "passed":
            by_dir_failed[top_dir] += 1
            blob = (entry.get("message") or "") + "\n" + (entry.get("details") or "")
            found = ERRORS.findall(blob)
            phrases[normalize(found[0]) if found else "<no recognizable server error>"] += 1
    lines = ["failures by directory:"]
    for d in sorted(by_dir_total):
        lines.append(f"  {d:30s} {by_dir_failed[d]:5d} / {by_dir_total[d]:5d}")
    lines.append("top error phrases:")
    for phrase, n in phrases.most_common(top):
        lines.append(f"  {n:5d}  {phrase}")
    return "\n".join(lines)


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("results")
    ap.add_argument("--top", type=int, default=12)
    args = ap.parse_args(argv[1:])
    with open(args.results, encoding="utf-8") as fh:
        suites = json.load(fh)["suites"]
    print(triage(suites, args.top))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
