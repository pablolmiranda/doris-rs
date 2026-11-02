#!/usr/bin/env python3
"""Turn the regression framework's TeamCity service messages into a results JSON.

Input: one or more log files produced by `run-regression-test.sh --teamcity`.
Output: JSON with one entry per suite:
    {"meta": {...}, "suites": {"<suite>": {"status": "passed|failed|incomplete",
                                          "duration_ms": int|null, "message": str|null}}}

A suite that started but never finished is "incomplete" (the runner or server died).
"""
import json
import re
import sys

MESSAGE = re.compile(r"##teamcity\[(\w+)\s*(.*)\]\s*$")
ATTR = re.compile(r"(\w+)='((?:[^'|]|\|.)*)'")
SUMMARY = re.compile(
    r"Test (\d+) suites, failed (\d+) suites, fatal (\d+) scripts, skipped (\d+) scripts"
)
UNESCAPE = {"|": "|", "'": "'", "n": "\n", "r": "\r", "[": "[", "]": "]"}


def unescape(value):
    out = []
    i = 0
    while i < len(value):
        ch = value[i]
        if ch == "|" and i + 1 < len(value):
            nxt = value[i + 1]
            if nxt in UNESCAPE:
                out.append(UNESCAPE[nxt])
                i += 2
                continue
            if nxt == "0" and i + 5 < len(value) and value[i + 2] == "x":
                out.append(chr(int(value[i + 3 : i + 7], 16)))
                i += 7
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def parse_message(line):
    match = MESSAGE.search(line)
    if not match:
        return None
    kind, body = match.group(1), match.group(2)
    attrs = {key: unescape(val) for key, val in ATTR.findall(body)}
    return kind, attrs


def parse_lines(lines):
    suites = {}
    meta = {"summary": None}
    for line in lines:
        summary = SUMMARY.search(line)
        if summary:
            total, failed, fatal, skipped = (int(x) for x in summary.groups())
            meta["summary"] = {
                "total": total,
                "failed": failed,
                "fatal_scripts": fatal,
                "skipped": skipped,
            }
        parsed = parse_message(line)
        if not parsed:
            continue
        kind, attrs = parsed
        name = attrs.get("name")
        if not name:
            continue
        entry = suites.setdefault(
            name, {"status": "incomplete", "duration_ms": None, "message": None}
        )
        if kind == "testFailed":
            entry["status"] = "failed"
            entry["message"] = attrs.get("message") or ""
        elif kind == "testFinished":
            if entry["status"] != "failed":
                entry["status"] = "passed"
            duration = attrs.get("duration")
            entry["duration_ms"] = int(duration) if duration and duration.isdigit() else None
    counts = {"passed": 0, "failed": 0, "incomplete": 0}
    for entry in suites.values():
        counts[entry["status"]] += 1
    meta["counts"] = counts
    return {"meta": meta, "suites": suites}


def main(argv):
    if len(argv) < 3:
        print("usage: parse_teamcity.py OUTPUT.json LOG [LOG...]", file=sys.stderr)
        return 2
    out_path, logs = argv[1], argv[2:]
    lines = []
    for path in logs:
        with open(path, encoding="utf-8", errors="replace") as fh:
            lines.extend(fh.readlines())
    result = parse_lines(lines)
    result["meta"]["sources"] = logs
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2, sort_keys=True)
    counts = result["meta"]["counts"]
    print(
        f"parsed {len(result['suites'])} suites: "
        f"{counts['passed']} passed, {counts['failed']} failed, {counts['incomplete']} incomplete"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
