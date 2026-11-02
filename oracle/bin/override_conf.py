#!/usr/bin/env python3
"""Copy the upstream regression-conf.groovy and override selected keys.

usage: override_conf.py BASE OUTPUT KEY=VALUE [KEY=VALUE ...]

VALUE is written verbatim as a Groovy expression, so strings need their own quotes:
    jdbcUrl='"jdbc:mysql://doris:9030/?useLocalSessionState=true"'  excludeDockerTest=true
Starting from the upstream file (rather than a hand-written template) keeps every other option
at the value the suites were written against, even when the pinned release changes.
"""
import re
import sys


def apply_overrides(text, overrides):
    lines = text.splitlines()
    seen = set()
    for i, line in enumerate(lines):
        m = re.match(r"^(\s*)([A-Za-z_][A-Za-z0-9_]*)\s*=", line)
        if not m:
            continue
        key = m.group(2)
        if key in overrides and key not in seen:
            lines[i] = f"{m.group(1)}{key} = {overrides[key]}"
            seen.add(key)
    missing = [k for k in overrides if k not in seen]
    if missing:
        lines.append("")
        lines.append("// added by doris-rs oracle harness")
        for key in missing:
            lines.append(f"{key} = {overrides[key]}")
    return "\n".join(lines) + "\n"


def parse_pairs(pairs):
    overrides = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or not key:
            raise ValueError(f"bad override (want KEY=VALUE): {pair!r}")
        overrides[key.strip()] = value
    return overrides


def main(argv):
    if len(argv) < 4:
        print(__doc__, file=sys.stderr)
        return 2
    base, out, pairs = argv[1], argv[2], argv[3:]
    with open(base, encoding="utf-8") as fh:
        text = fh.read()
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(apply_overrides(text, parse_pairs(pairs)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
