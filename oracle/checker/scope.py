"""Which upstream suites Epic 1 owns, by static rules over the suite sources.

    scope.py SUITES_ROOT SUITE_LIST OUT_JSON OUT_DIRS OUT_EXCLUSIONS

SUITES_ROOT is regression-test/suites of the pinned source; SUITE_LIST the p0-core directory
list. A suite's text is its .groovy file plus every .sql file in its directory tree (upstream
keeps DDL there). OUT_JSON maps each suite key (the results.json format) to its file and the
rules it trips; an empty list means in scope. OUT_DIRS lists the directories with at least one
in-scope suite, for the runner. OUT_EXCLUSIONS is the exclusion report: the count per rule and
the suites each rule excluded.
"""
import json
import os
import re
import sys

I = re.I | re.S
RULES = [
    ("variant", re.compile(r"\bVARIANT\b(?!\s*\()", I)),
    ("text_index", re.compile(r"USING\s+(INVERTED|NGRAM_BF)\b|\bMATCH_(ANY|ALL|PHRASE\w*|REGEXP)\b", I)),
    ("agg_types", re.compile(r"\b(BITMAP|HLL|QUANTILE_STATE)\b(?!\s*\()|\bAGG_STATE\s*<"
                             r"|\b(to_bitmap|bitmap_\w+|hll_\w+|quantile_\w+)\s*\(", I)),
    ("aggregate_key", re.compile(r"\bAGGREGATE\s+KEY\b", I)),
    ("complex_types", re.compile(r"\b(ARRAY|MAP|STRUCT)\s*<|\b\w+`?\s+JSONB?\b(?=\s*(NULL|NOT|COMMENT|DEFAULT|,|\)))", I)),
    ("arrow_flight", re.compile(r"arrow_flight_sql", I)),
    ("ai_functions", re.compile(r"\bAI_\w+\s*\(|['\"]type['\"]\s*=\s*['\"]ai['\"]", I)),
    ("vector", re.compile(r"USING\s+ANN\b|\b\w*(l2_distance|cosine_distance|inner_product)\w*\s*\(", I)),
    ("external_catalog", re.compile(r"\bCREATE\s+(EXTERNAL\s+)?CATALOG\b", I)),
    ("debug_points", re.compile(r"DebugPoint", I)),
]
SUITE_NAME = re.compile(r"""\bsuite\s*\(\s*["']([^"']+)["']""")


def exclusion_reasons(text):
    return sorted({name for name, rule in RULES if rule.search(text)})


def read(path):
    with open(path, encoding="utf-8", errors="replace") as fh:
        return fh.read()


def suite_key(groovy_path, root):
    rel = os.path.relpath(groovy_path, root)
    match = SUITE_NAME.search(read(groovy_path))
    if not match:
        return None
    return ".".join([*os.path.dirname(rel).split(os.sep), os.path.splitext(os.path.basename(rel))[0], match.group(1)])


def is_sql_suite(path):
    return path.endswith(".sql") and "sql" in path.split(os.sep)[:-1]


def sql_suite_key(path, root):
    """The framework runs every .sql file under a sql/ directory as a suite named after it."""
    rel = os.path.relpath(path, root)
    stem = os.path.splitext(os.path.basename(rel))[0]
    return ".".join([*os.path.dirname(rel).split(os.sep), stem, stem])


def named_sql(groovy_text, sql_files):
    """Text of the DDL .sql files (outside sql/ directories) the suite names by file stem."""
    return "".join(read(p) for p in sql_files
                   if re.search(r"\b" + re.escape(os.path.splitext(os.path.basename(p))[0]) + r"\b", groovy_text))


def scan(suites_root, dirs):
    out = {}
    for d in dirs:
        for here, _, files in os.walk(os.path.join(suites_root, d)):
            ddl = [os.path.join(sub, f) for sub, _, fs in os.walk(here) for f in sorted(fs)
                   if f.endswith(".sql") and not is_sql_suite(os.path.join(sub, f))]
            for f in sorted(files):
                path = os.path.join(here, f)
                rel = os.path.relpath(path, suites_root)
                if is_sql_suite(path):
                    out[sql_suite_key(path, suites_root)] = {"file": rel, "reasons": exclusion_reasons(read(path))}
                elif f.endswith(".groovy"):
                    key = suite_key(path, suites_root)
                    if key:
                        text = read(path)
                        out[key] = {"file": rel, "reasons": exclusion_reasons(text + named_sql(text, ddl))}
    return out


def exclusion_report(result):
    by_reason = {}
    for key, v in sorted(result.items()):
        for r in v["reasons"]:
            by_reason.setdefault(r, []).append(key)
    lines = [f"in scope: {sum(1 for v in result.values() if not v['reasons'])} of {len(result)} suites",
             "# a suite can be excluded by several rules; it is listed under each"]
    for r, keys in sorted(by_reason.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        lines.append(f"{r}: {len(keys)}")
        lines += [f"  {k}" for k in keys]
    return lines


def main(argv):
    if len(argv) != 6:
        print(__doc__, file=sys.stderr)
        return 2
    root, suite_list, out_json, out_dirs, out_exclusions = argv[1:]
    with open(suite_list, encoding="utf-8") as fh:
        dirs = [line.strip() for line in fh if line.strip() and not line.startswith("#")]
    result = scan(root, dirs)
    with open(out_json, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=1, sort_keys=True)
    keep_dirs = [d for d in dirs if any(not v["reasons"] and v["file"].startswith(d + "/") for v in result.values())]
    with open(out_dirs, "w", encoding="utf-8") as fh:
        fh.write("# p0-core directories with at least one Epic 1 suite (computed by checker/scope.py)\n")
        fh.write("".join(d + "\n" for d in keep_dirs))
    report = exclusion_report(result)
    with open(out_exclusions, "w", encoding="utf-8") as fh:
        fh.write("\n".join(report) + "\n")
    print("\n".join(line for line in report if not line.startswith("  ")))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
