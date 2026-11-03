"""From a recorded run to per-suite canonical exchanges, and the strict comparison of two.

A run directory holds results.json (suite status and time window), tap/*.jsonl (the MySQL tap's
logs) and facts.json (the serving BE's deployment facts). Exchanges are linked to the suite
whose window contains their send time, polling is collapsed, connections are numbered by first
appearance in the suite, and declared deployment facts are replaced by placeholders.
"""
import gzip
import json
import os

import marks as probe_marks
import mysql_wire
import tap

UNATTRIBUTED = "unattributed"


def load_json(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def results(run_dir):
    return load_json(os.path.join(run_dir, "results.json"))["suites"]


def facts(run_dir):
    path = os.path.join(run_dir, "facts.json")
    return load_json(path) if os.path.exists(path) else {"facts": []}


def windows(suites):
    """{suite: (started_ms, finished_ms)} for suites with both times; overlapping windows are an
    error, because the checker's runs are serial and overlap would mean guessing."""
    out = {k: (v["started_ms"], v["finished_ms"]) for k, v in suites.items()
           if v.get("started_ms") is not None and v.get("finished_ms") is not None}
    ordered = sorted(out.items(), key=lambda kv: kv[1])
    for (a, (_, a_end)), (b, (b_start, _)) in zip(ordered, ordered[1:]):
        if b_start < a_end:
            raise ValueError(f"suite windows overlap: {a} and {b}; checker runs must be serial")
    return out


def owner(t_ms, wins):
    if t_ms is None:
        return UNATTRIBUTED
    for key, (start, end) in wins.items():
        if start <= t_ms <= end:
            return key
    return UNATTRIBUTED


def identity(ex):
    return (ex["command"], ex["statement"], json.dumps(ex["params"], sort_keys=True))


def collapse(exchanges):
    """A poll is a run of identical commands on one connection with nothing else sent on that
    connection in between; it keeps only its last exchange, in the place of the first, because
    the reply that ended the poll is the one that matters."""
    out, last = [], {}
    for ex in exchanges:
        i = last.get(ex["conn"])
        if i is not None and identity(out[i]) == identity(ex):
            out[i] = ex
        else:
            last[ex["conn"]] = len(out)
            out.append(ex)
    return out


def plugin_lookup(exchanges):
    """The regression framework's query-timeout plugin opens a connection on a timer, whichever
    suite is running, to look up the backends: nothing but the driver's setup and
    `show backends`."""
    statements = [ex["statement"] for ex in exchanges if ex["command"] == "COM_QUERY"]
    setup = [st for st in statements[:-1] if st.startswith(("/* mysql-connector-java", "SET "))]
    return bool(statements) and statements[-1] == "show backends" and len(setup) == len(statements) - 1


def run_exchanges(run_dir):
    """{suite: [exchange]} for one run, facts substituted, with UNATTRIBUTED for the rest.

    A connection belongs to the suite whose window it was opened in. What it sends outside that
    window belongs to no suite: a thread a suite leaves running keeps polling into the windows
    of the suites after it, a different number of times in every run."""
    wins = windows(results(run_dir))
    run_facts = facts(run_dir)
    by_suite = {}
    tap_dir = os.path.join(run_dir, "tap")
    for name in sorted(os.listdir(tap_dir)) if os.path.isdir(tap_dir) else []:
        c2s, s2c, times = tap.read_log(os.path.join(tap_dir, name))
        # Commands without a reply say nothing about the server, and whether they land inside
        # the suite's window is timing.
        exchanges = [ex for ex in mysql_wire.decode_session(c2s, s2c, times) if ex["command"] not in mysql_wire.NO_REPLY]
        if not exchanges or plugin_lookup(exchanges):
            continue
        opener = owner(exchanges[0]["t_ms"], wins)
        for ex in exchanges:
            ex["conn"] = name
        inside = [ex for ex in exchanges if owner(ex["t_ms"], wins) == opener]
        outside = [ex for ex in exchanges if owner(ex["t_ms"], wins) != opener]
        by_suite.setdefault(opener, []).append(inside)
        if outside:
            by_suite.setdefault(UNATTRIBUTED, []).append(outside)
    out = {}
    for suite, connections in by_suite.items():
        # Each connection's own sequence is deterministic; when connections open and how they
        # interleave is not, so a suite's connections are ordered by what they sent.
        connections = sorted((collapse(c) for c in connections),
                             key=lambda c: json.dumps([identity(ex) for ex in c]))
        exchanges = []
        for n, conn in enumerate(connections):
            for ex in conn:
                ex["conn"] = n
                ex.pop("t_ms", None)
                exchanges.append(substitute(ex, run_facts))
        out[suite] = exchanges
    return out


COLUMN_TEXT_FIELDS = ("catalog", "schema", "table", "org_table", "name", "org_name")


def fact_rules(run_facts):
    """[(match, value bytes, placeholder bytes, columns or None)], longest values first so that a
    value never replaces part of a longer one. A cell fact with "columns" applies only to row
    values in columns of those names."""
    rules = [(f["match"], v.encode(), f"<{f['name']}>".encode(), f.get("columns"))
             for f in run_facts.get("facts", []) for v in f["values"] if v]
    return sorted(rules, key=lambda r: -len(r[1]))


def sub_bytes(value, rules, column=None):
    for match, v, placeholder, columns in rules:
        if match == "cell" and value == v and (columns is None or column in columns):
            return placeholder
    for match, v, placeholder, _ in rules:
        if match == "substring":
            value = value.replace(v, placeholder)
    return value


def sub_text(value, rules):
    if value is None:
        return None
    return sub_bytes(value.encode("utf-8", errors="surrogateescape"), rules).decode("utf-8", errors="surrogateescape")


def sub_reply(reply, rules):
    if not isinstance(reply, dict):
        return reply
    out = dict(reply)
    if "rows" in out and out.get("protocol") != "binary":
        names = [c.get("name") for c in out.get("columns", [])]
        out["rows"] = [[None if v is None else sub_bytes(bytes.fromhex(v), rules, names[j] if j < len(names) else None).hex()
                        for j, v in enumerate(row)] for row in out["rows"]]
    for key in ("message", "info"):
        if isinstance(out.get(key), str):
            out[key] = sub_text(out[key], rules)
    if "columns" in out:
        out["columns"] = [{k: sub_text(v, rules) if k in COLUMN_TEXT_FIELDS else v for k, v in c.items()} for c in out["columns"]]
    for key in ("parts",):
        if key in out:
            out[key] = [sub_reply(p, rules) for p in out[key]]
    for key in ("then", "login", "error"):
        if key in out:
            out[key] = sub_reply(out[key], rules)
    return out


def substitute(exchange, run_facts):
    """The exchange with every declared deployment fact replaced by its placeholder: a "cell"
    fact replaces a whole row value equal to it, a "substring" fact every occurrence in row
    values, messages, info, column names and the statement text."""
    rules = fact_rules(run_facts)
    if not rules:
        return exchange
    out = dict(exchange)
    out["statement"] = sub_text(out.get("statement"), [r for r in rules if r[0] == "substring"])
    out["reply"] = sub_reply(out.get("reply"), rules)
    return out


def write_suite(path, exchanges):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for ex in exchanges:
            fh.write(json.dumps(ex, sort_keys=True) + "\n")


def read_suite(path):
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


FIELD_ORDER = ("type", "code", "sqlstate", "message", "columns", "rows", "affected_rows",
               "last_insert_id", "status", "warnings", "info", "parts", "then", "file", "params")


def first_field(want, got, prefix=""):
    """Name of the first field where two canonical values differ, or None."""
    if isinstance(want, dict) and isinstance(got, dict):
        keys = [k for k in FIELD_ORDER if k in want or k in got]
        keys += sorted(k for k in set(want) | set(got) if k not in keys)
        for k in keys:
            if want.get(k) != got.get(k):
                return f"{prefix}{k}"
        return None
    return None if want == got else (prefix.rstrip(".") or "value")


def compare(expected, marks, actual):
    """None when actual equals expected under the suite's marks, else the first difference as
    {"index", "statement", "field", "expected", "actual"}."""
    for i, (want, got) in enumerate(zip(expected, actual)):
        relaxed = probe_marks.relax(marks.get("exchanges", {}).get(str(i), {}), want, got)
        if relaxed is None:
            continue
        want, got = relaxed
        for key in ("command", "statement", "params", "conn"):
            if want.get(key) != got.get(key):
                return {"index": i, "statement": want.get("statement"), "field": key,
                        "expected": want.get(key), "actual": got.get(key)}
        field = first_field(want.get("reply"), got.get("reply"))
        if field is not None:
            w, g = want.get("reply"), got.get("reply")
            if isinstance(w, dict) and isinstance(g, dict) and field in w | g:
                w, g = w.get(field), g.get(field)
            return {"index": i, "statement": want.get("statement"), "field": field, "expected": w, "actual": g}
    if len(expected) != len(actual):
        i = min(len(expected), len(actual))
        return {"index": i, "statement": (expected[i] if i < len(expected) else actual[i]).get("statement"),
                "field": "exchange_count", "expected": len(expected), "actual": len(actual)}
    return None
