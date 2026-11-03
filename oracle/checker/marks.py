"""Probe marks: what the reference itself varied across runs, and nothing else, is relaxed.

derive_marks compares one suite's exchanges from every reference run with the anchor (run 1)
and returns the suite's marks; relax applies an exchange's mark to an expected and an actual
exchange before they are compared. Marks are never written by hand.

Per exchange (index into the suite's exchanges):
  skip                the whole reply varied (kind, error code, row count): not compared
  statement_pattern   regex the statement text must fully match (generated labels, names)
  unordered           rows compare as a multiset
  masked_columns      [j]: column j's values are not compared
  masked_column_meta  [[j, field]]: that column-definition field is not compared
  message_pattern     regex the error message must fully match
  masked_fields       scalar reply fields not compared (affected_rows, info, warnings, ...)
A suite whose runs have different exchange counts or commands is "unstable" and not compared.
"""
import difflib
import json
import re

SCALAR_FIELDS = ("affected_rows", "last_insert_id", "status", "warnings", "info", "server_caps", "server_version")


TOKEN = re.compile(r"\w+|\W", re.S)


def pattern(anchor, variants):
    """A regex matching the anchor and every variant. Texts are compared as word tokens, so a
    varying id or label is wildcarded whole; tokens of the anchor that every variant keeps are
    literal, the others become one lazy wildcard per run."""
    tokens = TOKEN.findall(anchor)
    keep = [True] * len(tokens)
    for v in variants:
        kept = [False] * len(tokens)
        sm = difflib.SequenceMatcher(None, tokens, TOKEN.findall(v), autojunk=False)
        for block in sm.get_matching_blocks():
            for i in range(block.a, block.a + block.size):
                kept[i] = True
        keep = [a and b for a, b in zip(keep, kept)]
    out = []
    for tok, k in zip(tokens, keep):
        if k:
            out.append(re.escape(tok))
        elif not out or out[-1] != ".*?":
            out.append(".*?")
    if out and out[-1] == ".*?":
        out[-1] = ".*"
    return "".join(out)


def rowkey(row):
    return json.dumps(row)


def mask_rows(rows, columns):
    return [["*" if j in columns else v for j, v in enumerate(r)] for r in rows]


def reply_mark(anchor, others):
    """Mark for one exchange's reply, or {"skip": True} when it cannot be relaxed precisely."""
    if all(o == anchor for o in others):
        return {}
    if any(not isinstance(o, dict) or o.get("type") != (anchor or {}).get("type") for o in others):
        return {"skip": True} if anchor != others[0] or len(set(map(json.dumps, others))) > 1 else {}
    mark = {}
    kind = anchor.get("type")
    fields = sorted({f for o in others for f in SCALAR_FIELDS if o.get(f) != anchor.get(f)})
    if fields:
        mark["masked_fields"] = fields
    if kind == "err":
        if any((o.get("code"), o.get("sqlstate")) != (anchor.get("code"), anchor.get("sqlstate")) for o in others):
            return {"skip": True}
        if any(o.get("message") != anchor.get("message") for o in others):
            mark["message_pattern"] = pattern(anchor["message"], [o["message"] for o in others])
    elif kind == "resultset":
        cols = anchor.get("columns", [])
        if any(len(o.get("columns", [])) != len(cols) or len(o.get("rows", [])) != len(anchor.get("rows", []))
               for o in others):
            return {"skip": True}
        meta = sorted({(j, k) for o in others for j, c in enumerate(o["columns"]) for k in c if c[k] != cols[j].get(k)})
        if meta:
            mark["masked_column_meta"] = [list(x) for x in meta]
        masked = set()
        for o in others:
            if sorted(map(rowkey, o["rows"])) == sorted(map(rowkey, anchor["rows"])):
                continue
            for a_row, o_row in zip(anchor["rows"], o["rows"]):
                masked |= {j for j, (x, y) in enumerate(zip(a_row, o_row)) if x != y}
        if masked:
            mark["masked_columns"] = sorted(masked)
        a_rows = mask_rows(anchor["rows"], masked)
        for o in others:
            o_rows = mask_rows(o["rows"], masked)
            if o_rows != a_rows:
                if sorted(map(rowkey, o_rows)) == sorted(map(rowkey, a_rows)):
                    mark["unordered"] = True
                else:
                    return {"skip": True}
    elif any({k: v for k, v in o.items() if k not in SCALAR_FIELDS} != {k: v for k, v in anchor.items() if k not in SCALAR_FIELDS}
             for o in others):
        return {"skip": True}
    return mark


def derive_marks(runs):
    """runs: one suite's exchange lists, anchor first."""
    anchor, others = runs[0], runs[1:]
    if any(len(o) != len(anchor) for o in others):
        return {"status": "unstable", "reason": "exchange counts differ between runs", "exchanges": {}}
    exchanges = {}
    for i, ex in enumerate(anchor):
        variants = [o[i] for o in others]
        if any((v["command"], v["conn"], v["params"]) != (ex["command"], ex["conn"], ex["params"]) for v in variants):
            return {"status": "unstable", "reason": f"exchange {i} differs in command or connection", "exchanges": {}}
        mark = {}
        if any(v["statement"] != ex["statement"] for v in variants):
            mark["statement_pattern"] = pattern(ex["statement"] or "", [v["statement"] or "" for v in variants])
        mark.update(reply_mark(ex["reply"], [v["reply"] for v in variants]))
        if mark:
            exchanges[str(i)] = mark
    return {"status": "stable", "reason": None, "exchanges": exchanges}


def relax(mark, want, got):
    """(want, got) with the mark applied, ready for an exact comparison; None when the exchange
    is skipped. Pattern fields that match are made equal; ones that do not are left to differ."""
    if mark.get("skip"):
        return None
    want, got = json.loads(json.dumps(want)), json.loads(json.dumps(got))
    if "statement_pattern" in mark and re.fullmatch(mark["statement_pattern"], got.get("statement") or "", re.S):
        got["statement"] = want["statement"]
    w, g = want.get("reply"), got.get("reply")
    if isinstance(w, dict) and isinstance(g, dict):
        for f in mark.get("masked_fields", []):
            w.pop(f, None)
            g.pop(f, None)
        if "message_pattern" in mark and re.fullmatch(mark["message_pattern"], g.get("message") or "", re.S):
            g["message"] = w.get("message")
        for j, f in mark.get("masked_column_meta", []):
            for r in (w, g):
                if j < len(r.get("columns", [])):
                    r["columns"][j].pop(f, None)
        masked = set(mark.get("masked_columns", []))
        if masked:
            w["rows"], g["rows"] = mask_rows(w.get("rows", []), masked), mask_rows(g.get("rows", []), masked)
        if mark.get("unordered"):
            w["rows"], g["rows"] = sorted(w.get("rows", []), key=rowkey), sorted(g.get("rows", []), key=rowkey)
    return want, got
