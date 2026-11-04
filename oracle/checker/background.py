"""Traffic sent on a clock rather than by a suite (the harness's own, and the cluster's periodic
RPCs), declared in oracle/background_traffic.json with the code that sends it. It belongs to no
suite, so the checker leaves it out."""
import json
import os

DECLARATION = os.path.join(os.path.dirname(__file__), "..", "background_traffic.json")

with open(DECLARATION, encoding="utf-8") as _fh:
    _DECLARED = json.load(_fh)

HTTP_PATHS = {entry["path"] for entry in _DECLARED["harness"]["be_http"]}
CLUSTER_RPCS = {entry["method"] for entry in _DECLARED["cluster_rpcs"]}


def is_harness_http(header):
    return header.get("protocol") == "http" and header.get("path") in HTTP_PATHS


def is_harness_connection(exchanges):
    """A MySQL connection that holds nothing but a declared connection's driver setup and
    statements."""
    statements = [ex["statement"] for ex in exchanges if ex["command"] == "COM_QUERY"]
    for entry in _DECLARED["harness"]["mysql_connections"]:
        own = [st for st in statements if not st.startswith(tuple(entry["driver_setup_prefixes"]))]
        tail = statements[len(statements) - len(entry["statements"]):]
        if own == entry["statements"] and tail == entry["statements"]:
            return True
    return False
