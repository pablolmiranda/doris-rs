"""Traffic the harness causes on its own clock, declared in oracle/harness_traffic.json with the
source that sends it. It belongs to no suite, so the checker leaves it out."""
import json
import os

DECLARATION = os.path.join(os.path.dirname(__file__), "..", "harness_traffic.json")

with open(DECLARATION, encoding="utf-8") as _fh:
    _DECLARED = json.load(_fh)

HTTP_PATHS = {entry["path"] for entry in _DECLARED["be_http"]}


def is_http(header):
    return header.get("protocol") == "http" and header.get("path") in HTTP_PATHS


def is_connection(exchanges):
    """A MySQL connection that holds nothing but a declared connection's driver setup and
    statements."""
    statements = [ex["statement"] for ex in exchanges if ex["command"] == "COM_QUERY"]
    for entry in _DECLARED["mysql_connections"]:
        own = [st for st in statements if not st.startswith(tuple(entry["driver_setup_prefixes"]))]
        tail = statements[len(statements) - len(entry["statements"]):]
        if own == entry["statements"] and tail == entry["statements"]:
            return True
    return False
