"""Builds recorded run directories for tests, in the layout a real run leaves behind:
results.json (suite status and time window), tap/<conn>.jsonl (the MySQL tap's chunk log),
facts.json (the serving BE's deployment facts) and, optionally, traffic.bin.gz (FE<->BE
messages)."""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import mysqlfake as f  # noqa: E402

DEFAULT_FACTS = {"cluster": "test", "facts": [{"name": "be-host", "values": ["127.0.0.1"], "match": "substring"}]}


def q(sql, rows, columns=None):
    """A query command and its text result set (one column per value, named c0, c1, ...)."""
    columns = columns or [f.coldef(f"c{i}") for i in range(len(rows[0]) if rows else 1)]
    return [f.query(sql)], f.resultset(columns, rows)


def e(sql, code, message, sqlstate="HY000"):
    return [f.query(sql)], [f.err(code, sqlstate, message)]


def write_run(run_dir, suites, connections, facts=None, traffic=None):
    """suites: {key: (status, started_ms, finished_ms)}.
    connections: [[(t_ms, (client_payloads, server_payloads)), ...], ...]; the login is sent
    1 ms before the first command.
    traffic: [(header dict, data bytes)] written as the run's FE<->BE message file."""
    os.makedirs(os.path.join(run_dir, "tap"), exist_ok=True)
    results = {"meta": {}, "suites": {k: {"status": s, "started_ms": a, "finished_ms": b}
                                       for k, (s, a, b) in suites.items()}}
    with open(os.path.join(run_dir, "results.json"), "w") as fh:
        json.dump(results, fh)
    with open(os.path.join(run_dir, "facts.json"), "w") as fh:
        json.dump(facts or DEFAULT_FACTS, fh)
    for n, commands in enumerate(connections):
        start = commands[0][0] - 1
        lines = [("s2c", start, f.frame(0, f.greeting())), ("c2s", start, f.frame(1, f.login())),
                 ("s2c", start, f.frame(2, f.ok()))]
        for t, (client, server) in commands:
            lines.append(("c2s", t, f.frames(0, client)))
            if server:
                lines.append(("s2c", t, f.frames(len(client), server)))
        with open(os.path.join(run_dir, "tap", f"{n:04d}.jsonl"), "w") as fh:
            for d, t, data in lines:
                fh.write(json.dumps({"t_ms": t, "dir": d, "hex": data.hex()}) + "\n")
    if traffic is not None:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "checker"))
        import records  # noqa: E402
        records.write(os.path.join(run_dir, "traffic.bin.gz"), traffic)
    return run_dir
