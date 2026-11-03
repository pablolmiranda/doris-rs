# The upstream p0 suites drive Epic 1; recorded agent sessions are deferred

The first draft of the Epic 1 spec made recorded AI-agent sessions over TPC-H the finish line, with a plan corpus and frozen tablets as the inner loop. We switched to Doris's own regression suites (`oracle/suites/epic1.txt`, computed from `p0-core`) as driver and gate, because the harness already runs them with a reference baseline (2,605 passing suites at 4.1.5), and recording agents is a project of its own that would have made Epic 1 depend on two projects at once.

The agent goal is not dropped: a strict gate compares every MySQL reply the suites receive field by field (column metadata, errors, warnings), since the suites' own `.out` check sees only result text. Agent recording moves to its own issue.

## Considered Options

- Suites for day-to-day work, agent sessions still the finish line: rejected, it keeps both projects on the critical path.
- Dropping agent sessions entirely: rejected, they measure the real client and are worth building later.
