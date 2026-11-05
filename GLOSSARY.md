# doris-rs

A Rust re-implementation of Apache Doris that clients, operators and existing clusters cannot tell
apart from the original, shipped as one binary.

## Roles and deployment

**doris-rs binary**:
The single executable that holds every Doris role. One process may run several roles at once, or
each node may run one.
_Avoid_: server, engine (for the whole binary)

**Role**:
One of the parts Doris splits a cluster into: FE (frontend: SQL, planning, metadata) and BE
(backend: storage and execution).
_Avoid_: service, component (when meaning FE or BE)

**Reference**:
The C++/Java Apache Doris release that doris-rs is measured against (pinned in `oracle/versions.env`).
_Avoid_: upstream (for the running cluster), original, legacy

## Migration

**In-place migration**:
Moving a running Doris cluster to doris-rs one role and one node at a time, on the same disks and
metadata, with a way back at each step.
_Avoid_: upgrade, cutover

**Rollback**:
Returning a node from doris-rs to the reference on the same disks and metadata.
_Avoid_: downgrade

**Cold cutover**:
In-place migration of the FE role: every reference FE stops, and doris-rs FEs start from their
metadata; BEs keep running. Rollback is the same in reverse.
_Avoid_: rolling FE upgrade (that is a mixed FE group)

**Mixed FE group**:
An FE group in which reference FEs and doris-rs FEs replicate the same metadata at once.

**Swap check**:
A test that starts the reference on disks doris-rs wrote, and doris-rs on disks the reference
wrote, and requires both to see the same state.

## Grading

**Gate suite**:
An upstream regression suite that passed in every reference run and is in an epic's scope.

**Strict gate**:
The check that every MySQL reply a suite receives is byte-equal to the reference's, except where
the reference itself varied.
_Avoid_: exact match, golden check
