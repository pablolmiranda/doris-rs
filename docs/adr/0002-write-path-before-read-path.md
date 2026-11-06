# The doris-rs BE takes writes before it reads cloned data

The first draft had doris-rs read TPC-H tablets cloned from a C++ BE first and add loading and segment writing at the end of the epic. Once the upstream suites drive the work (ADR 0001), that order cannot hold: almost every suite creates its tables and loads its data mid-run, so doris-rs needs tablet creation, `INSERT`, stream load, transactions and segment writing before it can pass almost anything. The write path is therefore milestone 3, right after joining the cluster. (Milestone 4 since ADR 0003 put the C++-compatible data directory before it.)

## Consequences

- "Data written by doris-rs stays readable by the C++ BE" binds from milestone 3, not from the end of the epic. Suites drop their own tables, so a separate readback check proves it: doris-rs loads a fixed dataset, a C++ BE clones it back, and replies must match.
- That readback check is also the rollback. Until it lands (with unique-key merge-on-write, at the latest when the write path is complete), there is no way to move data back to a C++ BE; on the test cluster this costs nothing because suites recreate their data.
- Milestone 4 (the write loop: `INSERT … VALUES` and CSV/JSON stream load into duplicate-key tables) lands the readback check's first form: a fixed dataset, not TPC-H, loaded by doris-rs and read back by the C++ BE taking its storage root over in place (`make -C oracle readback-check`), rather than cloned. The unique-key update load and TPC-H wait for the milestones that write unique keys and run queries.

