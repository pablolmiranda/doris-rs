# M2 BE fixtures

Captured from the reference C++ BE (`apache/doris:all-in-one-4.1.5`, tag `4.1.5-release`) on
2026-11-04 with `make -C oracle be-fixtures`, which runs `oracle/wire/scenarios/m2.sh` under the
wire capture and then `oracle/wire/bin/be_fixtures.py`.

- `messages/`, `manifest.json`: every FE↔BE message, in the wire corpus format. The manifest adds
  `task_types`, `created_tablets` and `dropped_tablets`, decoded from the `submit_tasks` calls.
  Tablets other than the scenario's belong to the FE's internal tables, created at start.
- `headers/<tablet_id>.json`: the reference BE's `GET /api/meta/header/<id>?byte_to_base64=true`
  for each tablet the scenario created, taken before any data was loaded into it.
- `layout.txt`: the storage root (`$ROOT`, `/opt/apache-doris/be/storage`) without `meta/`'s
  contents, after the scenario's tables were created, and again under `# after drop`, 80 s after
  `DROP TABLE m2.mow` (the C++ BE moves a dropped tablet to `trash/` on a later sweep).
