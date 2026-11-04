# Oracle harness

The oracle is the C++ Apache Doris release. This harness runs Doris's own regression suites,
unchanged, against a reference cluster to record what passes, then against `doris-rs` and fails
when anything that passed on the reference stops passing.

It works because the upstream framework (`regression-test/` in the Doris repo) is a black box:
it reaches the cluster only through the MySQL port, the FE HTTP API, and a small Thrift surface.
Any implementation that speaks those protocols can be measured by the same 9,000+ suites.

## Pieces

| Path | Role |
|---|---|
| `versions.env` | Pinned release tag, image tags, network name. Change tag and image together. |
| `bin/ensure_src.sh` | Git worktree of `../doris` at the pinned tag, under `../.cache/doris-src` |
| `compose/reference-single.yml` | 1 FE + 1 BE reference cluster (upstream all-in-one image) |
| `compose/reference-multi.yml` | 3 FE + 3 BE reference cluster, for replica and failover suites |
| `compose/runner.Dockerfile` | JDK 17 + Maven image that runs the Groovy framework |
| `targets/*.env` | Where a target cluster listens (`reference-single`, `reference-multi`, `rust`, add more) |
| `suites/*.txt` | Which suite directories to run; `ignore.txt` lists known-flaky suites |
| `bin/run_suites.sh` | Renders the conf, runs the framework in the runner container, parses output |
| `bin/override_conf.py` | Copies the upstream conf and overrides only the address keys |
| `bin/parse_teamcity.py` | TeamCity service messages → `results.json` (passed/failed/incomplete) |
| `bin/compare.py` | Baseline vs candidate → regression/missing/fixed/new/known, exit 1 on regressions |
| `bin/triage.py` | Failures per directory and top server error phrases for a results file |
| `baselines/` | Committed reference results, one file per version, profile, and suite list |
| `results/` | Untracked raw runs: conf, log, results.json, meta.json; `latest` symlink per target |

## First run

```
make doctor                         # docker daemon, upstream checkout, tag, images
make src                            # worktree at the pinned tag
make image                          # all-in-one image for the pinned version (needs docker)
make runner                         # JDK 17 + Maven runner image
make up                             # reference cluster, waits for healthy
make baseline SUITES=suites/smoke.txt
make baseline SUITES=suites/p0-core.txt
```

The first `baseline` also downloads the framework's Maven dependencies into the
`doris-oracle-m2` volume, so it is slow once. The runner image builds the Thrift 0.24.0 compiler
from source because the upstream script regenerates Thrift Java sources before compiling the
framework; that is a one-time image build cost.

After every baseline the Makefile prints a triage: failures per directory and the most common
server errors. Read it before trusting the baseline. Hundreds of failures with one error such as
`Table ... does not exist` mean a shared fixture failed to load (usually a dataset fetched from
S3 with credentials the harness does not have), not hundreds of behaviors. Those suites are
recorded as known failures and never gate anything; add real S3 credentials to the rendered conf
if you want them to count.

## Gating doris-rs

Start the Rust binary with the stock ports, then:

```
make check TARGET=rust SUITES=suites/p0-core.txt
```

Exit status 0 means no suite that passed on the reference failed or went missing. The full
classification is in `results/rust/latest/compare.json`. Suites that are flaky on the reference
belong in `suites/ignore.txt` with a reason; nothing else does.

## Epic 1 checker

Epic 1 (#4) replaces the BE first, so its checker grades a candidate on the upstream p0 suites
that exercise the BE, with two gates. Design: `docs/superpowers/specs/2026-11-02-epic-1-reporting-design.md`.

- **`.out` gate:** each suite still passes, as upstream judges it.
- **Strict gate:** every MySQL reply is byte-equal to the reference's, except where the reference
  itself varied. A tap between the framework and the FE records each connection. Probe marks,
  derived from the reference runs, are the only relaxations: a reply that varied is compared
  unordered, with columns or fields masked, or by pattern. A suite whose replies varied in a way
  no mark describes (a different kind of reply, error code, or row count) is unstable and not
  compared. Values that depend on the deployment (host,
  ports, BE cores and memory) are substituted from `clusters/*.facts.json`; cores and memory are
  read from `SHOW BACKENDS` at run time.

```
make epic-baseline                  # 5 reference runs (3 on cluster A, 2 on split cluster B) → epic1/
make epic-check TARGET=rust         # one run, both gates, grade.json in results/rust/latest/
make epic-grade RUN=results/<target>/<run-id>
```

`oracle/epic1/` is the committed reference, derived by `checker/derive.py`:

| File | Content |
|---|---|
| `suites.txt` | The gate: in-scope suites that passed in every reference run |
| `flaky.txt` | In-scope suites that failed in some reference run, with the runs |
| `replies/`, `marks.json` | Reference replies per suite and the probe marks |
| `traffic/` | Per-suite FE↔BE corpus (Thrift, gRPC, BE HTTP); `manifest.json` lists background traffic, capture holes and unnamed gRPC frames |
| `reaches_be.txt` | Gate suites that sent work to the BE |
| `needs.json`, `ranking.json` | Features each suite needs; features ranked by suites they block |

Ratchets (`scores/epic1-out.txt`, `scores/epic1-strict.txt`) are empty until doris-rs passes
suites; `scores/epic1-supported.json` lists the features doris-rs supports.

**Reference numbers (4.1.5, 2026-11-03).**

- **Scope:** 1712 of 2739 p0 suites are in scope. The rest are excluded by feature; counts in
  brackets are suites excluded for that reason alone:
  - complex types: 385 (194)
  - text index: 358 (191)
  - aggregate key: 200 (85)
  - variant: 186 (53)
  - agg types: 157 (45)
  - debug points: 94 (50)
  - Arrow Flight: 64 (44)
  - external catalog: 12 (12)
  - vector: 2 (1)
- **Gate:** 1622 suites; 8 flaky; 1453 reach the BE. Of the other 169, 166 send the BE nothing;
  3 carry only `reportExecStatus` or `check_storage_format` calls, which do not count as work.
- **Strict:**
  - 1498 suites compared (stable);
  - 41 suites not compared because they are unstable: their exchanges differed in number or
    command, or a reply varied in a way no mark describes;
  - 83 suites not compared because they have no replies: they pass without a statement of their
    own, mostly suites that return early off a cloud or multi-FE deployment (34 in `load_p0`);
- **Traffic corpus:** 48 MB.
  - Every message re-encodes to its bytes in `cargo test`: 494,496 messages, of which 4,592 match
    up to set order.
  - HTTP bodies over 64 KiB are kept as length and SHA-256.
  - 11 gRPC frames with no method are listed but not kept, from tshark misframing compressed streams.
- **Self-check:** a sixth run on a fresh cluster A, graded against the reference:
  - `.out` gate: 1622 of 1622;
  - strict gate: 1489 of 1622, or 1489 of the 1498 compared. The other 9 show a difference.

  The 9 differences are rare nondeterminism in Doris itself, and each held steady in all five
  reference runs:
  - which row overflows first;
  - last-digit float rounding;
  - random-bucket tablet counts;
  - a task counter in `show backends`.

  A candidate can fail these by chance; treat a strict failure in one of them as noise.

Top of the ranking (`alone` = suites blocked by this feature only, `total` = suites needing it):

| Feature | Alone | Total |
|---|---|---|
| rpc:FrontendService.reportExecStatus | 1 | 775 |
| rpc:BackendService.check_storage_format | 1 | 1 |
| type:VARCHAR | 0 | 1396 |
| expr:SLOT_REF | 0 | 1378 |
| type:BIGINT | 0 | 1357 |
| rpc:PBackendService.cancel_plan_fragment | 0 | 1355 |
| sink:RESULT_SINK | 0 | 1334 |
| rpc:PBackendService.fetch_data | 0 | 1331 |
| type:BOOLEAN | 0 | 1269 |
| plan_node:OLAP_SCAN_NODE | 0 | 1263 |
| type:TINYINT | 0 | 1263 |
| type:INT | 0 | 1256 |
| type:STRING | 0 | 1245 |
| type:DATETIMEV2 | 0 | 1224 |
| plan_node:EXCHANGE_NODE | 0 | 1202 |

Nearly every suite needs many features at once, so `alone` stays near zero until doris-rs
supports the common ones; `total` is the better guide to order early on.

## Design notes

- **Pinning.** The suites must match the server they test, so `DORIS_SRC_TAG` and `DORIS_IMAGE`
  move together. The all-in-one Dockerfile exists only on upstream master, so `make image` builds
  from the sibling master checkout while suites come from the release worktree.
- **Network.** The runner joins the `doris-oracle` Docker network so stream load redirects to BE
  addresses resolve. A Rust binary on the host is reached as `host.docker.internal`.
- **Server config matches upstream CI.** Both compose files append the FE and BE settings that
  upstream's p0 pipeline uses (`regression-test/pipeline/p0/conf/` at the pinned tag): legacy
  DATE and DECIMAL types enabled, debug points, binlog, MTMV, short label retention, single
  replica forced on the single profile. Fuzzy session-variable mode is deliberately left out so a
  baseline is deterministic. Without these, hundreds of suites fail on the reference for reasons
  unrelated to behavior.
- **Baselines contain failures.** A reference run is not expected to be 100 percent green: some
  suites need external systems (Kafka, HDFS, S3) or three replicas. The gate only cares about
  suites that passed on the reference.
- **Incomplete beats silent.** A suite that started and never finished is recorded as
  `incomplete` and counts as a regression, which catches server crashes.

## Not yet covered

Golden on-disk format corpus, mixed-cluster wire compatibility, differential SQL fuzzing, and
performance budgets are the next harness pieces. See the project README for the migration plan.
