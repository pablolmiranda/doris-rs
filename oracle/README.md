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
| `targets/*.env` | Where a target cluster listens (`reference`, `rust`, add more) |
| `suites/*.txt` | Which suite directories to run; `ignore.txt` lists known-flaky suites |
| `bin/run_suites.sh` | Renders the conf, runs the framework in the runner container, parses output |
| `bin/override_conf.py` | Copies the upstream conf and overrides only the address keys |
| `bin/parse_teamcity.py` | TeamCity service messages → `results.json` (passed/failed/incomplete) |
| `bin/compare.py` | Baseline vs candidate → regression/missing/fixed/new/known, exit 1 on regressions |
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
`doris-oracle-m2` volume, so it is slow once.

## Gating doris-rs

Start the Rust binary with the stock ports, then:

```
make check TARGET=rust SUITES=suites/p0-core.txt
```

Exit status 0 means no suite that passed on the reference failed or went missing. The full
classification is in `results/rust/latest/compare.json`. Suites that are flaky on the reference
belong in `suites/ignore.txt` with a reason; nothing else does.

## Design notes

- **Pinning.** The suites must match the server they test, so `DORIS_SRC_TAG` and `DORIS_IMAGE`
  move together. The all-in-one Dockerfile exists only on upstream master, so `make image` builds
  from the sibling master checkout while suites come from the release worktree.
- **Network.** The runner joins the `doris-oracle` Docker network so stream load redirects to BE
  addresses resolve. A Rust binary on the host is reached as `host.docker.internal`.
- **Baselines contain failures.** A reference run is not expected to be 100 percent green: some
  suites need external systems (Kafka, HDFS, S3) or three replicas. The gate only cares about
  suites that passed on the reference.
- **Incomplete beats silent.** A suite that started and never finished is recorded as
  `incomplete` and counts as a regression, which catches server crashes.

## Not yet covered

Golden on-disk format corpus, mixed-cluster wire compatibility, differential SQL fuzzing, and
performance budgets are the next harness pieces. See the project README for the migration plan.
