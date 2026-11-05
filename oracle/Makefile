# Oracle harness for doris-rs: run the upstream Doris regression suites against the C++ reference
# to record a baseline, then against doris-rs and fail on any suite that regresses.
#
#   make doctor                      check prerequisites
#   make src                         pin the upstream source worktree (.cache/doris-src)
#   make image                       build the reference all-in-one image for DORIS_VERSION
#   make runner                      build the JDK 17 + Maven runner image
#   make up / make down              start / stop the reference cluster (PROFILE=single|split|multi)
#   make baseline SUITES=...         run suites against the reference (PROFILE), store baselines/<id>.json
#   make check TARGET=rust SUITES=.. run suites against a target and gate against the baseline
#   make compare BASE=.. CAND=..     compare two results files
#   make triage RESULTS=..           failures by directory and top error phrases
#   make test                        unit tests for the harness scripts
#   make corpus [TABLES="t1 t2"]     build the golden corpus; TABLES limits it to new tables
#   make wire-capture                record FE<->BE RPC traffic (recreates the reference)
#   make wire-extract [PCAP=...]     turn the newest capture into oracle/wire/corpus
#   make be-fixtures                 capture the M2 BE fixtures (crates/doris-be/tests/fixtures/m2)
#   make epic-baseline               Epic 1 reference: 5 serial runs on clusters A and B, derived into epic1/
#   make fe-up / fe-down / rust-be   FE-only cluster for doris-rs; rust-be runs doris-rs as its BE
#   make meta-fixtures               capture the C++ BE's meta/ databases (crates/doris-rocks/tests/fixtures)
#   make loaded-fixture              capture the C++ BE after the M4 load dataset (crates/doris-storage/tests/fixtures/cpp-loaded)
#   make swap-check                  doris-rs and the C++ BE take turns on one root; the FE must notice nothing
#   make readback-check              the C++ BE reads what doris-rs loaded as it reads its own loads
#   make epic-check TARGET=..        run a target through the tap and grade it (suite + strict gates, ratchets)
#   make epic-grade RUN=..           grade an already recorded run again

SHELL := /bin/bash
.DEFAULT_GOAL := help
include versions.env
export

PROFILE   ?= single
SUITES    ?= suites/smoke.txt
TARGET    ?= rust
PARALLEL  ?= 4
SUITE_PARALLEL ?= 1
UPSTREAM  ?= $(abspath ../../doris)
SRC       := $(abspath ../.cache/doris-src)
COMPOSE   := docker compose -f compose/reference-$(PROFILE).yml
FE_CONTAINER := $(if $(filter split,$(PROFILE)),doris-oracle-split-fe-1,$(if $(filter multi,$(PROFILE)),doris-oracle-fe-1-1,doris-oracle-doris-1))
BASELINE_ID := $(DORIS_VERSION)-$(PROFILE)-$(basename $(notdir $(SUITES)))
BASELINE  := baselines/$(BASELINE_ID).json

help:
	@sed -n '1,25p' Makefile | sed 's/^# \{0,1\}//'

doctor:
	@echo "upstream:  $(UPSTREAM)"; test -d "$(UPSTREAM)/.git" && echo "  ok" || echo "  MISSING (set UPSTREAM=path)"
	@echo "tag:       $(DORIS_SRC_TAG)"; git -C "$(UPSTREAM)" rev-parse -q --verify refs/tags/$(DORIS_SRC_TAG) >/dev/null && echo "  ok" || echo "  MISSING (git fetch --tags)"
	@echo "docker:"; if docker info >/dev/null 2>&1; then docker info --format '  ok {{.ServerVersion}} {{.OSType}}/{{.Architecture}}'; else echo "  daemon not reachable (context: $$(docker context show 2>/dev/null))"; fi
	@echo "image:     $(DORIS_IMAGE)"; docker image inspect $(DORIS_IMAGE) >/dev/null 2>&1 && echo "  ok" || echo "  not present (make image, or set DORIS_IMAGE=apache/doris:all-in-one-4.1.3)"
	@echo "runner:    $(RUNNER_IMAGE)"; docker image inspect $(RUNNER_IMAGE) >/dev/null 2>&1 && echo "  ok" || echo "  not present (make runner)"
	@echo "python3:   $$(python3 --version 2>&1)"

src:
	@DORIS_UPSTREAM="$(UPSTREAM)" DORIS_SRC="$(SRC)" bin/ensure_src.sh

# The all-in-one Dockerfile lives only on upstream master, so it is taken from the sibling
# checkout, while the suites come from the pinned release worktree.
image:
	cd "$(UPSTREAM)/docker/runtime/all-in-one/4.1" && ./build.sh -v $(DORIS_VERSION) -f base

runner:
	docker build --build-arg RUNNER_BASE=$(RUNNER_BASE) -t $(RUNNER_IMAGE) -f compose/runner.Dockerfile compose

up:
	$(COMPOSE) up --wait
	bin/cluster_setup.sh $(FE_CONTAINER)
	@echo "FE mysql: 127.0.0.1:$${FE_PORT:-9030}  FE http: 127.0.0.1:$${FE_HTTP_PORT:-8030}  (inside network: doris)"

down:
	$(COMPOSE) down -v

status:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=200

sql:
	$(COMPOSE) exec $(if $(filter multi,$(PROFILE)),client,doris) mysql -uroot -h$(if $(filter multi,$(PROFILE)),fe-1,127.0.0.1) -P9030

baseline: src
	@test -n "$(SUITES)"
	bin/run_suites.sh reference-$(PROFILE) "$(SUITES)"
	cp results/reference-$(PROFILE)/latest/results.json "$(BASELINE)"
	@echo "baseline written: $(BASELINE)"
	@python3 bin/triage.py "$(BASELINE)" --top 6

check: src
	@test -f "$(BASELINE)" || { echo "no baseline $(BASELINE); run: make baseline SUITES=$(SUITES)"; exit 1; }
	bin/run_suites.sh "$(TARGET)" "$(SUITES)"
	python3 bin/compare.py "$(BASELINE)" results/$(TARGET)/latest/results.json \
	  --ignore suites/ignore.txt --json results/$(TARGET)/latest/compare.json

compare:
	@test -n "$(BASE)" -a -n "$(CAND)" || { echo "usage: make compare BASE=a.json CAND=b.json"; exit 1; }
	python3 bin/compare.py "$(BASE)" "$(CAND)" --ignore suites/ignore.txt

corpus-load:
	corpus/bin/load.sh $(TABLES)

corpus-expect:
	corpus/bin/expect.sh $(TABLES)

corpus-extract:
	python3 corpus/bin/extract.py $(TABLES)

corpus: corpus-load corpus-expect corpus-extract

wire-capture:
	wire/bin/capture.sh

wire-extract:
	python3 wire/bin/extract.py $(PCAP)

BE_FIXTURES := ../crates/doris-be/tests/fixtures/m2

be-fixtures:
	pcap="$$(SCENARIO=wire/scenarios/m2.sh OUT=$(BE_FIXTURES) wire/bin/capture.sh)" && \
	  python3 wire/bin/be_fixtures.py "$$pcap" $(BE_FIXTURES)

meta-fixtures:
	swap/capture-meta.sh

loaded-fixture:
	swap/capture-loaded.sh

swap-check:
	swap/swap-check.sh

readback-check:
	swap/readback-check.sh

EPIC_SUITES ?= suites/epic1.txt
EPIC_REF    ?= epic1

epic-baseline: src
	bin/epic_baseline.sh $(EPIC_SUITES)

# The cluster doris-rs is graded in: the reference FE alone, and doris-rs on the host as its BE.
RUST_COMPOSE := docker compose -f compose/fe-only.yml
RUST_FE      := doris-oracle-fe-only

fe-up:
	$(RUST_COMPOSE) up --wait
	@ip="$$(docker exec $(RUST_FE) getent ahostsv4 host.docker.internal | awk 'NR==1{print $$1}')"; \
	  test -n "$$ip" || { echo "host.docker.internal does not resolve in $(RUST_FE)"; exit 1; }; \
	  docker exec $(RUST_FE) mysql -uroot -h127.0.0.1 -P9030 -e "SHOW BACKENDS" | grep -q "$$ip" || \
	  docker exec $(RUST_FE) mysql -uroot -h127.0.0.1 -P9030 -e "ALTER SYSTEM ADD BACKEND '$$ip:9050'"; \
	  echo "doris-rs registers as $$ip; start it with: make rust-be"

fe-down:
	$(RUST_COMPOSE) down -v

rust-be:
	cargo build --release -p doris-rs --manifest-path ../Cargo.toml
	mkdir -p /tmp/doris-rs-oracle/storage
	../target/release/doris-rs be --config rust/be.conf

epic-check: src
	@test -f $(EPIC_REF)/marks.json || { echo "no Epic 1 reference in $(EPIC_REF); run: make epic-baseline"; exit 1; }
	PROXY=1 SERIAL=1 bin/run_suites.sh "$(TARGET)" "$(EPIC_SUITES)"
	-cat suites/ignore.txt $(EPIC_REF)/flaky.txt > results/$(TARGET)/latest/ignore.txt
	-python3 bin/compare.py $(EPIC_REF)/baseline.json results/$(TARGET)/latest/results.json --ignore results/$(TARGET)/latest/ignore.txt
	python3 checker/grade.py results/$(TARGET)/latest $(EPIC_REF) scores --json results/$(TARGET)/latest/grade.json

epic-grade:
	@test -n "$(RUN)" || { echo "usage: make epic-grade RUN=results/<target>/<run-id>"; exit 1; }
	python3 checker/grade.py $(RUN) $(EPIC_REF) scores --json $(RUN)/grade.json

triage:
	python3 bin/triage.py "$(or $(RESULTS),results/reference-$(PROFILE)/latest/results.json)"

test:
	python3 -m unittest discover -s tests -v

.PHONY: help doctor src image runner up down status logs sql baseline check compare triage test corpus corpus-load corpus-expect corpus-extract wire-capture wire-extract be-fixtures meta-fixtures loaded-fixture swap-check readback-check fe-up fe-down rust-be epic-baseline epic-check epic-grade
