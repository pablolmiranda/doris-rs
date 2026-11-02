# Oracle harness for doris-rs: run the upstream Doris regression suites against the C++ reference
# to record a baseline, then against doris-rs and fail on any suite that regresses.
#
#   make doctor                      check prerequisites
#   make src                         pin the upstream source worktree (.cache/doris-src)
#   make image                       build the reference all-in-one image for DORIS_VERSION
#   make runner                      build the JDK 17 + Maven runner image
#   make up / make down              start / stop the reference cluster (PROFILE=single|multi)
#   make baseline SUITES=...         run suites against the reference, store baselines/<id>.json
#   make check TARGET=rust SUITES=.. run suites against a target and gate against the baseline
#   make compare BASE=.. CAND=..     compare two results files
#   make test                        unit tests for the harness scripts

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
BASELINE_ID := $(DORIS_VERSION)-$(PROFILE)-$(basename $(notdir $(SUITES)))
BASELINE  := baselines/$(BASELINE_ID).json

help:
	@sed -n '1,13p' Makefile | sed 's/^# \{0,1\}//'

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
	bin/run_suites.sh reference "$(SUITES)"
	cp results/reference/latest/results.json "$(BASELINE)"
	@echo "baseline written: $(BASELINE)"

check: src
	@test -f "$(BASELINE)" || { echo "no baseline $(BASELINE); run: make baseline SUITES=$(SUITES)"; exit 1; }
	bin/run_suites.sh "$(TARGET)" "$(SUITES)"
	python3 bin/compare.py "$(BASELINE)" results/$(TARGET)/latest/results.json \
	  --ignore suites/ignore.txt --json results/$(TARGET)/latest/compare.json

compare:
	@test -n "$(BASE)" -a -n "$(CAND)" || { echo "usage: make compare BASE=a.json CAND=b.json"; exit 1; }
	python3 bin/compare.py "$(BASE)" "$(CAND)" --ignore suites/ignore.txt

test:
	python3 -m unittest discover -s tests -v

.PHONY: help doctor src image runner up down status logs sql baseline check compare test
