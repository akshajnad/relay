SHELL := /bin/bash

BUF_VERSION := v1.74.0
BUF := go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
COMPOSE := docker compose -f deploy/docker-compose.yml
BENCH_ARGS ?=
BACKEND ?= raft
WORKERS ?= 3
COMPOSE_FILE_redis := deploy/docker-compose.yml
COMPOSE_FILE_raft := deploy/docker-compose.raft.yml
SEED ?= 1
DURATION ?= 5m
SEEDS ?= 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
# Stamped into the image (OCI revision label) and recorded in every results
# file, so a number can be traced to the code that produced it.
export RELAY_GIT_REVISION := $(shell git rev-parse HEAD 2>/dev/null || echo unknown)$(shell git diff --quiet HEAD 2>/dev/null || echo -dirty)
SWEEP_TAG = relay:sweep-$(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
SWEEP_OUT = chaos-out/sweep-$(BACKEND)-$(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)

.PHONY: all generate build test integration bench bench-relay image lint clean chaos chaos-sweep up down

all: build

## generate: regenerate gen/ from proto/ (only Go is required; buf and plugins run via go run)
generate:
	$(BUF) lint
	$(BUF) generate

## build: compile all binaries into bin/
build:
	go build -o bin/ ./cmd/...

## test: unit tests (Lua scripts, coordinator, worker) against a real Redis via testcontainers
test:
	go test -race -count=1 ./internal/... ./cmd/...

OBSERVABILITY := -f deploy/docker-compose.observability.yml

## up: start a local cluster with Jaeger, Prometheus and Grafana
##     (BACKEND=raft: 3 coordinators; BACKEND=redis: Redis + 1 coordinator)
up:
	RELAY_OTLP_ENDPOINT=jaeger:4317 docker compose -f $(COMPOSE_FILE_$(BACKEND)) $(OBSERVABILITY) up -d --build --wait --scale worker=$(WORKERS)
	@echo "Grafana http://localhost:3000  Jaeger http://localhost:16686  Prometheus http://localhost:9095"

## down: stop the local cluster and delete its volumes
down:
	docker compose -f $(COMPOSE_FILE_$(BACKEND)) $(OBSERVABILITY) --profile extra down -v --remove-orphans

## image: build the relay container image used by compose
image:
	$(COMPOSE) build

## integration: multi-node Docker failure scenarios, plus the chaos checker's deliberate-bug test
integration: image
	go test -tags integration -count=1 -p 1 -timeout 60m -v ./test/...

## chaos: one seeded chaos run (make chaos SEED=3 BACKEND=raft DURATION=5m)
chaos: image
	go run ./cmd/relay-chaos run --backend $(BACKEND) --seed $(SEED) --duration $(DURATION)

## chaos-sweep: a chaos run per seed in SEEDS against a pinned image tag (so a
##              rebuild mid-sweep cannot change the binary under test), then
##              bench/results/chaos-sweep-$(BACKEND).json; fails if any seed fails
chaos-sweep: image
	docker tag relay:dev $(SWEEP_TAG)
	@failed=0; for s in $(SEEDS); do \
		go run ./cmd/relay-chaos run --backend $(BACKEND) --seed $$s --duration $(DURATION) \
			--image $(SWEEP_TAG) --out $(SWEEP_OUT) || failed=1; \
	done; \
	go run ./cmd/relay-chaos summarize --out bench/results/chaos-sweep-$(BACKEND).json $(SWEEP_OUT)/$(BACKEND)-seed*; \
	exit $$failed

## bench: comparative benchmark, Relay-Redis vs Relay-Raft vs asynq (~2.5h;
##        BENCH_ARGS="--trials 5 --sat-jobs 5000" for a quick look)
bench: image
	go run ./cmd/relay-bench compare $(BENCH_ARGS)

## bench-relay: the original single-backend benchmark (Relay-Redis, coordinator-measured latency)
bench-relay:
	go run ./cmd/relay-bench $(BENCH_ARGS)

lint:
	go vet ./...
	go vet -tags integration ./test/...
	go vet -tags relay_nofence ./internal/store/...

clean:
	rm -rf bin/

