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

.PHONY: all generate build test integration bench image lint clean chaos chaos-sweep up down

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

## up: start a local cluster (BACKEND=raft: 3 coordinators; BACKEND=redis: Redis + 1 coordinator)
up:
	docker compose -f $(COMPOSE_FILE_$(BACKEND)) up -d --build --wait --scale worker=$(WORKERS)

## down: stop the local cluster and delete its volumes
down:
	docker compose -f $(COMPOSE_FILE_$(BACKEND)) --profile extra down -v --remove-orphans

## image: build the relay container image used by compose
image:
	$(COMPOSE) build

## integration: multi-node Docker failure scenarios, plus the chaos checker's deliberate-bug test
integration: image
	go test -tags integration -count=1 -p 1 -timeout 60m -v ./test/...

## chaos: one seeded chaos run (make chaos SEED=3 BACKEND=raft DURATION=5m)
chaos: image
	go run ./cmd/relay-chaos run --backend $(BACKEND) --seed $(SEED) --duration $(DURATION)

## chaos-sweep: a chaos run per seed in SEEDS; stops at the first failure
chaos-sweep: image
	@for s in $(SEEDS); do \
		go run ./cmd/relay-chaos run --backend $(BACKEND) --seed $$s --duration $(DURATION) || exit 1; \
	done

## bench: bring up a compose cluster and measure scheduling latency
bench:
	go run ./cmd/relay-bench $(BENCH_ARGS)

lint:
	go vet ./...
	go vet -tags integration ./test/...
	go vet -tags relay_nofence ./internal/store/...

clean:
	rm -rf bin/
