SHELL := /bin/bash

BUF_VERSION := v1.74.0
BUF := go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
COMPOSE := docker compose -f deploy/docker-compose.yml
BENCH_ARGS ?=

.PHONY: all generate build test integration bench image lint clean

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

## image: build the relay container image used by compose
image:
	$(COMPOSE) build

## integration: multi-node Docker failure scenarios
integration: image
	go test -tags integration -count=1 -p 1 -timeout 30m -v ./test/integration/...

## bench: bring up a compose cluster and measure scheduling latency
bench:
	go run ./cmd/relay-bench $(BENCH_ARGS)

lint:
	go vet ./...
	go vet -tags integration ./test/...

clean:
	rm -rf bin/
