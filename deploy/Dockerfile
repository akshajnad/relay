# One image for every Relay binary; compose picks the command.
FROM golang:1.26-alpine AS build
# GO_TAGS=relay_nofence builds the deliberately broken relay:nofence image
# used to prove the chaos checker catches fencing violations.
ARG GO_TAGS=""
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -tags "$GO_TAGS" -o /out/ \
    ./cmd/coordinator ./cmd/worker ./cmd/relayctl ./cmd/relay-chaos ./cmd/relay-bench ./cmd/asynq-worker

FROM alpine:3.21
# GIT_REVISION is `git rev-parse HEAD` (plus "-dirty"), set by the Makefile;
# results files record it (internal/buildrev).
ARG GIT_REVISION=unknown
LABEL org.opencontainers.image.revision=$GIT_REVISION
RUN adduser -D -u 10001 relay && mkdir /data && chown relay /data
COPY --from=build /out/ /usr/local/bin/
USER relay
