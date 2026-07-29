# One image for every Relay binary; compose picks the command.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ \
    ./cmd/coordinator ./cmd/worker ./cmd/relayctl

FROM alpine:3.21
RUN adduser -D -u 10001 relay
COPY --from=build /out/ /usr/local/bin/
USER relay
