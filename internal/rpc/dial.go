// Package rpc holds the gRPC client settings shared by Relay's binaries.
package rpc

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Dial returns a client connection to the coordinator. Reconnects back off
// quickly (capped at 1s) so workers resume promptly after a coordinator
// restart, and keepalives detect a dead connection without waiting for TCP.
// extra options (e.g. a history interceptor) are applied after these.
func Dial(addr string, extra ...grpc.DialOption) (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   time.Second,
			},
			MinConnectTimeout: 2 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	return grpc.NewClient(addr, append(opts, extra...)...)
}
