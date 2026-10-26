//go:build !relay_nofence

package store

// FencingEnabled is true in every normal build. Building with the
// relay_nofence tag turns the fencing check off in every backend, so the
// chaos checker can be shown to catch the resulting violations. That build
// only ever goes into the relay:nofence test image.
const FencingEnabled = true
