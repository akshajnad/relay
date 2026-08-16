//go:build relay_nofence

package store

// FencingEnabled is false: this binary accepts lease reports without
// checking the fencing token. It exists only to prove the chaos checker
// catches the bug. Never deploy it.
const FencingEnabled = false
