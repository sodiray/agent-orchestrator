package ports

import "context"

// RemoteDaemonProber checks whether an address serves the expected daemon.
type RemoteDaemonProber interface {
	Probe(ctx context.Context, address string) error
}
