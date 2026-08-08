package remotehost

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type memoryStore struct {
	hosts map[domain.RemoteHostID]domain.RemoteHost
}

func (s *memoryStore) CreateRemoteHost(_ context.Context, host domain.RemoteHost) (bool, error) {
	if _, exists := s.hosts[host.HostID]; exists {
		return false, nil
	}
	s.hosts[host.HostID] = host
	return true, nil
}

func (s *memoryStore) ListRemoteHosts(_ context.Context) ([]domain.RemoteHost, error) {
	hosts := make([]domain.RemoteHost, 0, len(s.hosts))
	for _, host := range s.hosts {
		hosts = append(hosts, host)
	}
	return hosts, nil
}

func (s *memoryStore) GetRemoteHost(_ context.Context, id domain.RemoteHostID) (domain.RemoteHost, bool, error) {
	host, found := s.hosts[id]
	return host, found, nil
}

func (s *memoryStore) RecordRemoteHostProbe(_ context.Context, id domain.RemoteHostID, at time.Time, succeeded bool, reason string) (bool, error) {
	host, found := s.hosts[id]
	if !found {
		return false, nil
	}
	host.LastProbeAt = at
	host.LastProbeSucceeded = succeeded
	host.LastProbeError = reason
	host.UpdatedAt = at
	s.hosts[id] = host
	return true, nil
}

func (s *memoryStore) SetRemoteHostOperatorState(_ context.Context, id domain.RemoteHostID, state domain.RemoteHostState, at time.Time) (domain.RemoteHost, bool, error) {
	host, found := s.hosts[id]
	if !found {
		return domain.RemoteHost{}, false, nil
	}
	host.OperatorState = state
	host.UpdatedAt = at
	s.hosts[id] = host
	return host, true, nil
}

func (s *memoryStore) DeleteRemoteHost(_ context.Context, id domain.RemoteHostID) (bool, error) {
	if _, found := s.hosts[id]; !found {
		return false, nil
	}
	delete(s.hosts, id)
	return true, nil
}

type failingProber struct {
	calls int
}

func (p *failingProber) Probe(context.Context, string) error {
	p.calls++
	return errors.New("connection refused")
}

func TestFailedProbeOnlyProducesUnreachable(t *testing.T) {
	now := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)
	store := &memoryStore{hosts: map[domain.RemoteHostID]domain.RemoteHost{}}
	prober := &failingProber{}
	svc := New(Deps{Store: store, Prober: prober, Clock: func() time.Time { return now }})

	host, err := svc.Register(context.Background(), RegisterInput{HostID: "lab-host", Address: "127.0.0.1:3001"})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if host.State != domain.RemoteHostStateUnreachable {
		t.Fatalf("state = %q, want unreachable", host.State)
	}
	if host.LastProbeError != "connection refused" {
		t.Fatalf("last probe error = %q", host.LastProbeError)
	}
}

func TestHealthProbeDoesNotClearOperatorState(t *testing.T) {
	now := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)
	store := &memoryStore{hosts: map[domain.RemoteHostID]domain.RemoteHost{
		"lab-host": {HostID: "lab-host", Address: "127.0.0.1:3001", OperatorState: domain.RemoteHostStateStopped},
	}}
	prober := &failingProber{}
	svc := New(Deps{Store: store, Prober: prober, Clock: func() time.Time { return now }})

	svc.probeAll(context.Background())
	host, found, err := store.GetRemoteHost(context.Background(), "lab-host")
	if err != nil || !found {
		t.Fatalf("get host: found=%v err=%v", found, err)
	}
	if host.OperatorState != domain.RemoteHostStateStopped || host.CurrentState() != domain.RemoteHostStateStopped {
		t.Fatalf("state = %q/%q, want stopped", host.OperatorState, host.CurrentState())
	}
	if prober.calls != 0 {
		t.Fatalf("probe calls = %d, want 0", prober.calls)
	}
}
