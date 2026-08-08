package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func TestRemoteHostRoundTrip(t *testing.T) {
	s := newTestStore(t)
	now := time.Date(2026, 8, 7, 1, 2, 3, 0, time.UTC)
	host := domain.RemoteHost{
		HostID:             "lab-host",
		Address:            "127.0.0.1:3001",
		Label:              "Lab host",
		LastProbeAt:        now,
		LastProbeSucceeded: true,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	created, err := s.CreateRemoteHost(context.Background(), host)
	if err != nil || !created {
		t.Fatalf("create remote host: created=%v err=%v", created, err)
	}
	duplicate, err := s.CreateRemoteHost(context.Background(), host)
	if err != nil || duplicate {
		t.Fatalf("duplicate create: created=%v err=%v", duplicate, err)
	}

	got, found, err := s.GetRemoteHost(context.Background(), host.HostID)
	if err != nil || !found {
		t.Fatalf("get remote host: found=%v err=%v", found, err)
	}
	if got.Address != host.Address || got.Label != host.Label || !got.LastProbeSucceeded {
		t.Fatalf("remote host = %+v, want %+v", got, host)
	}

	changed, err := s.RecordRemoteHostProbe(context.Background(), host.HostID, now.Add(time.Minute), false, "connection refused")
	if err != nil || !changed {
		t.Fatalf("record probe: changed=%v err=%v", changed, err)
	}
	got, found, err = s.GetRemoteHost(context.Background(), host.HostID)
	if err != nil || !found || got.LastProbeSucceeded || got.LastProbeError != "connection refused" {
		t.Fatalf("failed probe round trip = %+v, found=%v err=%v", got, found, err)
	}
}
