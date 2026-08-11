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

func TestUpsertRemoteHostUpdatesAnExistingRegistration(t *testing.T) {
	s := newTestStore(t)
	createdAt := time.Date(2026, 8, 7, 1, 2, 3, 0, time.UTC)
	initial := domain.RemoteHost{
		HostID:             "lab-host",
		Address:            "127.0.0.1:3001",
		Label:              "Original",
		OperatorState:      domain.RemoteHostStateStopped,
		LastProbeAt:        createdAt,
		LastProbeSucceeded: true,
		CreatedAt:          createdAt,
		UpdatedAt:          createdAt,
	}
	created, err := s.UpsertRemoteHost(context.Background(), initial)
	if err != nil || !created {
		t.Fatalf("initial upsert: created=%v err=%v", created, err)
	}
	updatedAt := createdAt.Add(time.Minute)
	replacement := domain.RemoteHost{
		HostID:         initial.HostID,
		Address:        "127.0.0.1:3002",
		Label:          "Replacement",
		LastProbeAt:    updatedAt,
		LastProbeError: "probe has not completed",
		CreatedAt:      updatedAt,
		UpdatedAt:      updatedAt,
	}
	created, err = s.UpsertRemoteHost(context.Background(), replacement)
	if err != nil || created {
		t.Fatalf("replacement upsert: created=%v err=%v", created, err)
	}
	got, found, err := s.GetRemoteHost(context.Background(), initial.HostID)
	if err != nil || !found {
		t.Fatalf("get replacement: found=%v err=%v", found, err)
	}
	if got.Address != replacement.Address || got.Label != replacement.Label || got.OperatorState != "" || got.LastProbeError != replacement.LastProbeError || !got.CreatedAt.Equal(createdAt) || !got.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("replacement = %+v", got)
	}
}

func TestRemoteSessionSnapshotsRoundTrip(t *testing.T) {
	s := newTestStore(t)
	now := time.Date(2026, 8, 7, 1, 2, 3, 0, time.UTC)
	host := domain.RemoteHost{
		HostID:             "lab-host",
		Address:            "127.0.0.1:3001",
		LastProbeAt:        now,
		LastProbeSucceeded: true,
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	if created, err := s.CreateRemoteHost(context.Background(), host); err != nil || !created {
		t.Fatalf("create remote host: created=%v err=%v", created, err)
	}
	if err := s.ReplaceRemoteSessionSnapshots(context.Background(), host.HostID, []domain.RemoteSessionSnapshot{{
		SessionID:  "project-7",
		View:       []byte(`{"id":"project-7","status":"idle"}`),
		ObservedAt: now,
	}}); err != nil {
		t.Fatalf("replace remote session snapshots: %v", err)
	}
	snapshots, err := s.ListRemoteSessionSnapshots(context.Background(), host.HostID)
	if err != nil {
		t.Fatalf("list remote session snapshots: %v", err)
	}
	if len(snapshots) != 1 || snapshots[0].SessionID != "project-7" || string(snapshots[0].View) != `{"id":"project-7","status":"idle"}` {
		t.Fatalf("snapshots = %#v", snapshots)
	}
}
