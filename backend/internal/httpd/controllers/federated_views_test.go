package controllers

import (
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	federationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/federation"
)

func TestFederatedSessionViewsQualifiesAndMarksUnavailableRemoteSessions(t *testing.T) {
	views := federatedSessionViews([]federationsvc.ListedSession{{
		Remote: &federationsvc.RemoteSession{
			HostID:            "workstation",
			SessionID:         "project-7",
			View:              []byte(`{"id":"project-7","projectId":"project","kind":"worker","status":"idle","prs":[]}`),
			UnavailableReason: "connection refused",
		},
	}})
	if len(views) != 1 {
		t.Fatalf("views = %#v", views)
	}
	view := views[0]
	if view.ID != domain.SessionID("workstation~project-7") || view.HostID != "workstation" {
		t.Fatalf("identity = (%q, %q)", view.ID, view.HostID)
	}
	if view.Availability != "unavailable" || view.UnavailableReason != "connection refused" {
		t.Fatalf("availability = (%q, %q)", view.Availability, view.UnavailableReason)
	}
}
