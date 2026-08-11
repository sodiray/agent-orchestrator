package controllers_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	federationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/federation"
	projectsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/project"
	remotehostsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/remotehost"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

type boardInventoryProvider struct {
	hosts []remotehostsvc.InventoryHost
}

func (i boardInventoryProvider) List(context.Context) ([]remotehostsvc.InventoryHost, error) {
	return append([]remotehostsvc.InventoryHost(nil), i.hosts...), nil
}

func TestProjectsAPIBoardIncludesEveryInventoryHost(t *testing.T) {
	store := sqlitetest.MustOpen(t)
	remoteHosts := remotehostsvc.New(remotehostsvc.Deps{
		Store: store,
		Inventory: boardInventoryProvider{hosts: []remotehostsvc.InventoryHost{
			{HostID: "running-host", Label: "Running host", Lifecycle: remotehostsvc.InventoryLifecycleRunning},
			{HostID: "stopped-host", Label: "Stopped host", Lifecycle: remotehostsvc.InventoryLifecycleStopped},
		}},
	})
	if err := remoteHosts.LoadPresence(t.Context()); err != nil {
		t.Fatalf("load presence: %v", err)
	}
	federation := federationsvc.New(federationsvc.Deps{
		Projects: projectsvc.New(store),
		Store:    store,
		Presence: remoteHosts,
	})
	router := httpd.NewRouterWithControl(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, httpd.APIDeps{
		Projects:    projectsvc.New(store),
		RemoteHosts: remoteHosts,
		Federation:  federation,
	}, httpd.ControlDeps{})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/projects", nil))
	body, status := recorder.Body.Bytes(), recorder.Code
	if status != http.StatusOK {
		t.Fatalf("GET projects = %d, want 200; body=%s", status, body)
	}
	var response struct {
		RemoteHosts []struct {
			HostID string `json:"hostId"`
			State  string `json:"state"`
		} `json:"remoteHosts"`
	}
	mustJSON(t, body, &response)
	states := make(map[string]string, len(response.RemoteHosts))
	for _, host := range response.RemoteHosts {
		states[host.HostID] = host.State
	}
	if len(states) != 2 || states["running-host"] != "unreachable" || states["stopped-host"] != "stopped" {
		t.Fatalf("board remote hosts = %#v", response.RemoteHosts)
	}
}
