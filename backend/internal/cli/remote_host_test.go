package cli

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
	remotehostsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/remotehost"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
)

type cliRemoteHostInventory struct {
	hosts []remotehostsvc.InventoryHost
}

func (i cliRemoteHostInventory) List(_ context.Context) ([]remotehostsvc.InventoryHost, error) {
	return append([]remotehostsvc.InventoryHost(nil), i.hosts...), nil
}

type cliRemoteHostProber struct{}

func (cliRemoteHostProber) Probe(context.Context, string) error { return nil }

func TestRemoteHostListJSONExcludesInventoryOnlyHosts(t *testing.T) {
	store := sqlitetest.MustOpen(t)
	remoteHosts := remotehostsvc.New(remotehostsvc.Deps{
		Store:     store,
		Prober:    cliRemoteHostProber{},
		Inventory: cliRemoteHostInventory{hosts: []remotehostsvc.InventoryHost{{HostID: "inventory-only", Label: "Inventory only", Lifecycle: remotehostsvc.InventoryLifecycleStopped}}},
	})
	if err := remoteHosts.LoadPresence(t.Context()); err != nil {
		t.Fatalf("load presence: %v", err)
	}
	if _, err := remoteHosts.Register(t.Context(), remotehostsvc.RegisterInput{HostID: "registered-host", Address: "127.0.0.1:3001"}); err != nil {
		t.Fatalf("register host: %v", err)
	}

	router := httpd.NewRouterWithControl(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, httpd.APIDeps{RemoteHosts: remoteHosts}, httpd.ControlDeps{})
	cfg := setConfigEnv(t)
	if err := runfile.Write(cfg.runFile, runfile.Info{PID: 1, Port: 3001}); err != nil {
		t.Fatalf("write run file: %v", err)
	}

	out, errOut, err := executeCLI(t, Deps{
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			return recorder.Result(), nil
		})},
		ProcessAlive: func(int) bool { return true },
	}, "remote-host", "ls", "--json")
	if err != nil {
		t.Fatalf("remote-host ls --json: %v\nstderr=%s", err, errOut)
	}
	var result remoteHostListResult
	if err := json.Unmarshal([]byte(out), &result); err != nil {
		t.Fatalf("decode list JSON: %v\nout=%s", err, out)
	}
	if len(result.RemoteHosts) != 1 || result.RemoteHosts[0].HostID != "registered-host" {
		t.Fatalf("remote-host ls result = %#v", result.RemoteHosts)
	}
}
