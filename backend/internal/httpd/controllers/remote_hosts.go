package controllers

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apispec"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	remotehostsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/remotehost"
)

// RemoteHostsController exposes registered remote-daemon lifecycle operations
// while leaving validation and persistence policy to the remote-host service.
type RemoteHostsController struct {
	Mgr remotehostsvc.Manager
}

// Register mounts the remote-host registry routes on the versioned API router.
func (c *RemoteHostsController) Register(r chi.Router) {
	r.Get("/remote-hosts", c.list)
	r.Post("/remote-hosts", c.register)
	r.Get("/remote-hosts/{hostId}", c.get)
	r.Patch("/remote-hosts/{hostId}/state", c.updateState)
	r.Delete("/remote-hosts/{hostId}", c.deregister)
}

func (c *RemoteHostsController) list(w http.ResponseWriter, r *http.Request) {
	if c.Mgr == nil {
		apispec.NotImplemented(w, r, http.MethodGet, "/api/v1/remote-hosts")
		return
	}
	hosts, err := c.Mgr.List(r.Context())
	if err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, ListRemoteHostsResponse{RemoteHosts: hosts})
}

func (c *RemoteHostsController) register(w http.ResponseWriter, r *http.Request) {
	if c.Mgr == nil {
		apispec.NotImplemented(w, r, http.MethodPost, "/api/v1/remote-hosts")
		return
	}
	var in remotehostsvc.RegisterInput
	if err := decodeJSONStrict(r, &in); err != nil {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_JSON", "Invalid JSON body", nil)
		return
	}
	host, err := c.Mgr.Register(r.Context(), in)
	if err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	envelope.WriteJSON(w, http.StatusCreated, RemoteHostResponse{RemoteHost: host})
}

func (c *RemoteHostsController) get(w http.ResponseWriter, r *http.Request) {
	if c.Mgr == nil {
		apispec.NotImplemented(w, r, http.MethodGet, "/api/v1/remote-hosts/{hostId}")
		return
	}
	host, err := c.Mgr.Get(r.Context(), remoteHostID(r))
	if err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, RemoteHostResponse{RemoteHost: host})
}

func (c *RemoteHostsController) updateState(w http.ResponseWriter, r *http.Request) {
	if c.Mgr == nil {
		apispec.NotImplemented(w, r, http.MethodPatch, "/api/v1/remote-hosts/{hostId}/state")
		return
	}
	var in UpdateRemoteHostStateRequest
	if err := decodeJSONStrict(r, &in); err != nil {
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_JSON", "Invalid JSON body", nil)
		return
	}
	host, err := c.Mgr.UpdateState(r.Context(), remoteHostID(r), domain.RemoteHostState(in.State))
	if err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, RemoteHostResponse{RemoteHost: host})
}

func (c *RemoteHostsController) deregister(w http.ResponseWriter, r *http.Request) {
	if c.Mgr == nil {
		apispec.NotImplemented(w, r, http.MethodDelete, "/api/v1/remote-hosts/{hostId}")
		return
	}
	if err := c.Mgr.Deregister(r.Context(), remoteHostID(r)); err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, DeregisterRemoteHostResponse{HostID: chi.URLParam(r, "hostId")})
}

func remoteHostID(r *http.Request) domain.RemoteHostID {
	return domain.RemoteHostID(chi.URLParam(r, "hostId"))
}
