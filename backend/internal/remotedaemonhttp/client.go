// Package remotedaemonhttp constructs HTTP clients for registered remote daemons.
package remotedaemonhttp

import (
	"errors"
	"net/http"
	"time"
)

// ErrRedirect identifies an attempt by a remote daemon to select another destination.
var ErrRedirect = errors.New("remote daemon redirects are not allowed")

// NewClient constructs a remote-daemon client with the given request timeout.
func NewClient(timeout time.Duration) *http.Client {
	return EnforceRedirectRefusal(&http.Client{Timeout: timeout})
}

// EnforceRedirectRefusal preserves a supplied client's transport and timeout in
// a copy so later caller changes cannot weaken remote-daemon redirect refusal.
func EnforceRedirectRefusal(client *http.Client) *http.Client {
	if client == nil {
		client = &http.Client{}
	}
	remoteClient := *client
	remoteClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return ErrRedirect
	}
	return &remoteClient
}
