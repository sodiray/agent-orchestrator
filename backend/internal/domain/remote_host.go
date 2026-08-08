package domain

import (
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const maxRemoteHostIDLength = 63

const qualifiedSessionSeparator = "~"

var remoteHostIDPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)

type RemoteHostID string

type RemoteHostState string

const (
	RemoteHostStateAvailable   RemoteHostState = "available"
	RemoteHostStateUnreachable RemoteHostState = "unreachable"
	RemoteHostStateStopped     RemoteHostState = "stopped"
	RemoteHostStateDestroyed   RemoteHostState = "destroyed"
)

type RemoteHost struct {
	HostID             RemoteHostID
	Address            string
	Label              string
	OperatorState      RemoteHostState
	LastProbeAt        time.Time
	LastProbeSucceeded bool
	LastProbeError     string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// QualifiedSessionID identifies a session that is owned by a registered remote
// daemon. Local session IDs intentionally remain bare.
type QualifiedSessionID struct {
	HostID    RemoteHostID
	SessionID SessionID
}

// QualifySessionID formats a remote session identity for the local API.
func QualifySessionID(hostID RemoteHostID, sessionID SessionID) SessionID {
	return SessionID(string(hostID) + qualifiedSessionSeparator + string(sessionID))
}

// ParseQualifiedSessionID recognizes the remote-only hostId~sessionId form.
// Any ID that does not meet this exact form is local by definition.
func ParseQualifiedSessionID(id SessionID) (QualifiedSessionID, bool) {
	parts := strings.Split(string(id), qualifiedSessionSeparator)
	if len(parts) != 2 || parts[1] == "" || strings.Contains(parts[1], qualifiedSessionSeparator) {
		return QualifiedSessionID{}, false
	}
	hostID := RemoteHostID(parts[0])
	if ValidateRemoteHostID(hostID) != nil {
		return QualifiedSessionID{}, false
	}
	return QualifiedSessionID{HostID: hostID, SessionID: SessionID(parts[1])}, true
}

// RemoteSessionSnapshot is the last read model received from a remote daemon.
// View retains its owning daemon's wire representation verbatim so a local
// daemon never derives display state for a remote session.
type RemoteSessionSnapshot struct {
	HostID     RemoteHostID
	SessionID  SessionID
	View       json.RawMessage
	ObservedAt time.Time
}

func ValidateRemoteHostID(id RemoteHostID) error {
	if len(id) == 0 || len(id) > maxRemoteHostIDLength || !remoteHostIDPattern.MatchString(string(id)) {
		return fmt.Errorf("remote host id must be a lowercase slug of at most %d characters", maxRemoteHostIDLength)
	}
	return nil
}

func ValidateRemoteHostAddress(address string) error {
	if strings.TrimSpace(address) != address || address == "" || len(address) > 255 {
		return fmt.Errorf("remote host address must be a host:port")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return fmt.Errorf("remote host address must be a host:port")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("remote host address must include a port between 1 and 65535")
	}
	return nil
}

func (h RemoteHost) CurrentState() RemoteHostState {
	if h.OperatorState == RemoteHostStateStopped || h.OperatorState == RemoteHostStateDestroyed {
		return h.OperatorState
	}
	if h.LastProbeSucceeded {
		return RemoteHostStateAvailable
	}
	return RemoteHostStateUnreachable
}
