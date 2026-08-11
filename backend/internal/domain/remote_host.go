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

// RemoteHostID is the stable operator-chosen slug used to route a qualified
// identity back to its owning registered daemon.
type RemoteHostID string

// RemoteHostState describes either the latest connectivity result or an
// operator-imposed lifecycle state for a registered remote daemon.
type RemoteHostState string

// Remote host states separate intentional operator stops from probe-derived
// availability, allowing the UI to explain why a host is unavailable.
const (
	RemoteHostStateAvailable   RemoteHostState = "available"
	RemoteHostStateUnreachable RemoteHostState = "unreachable"
	RemoteHostStateStopped     RemoteHostState = "stopped"
	RemoteHostStateDestroyed   RemoteHostState = "destroyed"
)

// RemoteHost is the durable registry record and most recent probe outcome for
// a daemon whose sessions may be federated into this AO instance.
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

// QualifiedNotificationID identifies a notification owned by a remote daemon.
// Local notification IDs intentionally remain bare.
type QualifiedNotificationID struct {
	HostID         RemoteHostID
	NotificationID string
}

// QualifySessionID formats a remote session identity for the local API.
func QualifySessionID(hostID RemoteHostID, sessionID SessionID) SessionID {
	return SessionID(string(hostID) + qualifiedSessionSeparator + string(sessionID))
}

// QualifyRemoteSessionID makes an owning daemon's bare session identity safe
// for use at the federation boundary.
func QualifyRemoteSessionID(hostID RemoteHostID, sessionID SessionID) (SessionID, error) {
	if err := ValidateRemoteHostID(hostID); err != nil {
		return "", err
	}
	if sessionID == "" || strings.Contains(string(sessionID), qualifiedSessionSeparator) {
		return "", fmt.Errorf("remote session id must be a non-empty bare id")
	}
	return QualifySessionID(hostID, sessionID), nil
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

// QualifyNotificationID formats a remote notification identity for the local API.
func QualifyNotificationID(hostID RemoteHostID, notificationID string) string {
	return string(hostID) + qualifiedSessionSeparator + notificationID
}

// ParseQualifiedNotificationID recognizes the remote-only hostId~notificationId form.
// A non-matching ID is local by definition.
func ParseQualifiedNotificationID(id string) (QualifiedNotificationID, bool) {
	parts := strings.Split(id, qualifiedSessionSeparator)
	if len(parts) != 2 || parts[1] == "" || strings.Contains(parts[1], qualifiedSessionSeparator) {
		return QualifiedNotificationID{}, false
	}
	hostID := RemoteHostID(parts[0])
	if ValidateRemoteHostID(hostID) != nil {
		return QualifiedNotificationID{}, false
	}
	return QualifiedNotificationID{HostID: hostID, NotificationID: parts[1]}, true
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

// ValidateRemoteHostID enforces the bounded slug form required to safely embed
// a host identity in qualified IDs and API routes.
func ValidateRemoteHostID(id RemoteHostID) error {
	if len(id) == 0 || len(id) > maxRemoteHostIDLength || !remoteHostIDPattern.MatchString(string(id)) {
		return fmt.Errorf("remote host id must be a lowercase slug of at most %d characters", maxRemoteHostIDLength)
	}
	return nil
}

// ValidateRemoteHostAddress accepts only a normalized host:port endpoint so
// remote client construction never needs to interpret an arbitrary URL.
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

// CurrentState prioritizes an operator's stop or destroy decision over a
// previous probe result; otherwise it exposes the latest observed reachability.
func (h RemoteHost) CurrentState() RemoteHostState {
	if h.OperatorState == RemoteHostStateStopped || h.OperatorState == RemoteHostStateDestroyed {
		return h.OperatorState
	}
	if h.LastProbeSucceeded {
		return RemoteHostStateAvailable
	}
	return RemoteHostStateUnreachable
}
