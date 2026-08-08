package domain

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const maxRemoteHostIDLength = 63

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
