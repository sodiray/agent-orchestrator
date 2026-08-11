package remotehost

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// InventoryLifecycle distinguishes hosts that an external inventory reports as
// runnable from ones intentionally stopped by its source system.
type InventoryLifecycle string

// Inventory lifecycle values are constrained so inventory refreshes cannot
// introduce an ambiguous host state into the registry view.
const (
	InventoryLifecycleRunning InventoryLifecycle = "running"
	InventoryLifecycleStopped InventoryLifecycle = "stopped"
)

// InventoryHost is a validated row supplied by the optional external host
// inventory command before it is reconciled with registered hosts.
type InventoryHost struct {
	HostID    domain.RemoteHostID `json:"id"`
	Label     string              `json:"label"`
	Lifecycle InventoryLifecycle  `json:"lifecycle"`
	Address   string              `json:"address,omitempty"`
}

// InventoryProvider lists externally managed hosts, keeping provider-specific
// command execution outside the remote-host service.
type InventoryProvider interface {
	List(context.Context) ([]InventoryHost, error)
}

// CommandInventoryProvider obtains inventory JSON by running the command the
// local operator configured for this daemon.
type CommandInventoryProvider struct {
	argv      []string
	timeout   time.Duration
	maxOutput int64
}

// NewCommandInventoryProvider validates execution limits and copies argv so a
// caller cannot mutate the command after the configuration has been accepted.
func NewCommandInventoryProvider(argv []string, timeout time.Duration, maxOutput int64) (*CommandInventoryProvider, error) {
	if len(argv) == 0 || strings.TrimSpace(argv[0]) == "" {
		return nil, errors.New("inventory command argv must contain an executable")
	}
	if timeout <= 0 {
		return nil, errors.New("inventory command timeout must be positive")
	}
	if maxOutput <= 0 {
		return nil, errors.New("inventory command output limit must be positive")
	}
	return &CommandInventoryProvider{argv: append([]string(nil), argv...), timeout: timeout, maxOutput: maxOutput}, nil
}

// List runs the operator-configured inventory command and validates its single
// JSON document before publishing any host rows to the service.
func (p *CommandInventoryProvider) List(ctx context.Context) ([]InventoryHost, error) {
	commandCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	command := exec.CommandContext(commandCtx, p.argv[0], p.argv[1:]...) // #nosec G702 -- argv comes from user-owned local config and is validated at load by validateCommandArgs.
	output := &limitedBuffer{limit: p.maxOutput}
	command.Stdout = output
	stderr := &limitedBuffer{limit: p.maxOutput}
	command.Stderr = stderr
	err := command.Run()
	if errors.Is(output.err, errOutputTooLarge) || errors.Is(stderr.err, errOutputTooLarge) {
		return nil, fmt.Errorf("inventory command output exceeds %d bytes", p.maxOutput)
	}
	if err != nil {
		if errors.Is(commandCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("inventory command timed out after %s", p.timeout)
		}
		if message := strings.TrimSpace(string(stderr.Bytes())); message != "" {
			return nil, fmt.Errorf("inventory command failed: %w: %s", err, message)
		}
		return nil, fmt.Errorf("inventory command failed: %w", err)
	}
	var hosts []InventoryHost
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&hosts); err != nil {
		return nil, fmt.Errorf("decode inventory JSON: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode inventory JSON: %w", err)
	}
	seen := make(map[domain.RemoteHostID]struct{}, len(hosts))
	for index, host := range hosts {
		if err := domain.ValidateRemoteHostID(host.HostID); err != nil {
			return nil, fmt.Errorf("inventory host %d: %w", index, err)
		}
		if _, exists := seen[host.HostID]; exists {
			return nil, fmt.Errorf("inventory host %d: duplicate id %q", index, host.HostID)
		}
		seen[host.HostID] = struct{}{}
		if strings.TrimSpace(host.Label) == "" || len(host.Label) > 120 {
			return nil, fmt.Errorf("inventory host %d: label must be between 1 and 120 characters", index)
		}
		if host.Lifecycle != InventoryLifecycleRunning && host.Lifecycle != InventoryLifecycleStopped {
			return nil, fmt.Errorf("inventory host %d: lifecycle must be running or stopped", index)
		}
		if host.Address != "" {
			if err := domain.ValidateRemoteHostAddress(host.Address); err != nil {
				return nil, fmt.Errorf("inventory host %d: %w", index, err)
			}
		}
	}
	return hosts, nil
}

var errOutputTooLarge = errors.New("inventory command output too large")

type limitedBuffer struct {
	buffer bytes.Buffer
	limit  int64
	err    error
}

func (b *limitedBuffer) Write(data []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if int64(b.buffer.Len()+len(data)) > b.limit {
		b.err = errOutputTooLarge
		return 0, b.err
	}
	return b.buffer.Write(data)
}

func (b *limitedBuffer) Bytes() []byte { return b.buffer.Bytes() }

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}
