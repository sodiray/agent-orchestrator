package httpd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/controllers"
	federationsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/federation"
)

const notificationEventsLiveBuffer = 64

// federatedNotificationStream keeps the controller's local hub semantics and
// adds one live owner subscription for each registered remote host.
type federatedNotificationStream struct {
	local      controllers.NotificationStream
	federation *federationsvc.Service
	client     *http.Client
	log        *slog.Logger
}

func newFederatedNotificationStream(local controllers.NotificationStream, federation *federationsvc.Service) controllers.NotificationStream {
	if local == nil || federation == nil {
		return local
	}
	return &federatedNotificationStream{local: local, federation: federation, client: newRemoteDaemonClient(0), log: slog.Default()}
}

func (s *federatedNotificationStream) Subscribe(projectID domain.ProjectID) (<-chan domain.NotificationEvent, func()) {
	// This guard preserves the existing single-machine stream exactly: no host
	// registry read, client, goroutine, or subscription beyond the local hub.
	if !s.federation.HasRegisteredHosts() {
		return s.local.Subscribe(projectID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan domain.NotificationEvent, notificationEventsLiveBuffer)
	local, unsubscribeLocal := s.local.Subscribe(projectID)
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		defer group.Done()
		forwardNotificationEvents(ctx, local, out)
	}()
	hosts, err := s.federation.RemoteHosts(ctx)
	if err != nil {
		s.logger().Error("list remote hosts for notification stream failed", "err", err)
	} else {
		for _, host := range hosts {
			if host.OperatorState == domain.RemoteHostStateStopped || host.OperatorState == domain.RemoteHostStateDestroyed {
				s.logger().Warn("remote notification stream unavailable", "hostId", host.HostID, "reason", host.OperatorState)
				continue
			}
			group.Add(1)
			go func(host domain.RemoteHost) {
				defer group.Done()
				s.forwardRemoteNotificationEvents(ctx, host, projectID, out)
			}(host)
		}
	}
	var once sync.Once
	return out, func() {
		once.Do(func() {
			cancel()
			unsubscribeLocal()
			group.Wait()
			close(out)
		})
	}
}

func forwardNotificationEvents(ctx context.Context, in <-chan domain.NotificationEvent, out chan<- domain.NotificationEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-in:
			if !ok {
				return
			}
			select {
			case out <- event:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *federatedNotificationStream) forwardRemoteNotificationEvents(ctx context.Context, host domain.RemoteHost, projectID domain.ProjectID, out chan<- domain.NotificationEvent) {
	for attempt := 0; ; attempt++ {
		err := s.readRemoteNotificationEvents(ctx, host, projectID, func(event domain.NotificationEvent) error {
			event.Record.ID = domain.QualifyNotificationID(host.HostID, event.Record.ID)
			event.Record.SessionID = domain.QualifySessionID(host.HostID, event.Record.SessionID)
			select {
			case out <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.logger().Warn("remote notification stream dropped", "hostId", host.HostID, "address", host.Address, "err", err)
		} else {
			s.logger().Warn("remote notification stream ended", "hostId", host.HostID, "address", host.Address)
		}
		if !waitForRemoteEventsRetry(ctx, attempt) {
			return
		}
	}
}

func (s *federatedNotificationStream) readRemoteNotificationEvents(ctx context.Context, host domain.RemoteHost, projectID domain.ProjectID, handle func(domain.NotificationEvent) error) error {
	endpoint := url.URL{Scheme: "http", Host: host.Address, Path: "/api/v1/notifications/stream"}
	query := endpoint.Query()
	query.Set("tail", "1")
	if projectID != "" {
		query.Set("projectId", string(projectID))
	}
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), http.NoBody) // #nosec G704 -- target is a registered remote-host endpoint.
	if err != nil {
		return fmt.Errorf("build remote notification stream request: %w", err)
	}
	req.Header.Set("X-AO-Federation-Local", "1")
	resp, err := s.client.Do(req) // #nosec G704 -- target is a registered remote-host endpoint.
	if err != nil {
		return fmt.Errorf("request remote notification stream: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("remote notification stream returned HTTP %d", resp.StatusCode)
	}
	return readNotificationSSE(resp.Body, handle)
}

func readNotificationSSE(body io.Reader, handle func(domain.NotificationEvent) error) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	name := ""
	data := make([]string, 0, 1)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if len(data) == 0 {
				continue
			}
			event, err := notificationEventFromSSE(name, data)
			if err != nil {
				return err
			}
			if err := handle(event); err != nil {
				return err
			}
			name = ""
			data = data[:0]
			continue
		}
		if value, ok := strings.CutPrefix(line, "event: "); ok {
			name = value
			continue
		}
		if value, ok := strings.CutPrefix(line, "data: "); ok {
			data = append(data, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func notificationEventFromSSE(name string, lines []string) (domain.NotificationEvent, error) {
	kind := domain.NotificationCreated
	if name == "notification_resolved" {
		kind = domain.NotificationResolved
	} else if name != "notification_created" {
		return domain.NotificationEvent{}, fmt.Errorf("unknown notification event %q", name)
	}
	var record domain.NotificationRecord
	if err := json.Unmarshal([]byte(strings.Join(lines, "\n")), &record); err != nil {
		return domain.NotificationEvent{}, fmt.Errorf("decode remote notification event: %w", err)
	}
	if record.ID == "" || strings.Contains(record.ID, "~") || strings.Contains(string(record.SessionID), "~") {
		return domain.NotificationEvent{}, fmt.Errorf("remote notification event is invalid")
	}
	if err := record.Validate(); err != nil {
		return domain.NotificationEvent{}, fmt.Errorf("remote notification event is invalid: %w", err)
	}
	return domain.NotificationEvent{Kind: kind, Record: record}, nil
}

func (s *federatedNotificationStream) logger() *slog.Logger {
	if s.log != nil {
		return s.log
	}
	return slog.Default()
}
