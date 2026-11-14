package pi

import (
	"context"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/sllt/tao/actor"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/infra"
)

const (
	metricWorkerRestartsTotal   = "pi_worker_restarts_total"
	metricActorRestartsTotal    = "pi_actor_restarts_total"
	metricWorkerBackoffSeconds  = "pi_worker_backoff_seconds"
	metricCronRunsTotal         = "pi_cron_runs_total"
	metricCronSkippedTotal      = "pi_cron_skipped_total"
	metricCronDurationSeconds   = "pi_cron_duration_seconds"
	metricPubSubFailuresTotal   = "pi_pubsub_failures_total"
	metricWSDroppedMessages     = "pi_ws_dropped_messages_total"
	metricWSWriteErrors         = "pi_ws_write_errors_total"
)

var supervisionDurationBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60,
}

func (a *App) ensureSupervisionMetrics() {
	if a == nil || a.container == nil {
		return
	}
	m := a.container.Metrics()
	if m == nil {
		return
	}
	if _, isStrictMock := m.(*infra.MockMetrics); isStrictMock {
		return
	}
	a.supMetricsOnce.Do(func() {
		m.NewCounter(metricWorkerRestartsTotal, "Total supervised business worker or client retry restarts.")
		m.NewCounter(metricActorRestartsTotal, "Total supervised actor controller restarts.")
		m.NewHistogram(metricWorkerBackoffSeconds, "Backoff duration in seconds before supervised retries.", supervisionDurationBuckets...)
		m.NewCounter(metricCronRunsTotal, "Total supervised cron job executions by status.")
		m.NewCounter(metricCronSkippedTotal, "Total skipped supervised cron job triggers by reason.")
		m.NewHistogram(metricCronDurationSeconds, "Duration of supervised cron job executions in seconds.", supervisionDurationBuckets...)
		m.NewCounter(metricPubSubFailuresTotal, "Total supervised pubsub failures by topic and stage.")
		m.NewCounter(metricWSDroppedMessages, "Total server WebSocket outbound messages dropped due to slow consumers.")
		m.NewCounter(metricWSWriteErrors, "Total server WebSocket write errors.")
		a.supMetricsReady.Store(true)
	})
}

func (a *App) recordWorkerRestartMetric(component, kind string) {
	if a == nil || !a.supMetricsReady.Load() || a.container == nil || a.container.Metrics() == nil {
		return
	}
	a.container.Metrics().IncrementCounter(context.Background(), metricWorkerRestartsTotal, "component", component, "kind", kind)
}

func (a *App) recordActorRestartMetric(component, kind string) {
	if a == nil || !a.supMetricsReady.Load() || a.container == nil || a.container.Metrics() == nil {
		return
	}
	a.container.Metrics().IncrementCounter(context.Background(), metricActorRestartsTotal, "component", component, "kind", kind)
}

func (a *App) recordWorkerBackoffMetric(component, kind string, delay time.Duration) {
	if a == nil || !a.supMetricsReady.Load() || a.container == nil || a.container.Metrics() == nil {
		return
	}
	a.container.Metrics().RecordHistogram(context.Background(), metricWorkerBackoffSeconds, delay.Seconds(), "component", component, "kind", kind)
}

func (a *App) recordCronRunMetric(jobName, status string, dur time.Duration) {
	if a == nil || !a.supMetricsReady.Load() || a.container == nil || a.container.Metrics() == nil {
		return
	}
	m := a.container.Metrics()
	m.IncrementCounter(context.Background(), metricCronRunsTotal, "job", jobName, "status", status)
	m.RecordHistogram(context.Background(), metricCronDurationSeconds, dur.Seconds(), "job", jobName)
}

func (a *App) recordCronSkippedMetric(jobName, reason string) {
	if a == nil || !a.supMetricsReady.Load() || a.container == nil || a.container.Metrics() == nil {
		return
	}
	a.container.Metrics().IncrementCounter(context.Background(), metricCronSkippedTotal, "job", jobName, "reason", reason)
}

func (a *App) recordPubSubFailureMetric(topic, stage string) {
	if a == nil || !a.supMetricsReady.Load() || a.container == nil || a.container.Metrics() == nil {
		return
	}
	a.container.Metrics().IncrementCounter(context.Background(), metricPubSubFailuresTotal, "topic", topic, "stage", stage)
}

func (a *App) recordWSSlowDropMetric() {
	if a == nil {
		return
	}
	a.wsServerDroppedTotal.Add(1)
	if !a.supMetricsReady.Load() || a.container == nil || a.container.Metrics() == nil {
		return
	}
	a.container.Metrics().IncrementCounter(context.Background(), metricWSDroppedMessages)
}

func (a *App) recordWSWriteErrorMetric() {
	if a == nil {
		return
	}
	a.wsServerWriteErrTotal.Add(1)
	if !a.supMetricsReady.Load() || a.container == nil || a.container.Metrics() == nil {
		return
	}
	a.container.Metrics().IncrementCounter(context.Background(), metricWSWriteErrors)
}

// RuntimeSnapshot provides a point-in-time, redacted, read-only diagnostic view of an App instance
// without exposing any network debug endpoints.
type RuntimeSnapshot struct {
	State        string                      `json:"state"`
	Startup      bool                        `json:"startup"`
	Alive        bool                        `json:"alive"`
	Ready        bool                        `json:"ready"`
	IntakeClosed bool                        `json:"intakeClosed"`
	FatalError   string                      `json:"fatalError,omitempty"`
	Config       map[string]string           `json:"config"`
	ActorTree    *actor.NodeSnapshot         `json:"actorTree,omitempty"`
	Workers      []WorkerRuntimeSnapshot     `json:"workers,omitempty"`
	CronJobs     []CronJobRuntimeSnapshot    `json:"cronJobs,omitempty"`
	Subscribers  []SubscriberRuntimeSnapshot `json:"subscribers,omitempty"`
	WSClients    []WSClientRuntimeSnapshot   `json:"wsClients,omitempty"`
	WSServer     WSServerRuntimeSnapshot     `json:"wsServer"`
}

// WorkerRuntimeSnapshot summarizes a supervised background worker.
type WorkerRuntimeSnapshot struct {
	Name          string          `json:"name"`
	ActorName     string          `json:"actorName"`
	State         string          `json:"state"`
	Mode          SupervisionMode `json:"mode"`
	Required      bool            `json:"required"`
	WaitReady     bool            `json:"waitReady"`
	Ready         bool            `json:"ready"`
	Generation    uint64          `json:"generation"`
	WorkerRetries uint64          `json:"workerRetries"`
	ActorRestarts uint64          `json:"actorRestarts"`
	LastError     string          `json:"lastError,omitempty"`
}

// CronJobRuntimeSnapshot summarizes a supervised cron job.
type CronJobRuntimeSnapshot struct {
	Name              string            `json:"name"`
	ActorName         string            `json:"actorName"`
	Schedule          string            `json:"schedule"`
	Overlap           CronOverlapPolicy `json:"overlap"`
	MaxConcurrency    int               `json:"maxConcurrency"`
	ActiveRuns        int               `json:"activeRuns"`
	QueuedNext        bool              `json:"queuedNext"`
	RunsTotal         uint64            `json:"runsTotal"`
	SkippedTotal      uint64            `json:"skippedTotal"`
	DroppedQueueTotal uint64            `json:"droppedQueueTotal"`
	FailuresTotal     uint64            `json:"failuresTotal"`
	PanicsTotal       uint64            `json:"panicsTotal"`
	ActorRestarts     uint64            `json:"actorRestarts"`
	LastDuration      time.Duration     `json:"lastDuration"`
	LastError         string            `json:"lastError,omitempty"`
}

// SubscriberRuntimeSnapshot summarizes a supervised Pub/Sub topic consumer.
type SubscriberRuntimeSnapshot struct {
	Topic               string                 `json:"topic"`
	CoordActorName      string                 `json:"coordActorName"`
	State               string                 `json:"state"`
	AckMode             string                 `json:"ackMode"`
	Concurrency         int                    `json:"concurrency"`
	QueueSize           int                    `json:"queueSize"`
	Required            bool                   `json:"required"`
	Shutdown            SubscriberShutdownMode `json:"shutdown"`
	Ready               bool                   `json:"ready"`
	InFlightTickets     int                 `json:"inFlightTickets"`
	FrozenPartitions    []int               `json:"frozenPartitions,omitempty"`
	ProcessedTotal      uint64              `json:"processedTotal"`
	CommittedTotal      uint64              `json:"committedTotal"`
	FailuresTotal       uint64              `json:"failuresTotal"`
	PanicsTotal         uint64              `json:"panicsTotal"`
	HandlerRetriesTotal uint64              `json:"handlerRetriesTotal"`
	SendRejectedTotal   uint64              `json:"sendRejectedTotal"`
	CommitFailuresTotal uint64              `json:"commitFailuresTotal"`
	CommitUnknownTotal  uint64              `json:"commitUnknownTotal"`
	DLQSentTotal        uint64              `json:"dlqSentTotal"`
	DLQSkippedTotal     uint64              `json:"dlqSkippedTotal"`
	DLQFailuresTotal    uint64              `json:"dlqFailuresTotal"`
	AbandonedTotal      uint64              `json:"abandonedTotal"`
	CoordRestarts       uint64              `json:"coordRestarts"`
	WorkerRestarts      uint64              `json:"workerRestarts"`
	LastRecoveryAction  string              `json:"lastRecoveryAction,omitempty"`
	LastError           string              `json:"lastError,omitempty"`
}

// WSClientRuntimeSnapshot summarizes a supervised outbound WebSocket client.
type WSClientRuntimeSnapshot struct {
	ServiceName   string `json:"serviceName"`
	ActorName     string `json:"actorName"`
	URL           string `json:"url"`
	State         string `json:"state"`
	Required      bool   `json:"required"`
	Ready         bool   `json:"ready"`
	Generation    uint64 `json:"generation"`
	ClientRetries uint64 `json:"clientRetries"`
	ActorRestarts uint64 `json:"actorRestarts"`
	LastError     string `json:"lastError,omitempty"`
}

// WSServerRuntimeSnapshot summarizes server-side WebSocket sessions without exposing headers or keys.
type WSServerRuntimeSnapshot struct {
	Registered      bool   `json:"registered"`
	ActiveSessions  int    `json:"activeSessions"`
	EnqueuedTotal   uint64 `json:"enqueuedTotal"`
	DroppedTotal    uint64 `json:"droppedTotal"`
	WriteErrorTotal uint64 `json:"writeErrorTotal"`
}

func (s lifecycleState) String() string {
	switch s {
	case lifecycleNew:
		return "new"
	case lifecycleStarting:
		return "starting"
	case lifecycleRunning:
		return "running"
	case lifecycleStopping:
		return "stopping"
	case lifecycleStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

var (
	embeddedURLPattern = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*://[^\s"'<>]+`)
	mysqlDSNPattern    = regexp.MustCompile(`([^\s:/'"]+):([^\s@'"]+)@tcp\(`)
	kvSecretPattern    = regexp.MustCompile(`(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|auth[_-]?key|private[_-]?key)\s*(=|:)\s*([^\s&;,)'"]+)`)
)

func errString(err error) string {
	if err == nil {
		return ""
	}
	return redactErrorString(err.Error())
}

func redactErrorString(raw string) string {
	if raw == "" {
		return ""
	}
	s := embeddedURLPattern.ReplaceAllStringFunc(raw, func(match string) string {
		trimmed := strings.TrimRight(match, ".,;:)]}")
		suffix := match[len(trimmed):]
		return redactEndpointURL(trimmed) + suffix
	})
	s = mysqlDSNPattern.ReplaceAllString(s, "[redacted]:[redacted]@tcp(")
	s = kvSecretPattern.ReplaceAllString(s, "${1}${2}[redacted]")
	return s
}

func redactEndpointURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "[redacted]"
	}
	if u.User != nil {
		u.User = url.UserPassword("[redacted]", "[redacted]")
	}
	if u.RawQuery != "" {
		normalizedQuery := strings.ReplaceAll(u.RawQuery, ";", "&")
		q, qErr := url.ParseQuery(normalizedQuery)
		if qErr != nil {
			u.RawQuery = "[redacted]"
		} else {
			for k := range q {
				upper := strings.ToUpper(k)
				for _, sensitive := range []string{"SECRET", "PASSWORD", "PASS", "PRIVATE", "TOKEN", "KEY", "DSN", "CREDENTIAL", "AUTH", "SIG", "HEADER"} {
					if strings.Contains(upper, sensitive) {
						q.Set(k, "[redacted]")
						break
					}
				}
			}
			u.RawQuery = q.Encode()
		}
	}
	return strings.ReplaceAll(u.String(), "%5Bredacted%5D", "[redacted]")
}

var knownLegacyConfigKeys = []string{
	"APP_NAME", "APP_VERSION", "APP_ENV", "LOG_LEVEL", "HTTP_ADDR", "HTTP_HOST", "HTTP_PORT", "HTTP_ENABLED",
	"GRPC_ADDR", "GRPC_HOST", "GRPC_PORT", "GRPC_ENABLED", "METRICS_ADDR", "METRICS_HOST", "METRICS_PORT",
	"METRICS_ENABLED", "REQUEST_TIMEOUT", "SHUTDOWN_GRACE_PERIOD", "DB_DIALECT", "DB_HOST", "DB_PORT",
	"DB_NAME", "DB_USER", "DB_PASSWORD", "REDIS_HOST", "REDIS_PORT", "REDIS_PASSWORD", "PUBSUB_BACKEND",
	"PUBSUB_BROKER", "TRACE_EXPORTER", "TRACER_URL", "TRACER_AUTH_KEY", "TRACER_HEADERS", "JWT_SECRET", "REMOTE_LOG_URL",
}

func (a *App) redactedConfigSnapshot() map[string]string {
	if a == nil || a.Config == nil {
		return map[string]string{}
	}
	if snap, ok := a.Config.(*config.Snapshot); ok {
		return snap.Redacted()
	}
	collected := make(map[string]string)
	for _, k := range knownLegacyConfigKeys {
		if v := a.Config.Get(k); v != "" {
			collected[k] = v
		}
	}
	return config.RedactMap(collected)
}

// RuntimeSnapshot returns an in-process, secret-redacted snapshot of runtime state,
// actor supervision tree counters, and supervised components.
func (a *App) RuntimeSnapshot() RuntimeSnapshot {
	if a == nil {
		return RuntimeSnapshot{Config: map[string]string{}}
	}

	a.runtimeMu.Lock()
	state := a.runtimeState
	rt := a.actorRT
	workers := append([]*workerSlot(nil), a.backgroundWorkers...)
	wsClients := append([]*wsClientSlot(nil), a.wsClients...)
	subs := append([]*subscriberSlot(nil), a.subscriberSlotsList...)
	wsRegistered := a.wsServerRegistered
	a.runtimeMu.Unlock()

	out := RuntimeSnapshot{
		State:        state.String(),
		Startup:      a.isStartupComplete(),
		Alive:        a.isAlive(),
		Ready:        a.isReady(),
		IntakeClosed: a.isIntakeClosed(),
		FatalError:   errString(a.fatalRuntimeCause()),
		Config:       a.redactedConfigSnapshot(),
	}

	if rt != nil && rt.node != nil {
		if nodeSnap, err := rt.node.Snapshot(actor.SnapshotOptions{}); err == nil {
			out.ActorTree = &nodeSnap
		}
	}

	if len(workers) > 0 {
		out.Workers = make([]WorkerRuntimeSnapshot, 0, len(workers))
		for _, w := range workers {
			ws := w.snapshot()
			out.Workers = append(out.Workers, WorkerRuntimeSnapshot{
				Name:          ws.Name,
				ActorName:     ws.ActorName,
				State:         ws.State.String(),
				Mode:          w.policy.Mode,
				Required:      w.policy.Required,
				WaitReady:     w.policy.WaitReady,
				Ready:         ws.Ready,
				Generation:    ws.Generation,
				WorkerRetries: ws.WorkerRetries,
				ActorRestarts: ws.ActorRestarts,
				LastError:     errString(ws.LastError),
			})
		}
	}

	cronSlots := a.cronJobSlots()
	if len(cronSlots) > 0 {
		out.CronJobs = make([]CronJobRuntimeSnapshot, 0, len(cronSlots))
		for _, cj := range cronSlots {
			cs := cj.snapshot()
			out.CronJobs = append(out.CronJobs, CronJobRuntimeSnapshot{
				Name:              cs.Name,
				ActorName:         cs.ActorName,
				Schedule:          cs.Schedule,
				Overlap:           cs.Overlap,
				MaxConcurrency:    cs.MaxConcurrency,
				ActiveRuns:        cs.ActiveRuns,
				QueuedNext:        cs.QueuedNext,
				RunsTotal:         cs.RunsTotal,
				SkippedTotal:      cs.SkippedTotal,
				DroppedQueueTotal: cs.DroppedQueueTotal,
				FailuresTotal:     cs.FailuresTotal,
				PanicsTotal:       cs.PanicsTotal,
				ActorRestarts:     cs.ActorRestarts,
				LastDuration:      cs.LastDuration,
				LastError:         errString(cs.LastError),
			})
		}
	}

	if len(subs) > 0 {
		out.Subscribers = make([]SubscriberRuntimeSnapshot, 0, len(subs))
		for _, sub := range subs {
			ss := sub.snapshot()
			out.Subscribers = append(out.Subscribers, SubscriberRuntimeSnapshot{
				Topic:               ss.Topic,
				CoordActorName:      ss.CoordActorName,
				State:               ss.State.String(),
				AckMode:             ss.AckMode.String(),
				Concurrency:         ss.Concurrency,
				QueueSize:           ss.QueueSize,
				Required:            ss.Required,
				Shutdown:            ss.Shutdown,
				Ready:               ss.Ready,
				InFlightTickets:     ss.InFlightTickets,
				FrozenPartitions:    ss.FrozenPartitions,
				ProcessedTotal:      ss.ProcessedTotal,
				CommittedTotal:      ss.CommittedTotal,
				FailuresTotal:       ss.FailuresTotal,
				PanicsTotal:         ss.PanicsTotal,
				HandlerRetriesTotal: ss.HandlerRetriesTotal,
				SendRejectedTotal:   ss.SendRejectedTotal,
				CommitFailuresTotal: ss.CommitFailuresTotal,
				CommitUnknownTotal:  ss.CommitUnknownTotal,
				DLQSentTotal:        ss.DLQSentTotal,
				DLQSkippedTotal:     ss.DLQSkippedTotal,
				DLQFailuresTotal:    ss.DLQFailuresTotal,
				AbandonedTotal:      ss.AbandonedTotal,
				CoordRestarts:       ss.CoordRestarts,
				WorkerRestarts:      ss.WorkerRestarts,
				LastRecoveryAction:  ss.LastRecoveryAction,
				LastError:           errString(ss.LastError),
			})
		}
	}

	if len(wsClients) > 0 {
		out.WSClients = make([]WSClientRuntimeSnapshot, 0, len(wsClients))
		for _, wsc := range wsClients {
			wsc.mu.Lock()
			wsState := wsc.state
			wsReady := wsc.ready
			wsGen := wsc.generation
			wsRetries := wsc.clientRetries
			wsActorRestarts := wsc.actorRestarts
			wsLastErr := wsc.lastErr
			wsc.mu.Unlock()

			out.WSClients = append(out.WSClients, WSClientRuntimeSnapshot{
				ServiceName:   wsc.serviceName,
				ActorName:     wsc.actorName,
				URL:           redactEndpointURL(wsc.url),
				State:         wsState.String(),
				Required:      wsc.policy.Required,
				Ready:         wsReady,
				Generation:    wsGen,
				ClientRetries: wsRetries,
				ActorRestarts: wsActorRestarts,
				LastError:     errString(wsLastErr),
			})
		}
	}

	wsStats := WSServerRuntimeSnapshot{
		Registered:      wsRegistered,
		EnqueuedTotal:   a.wsServerEnqueuedTotal.Load(),
		DroppedTotal:    a.wsServerDroppedTotal.Load(),
		WriteErrorTotal: a.wsServerWriteErrTotal.Load(),
	}
	a.wsServerSessionsMu.RLock()
	wsStats.ActiveSessions = len(a.wsServerSessions)
	a.wsServerSessionsMu.RUnlock()
	out.WSServer = wsStats

	return out
}
