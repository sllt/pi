package pi

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sllt/tao/actor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	piKafka "github.com/sllt/pi/pkg/pi/datasource/pubsub/kafka"
	"github.com/sllt/pi/pkg/pi/logging"
)

type stage3BrokerMock struct {
	semantics pubsub.AckSemantics
	subFn     func(ctx context.Context, topic string) (*pubsub.Message, error)
	pubFn     func(ctx context.Context, topic string, payload []byte) error
	closed    atomic.Int32
	closeHook func()
}

func (b *stage3BrokerMock) AckSemantics() pubsub.AckSemantics {
	return b.semantics
}

func (b *stage3BrokerMock) Publish(ctx context.Context, topic string, message []byte) error {
	if b.pubFn != nil {
		return b.pubFn(ctx, topic, message)
	}
	return nil
}

func (b *stage3BrokerMock) Subscribe(ctx context.Context, topic string) (*pubsub.Message, error) {
	if b.subFn != nil {
		return b.subFn(ctx, topic)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *stage3BrokerMock) DeleteTopic(context.Context, string) error { return nil }
func (b *stage3BrokerMock) CreateTopic(context.Context, string) error { return nil }
func (b *stage3BrokerMock) Query(context.Context, string, ...any) ([]byte, error) {
	return nil, nil
}
func (b *stage3BrokerMock) Health() datasource.Health { return datasource.Health{Status: "UP"} }
func (b *stage3BrokerMock) Close() error {
	b.closed.Add(1)
	if b.closeHook != nil {
		b.closeHook()
	}
	return nil
}

type stage3Committer struct {
	meta       pubsub.MessageMeta
	commits    atomic.Int32
	nacks      atomic.Int32
	commitFn   func(ctx context.Context) error
	legacyOnly bool
}

func (c *stage3Committer) Commit() {
	c.commits.Add(1)
}

func (c *stage3Committer) CommitContext(ctx context.Context) error {
	if c.commitFn != nil {
		if err := c.commitFn(ctx); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.commits.Add(1)
	return nil
}

func (c *stage3Committer) Nack() {
	c.nacks.Add(1)
}

func (c *stage3Committer) MessageMeta() pubsub.MessageMeta {
	return c.meta
}

type legacyOnlyCommitter struct {
	commits atomic.Int32
}

func (c *legacyOnlyCommitter) Commit() {
	c.commits.Add(1)
}

func buildStage3TestApp(t *testing.T, client pubsub.Client, ownership Ownership) *App {
	t.Helper()
	opts := []Option{
		WithConfig(map[string]string{
			"HTTP_ENABLED":          "false",
			"GRPC_ENABLED":          "false",
			"METRICS_ENABLED":       "false",
			"LOG_LEVEL":             "ERROR",
			"SHUTDOWN_GRACE_PERIOD": "3s",
		}),
		WithLogger(logging.NewMockLogger(logging.ERROR)),
	}
	if client != nil {
		opts = append(opts, WithPubSub(client, ownership))
	}
	app, err := Build(opts...)
	require.NoError(t, err)
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = app.Stop(stopCtx)
	})
	return app
}

func TestSubscriberPolicy_ValidationAndBackendCapabilities(t *testing.T) {
	t.Run("rejects uninitialized pubsub and invalid policy fields", func(t *testing.T) {
		noPubApp := buildStage3TestApp(t, nil, Owned)
		require.ErrorIs(t, noPubApp.AddSubscriber("orders", SubscribePolicy{}, func(*Context) error { return nil }), errSubscriberNotInitialized)

		indep := &stage3BrokerMock{
			semantics: pubsub.AckSemantics{Mode: pubsub.AckModeIndependent, SupportsContextCommit: true, SupportsNack: true},
		}
		app := buildStage3TestApp(t, indep, Owned)

		require.ErrorIs(t, app.AddSubscriber("   ", SubscribePolicy{}, func(*Context) error { return nil }), errSubscriberTopicEmpty)
		require.ErrorIs(t, app.AddSubscriber("orders", SubscribePolicy{}, nil), errSubscriberHandlerNil)
		require.ErrorIs(t, app.AddSubscriber("orders", SubscribePolicy{Shutdown: SubscriberShutdownMode(99)}, func(*Context) error { return nil }), errSubscriberInvalidShutdownMode)
		require.ErrorIs(t, app.AddSubscriber("orders", SubscribePolicy{Concurrency: -1}, func(*Context) error { return nil }), errSubscriberInvalidConcurrency)
		require.ErrorIs(t, app.AddSubscriber("orders", SubscribePolicy{QueueSize: -1}, func(*Context) error { return nil }), errSubscriberInvalidQueueSize)
		require.ErrorIs(t, app.AddSubscriber("orders", SubscribePolicy{MaxHandlerRetries: -1}, func(*Context) error { return nil }), errSubscriberInvalidRetryPolicy)
		require.ErrorIs(t, app.AddSubscriber("orders", SubscribePolicy{RetryBackoff: -time.Millisecond}, func(*Context) error { return nil }), errSubscriberInvalidRetryPolicy)
		require.ErrorIs(t, app.AddSubscriber("orders", SubscribePolicy{AllowDeadLetterSkip: true}, func(*Context) error { return nil }), errSubscriberDeadLetterHandlerRequired)

		require.NoError(t, app.AddSubscriber("orders", SubscribePolicy{}, func(*Context) error { return nil }))
		require.ErrorIs(t, app.AddSubscriber("orders", SubscribePolicy{}, func(*Context) error { return nil }), errSubscriberDuplicateTopic)

		app.Subscribe("payments", func(*Context) error { return nil })
		require.ErrorIs(t, app.AddSubscriber("payments", SubscribePolicy{}, func(*Context) error { return nil }), errSubscriberDuplicateTopic)

		require.NoError(t, app.Start(t.Context()))
		require.ErrorIs(t, app.AddSubscriber("late-topic", SubscribePolicy{}, func(*Context) error { return nil }), errSubscriberRegisterTooLate)
	})

	t.Run("enforces AckMode capability boundaries", func(t *testing.T) {
		unknownBackend := &stage3BrokerMock{} // AckModeUnknown by default
		appUnknown := buildStage3TestApp(t, unknownBackend, Borrowed)
		require.ErrorIs(t, appUnknown.AddSubscriber("orders", SubscribePolicy{Shutdown: SubscriberDrain}, func(*Context) error { return nil }), errSubscriberUnsupportedCapability)
		require.ErrorIs(t, appUnknown.AddSubscriber("orders", SubscribePolicy{
			AllowDeadLetterSkip: true,
			DeadLetterHandler:   func(*Context, *pubsub.Message, error) error { return nil },
		}, func(*Context) error { return nil }), errSubscriberUnsupportedCapability)
		// AckModeUnknown also rejects Concurrency > 1 and KeyExtractor until adapter capability evidence exists.
		require.ErrorIs(t, appUnknown.AddSubscriber("orders-concurrent", SubscribePolicy{Concurrency: 2}, func(*Context) error { return nil }), errSubscriberUnsupportedCapability)
		require.ErrorIs(t, appUnknown.AddSubscriber("orders-sharded", SubscribePolicy{
			KeyExtractor: func(m *pubsub.Message) string { return string(m.Value) },
		}, func(*Context) error { return nil }), errSubscriberUnsupportedCapability)
		// Default SubscriberCancel (Concurrency 1) remains allowed on AckModeUnknown for custom mocks/legacy adapters.
		require.NoError(t, appUnknown.AddSubscriber("orders", SubscribePolicy{Shutdown: SubscriberCancel}, func(*Context) error { return nil }))

		autoBackend := &stage3BrokerMock{semantics: pubsub.AckSemantics{Mode: pubsub.AckModeAuto}}
		appAuto := buildStage3TestApp(t, autoBackend, Borrowed)
		require.ErrorIs(t, appAuto.AddSubscriber("telemetry", SubscribePolicy{Shutdown: SubscriberDrain}, func(*Context) error { return nil }), errSubscriberUnsupportedCapability)
		require.ErrorIs(t, appAuto.AddSubscriber("telemetry", SubscribePolicy{
			AllowDeadLetterSkip: true,
			DeadLetterHandler:   func(*Context, *pubsub.Message, error) error { return nil },
		}, func(*Context) error { return nil }), errSubscriberUnsupportedCapability)

		noCtxCommitBackend := &stage3BrokerMock{semantics: pubsub.AckSemantics{Mode: pubsub.AckModeIndependent, SupportsContextCommit: false}}
		appNoCtx := buildStage3TestApp(t, noCtxCommitBackend, Borrowed)
		require.ErrorIs(t, appNoCtx.AddSubscriber("jobs", SubscribePolicy{Shutdown: SubscriberDrain}, func(*Context) error { return nil }), errSubscriberUnsupportedCapability)

		kafkaBackend := &stage3BrokerMock{semantics: pubsub.AckSemantics{Mode: pubsub.AckModeCumulativeOffset, SupportsContextCommit: true}}
		appKafka := buildStage3TestApp(t, kafkaBackend, Borrowed)
		require.ErrorIs(t, appKafka.AddSubscriber("events", SubscribePolicy{
			Concurrency:  4,
			KeyExtractor: func(m *pubsub.Message) string { return string(m.Value) },
		}, func(*Context) error { return nil }), errSubscriberKeyExtractorForbiddenOffsets)
		require.NoError(t, appKafka.AddSubscriber("events", SubscribePolicy{
			Concurrency: 4,
			Shutdown:    SubscriberDrain,
		}, func(*Context) error { return nil }))
	})

	t.Run("WithPubSub honors Owned vs Borrowed lifecycle", func(t *testing.T) {
		ownedBroker := &stage3BrokerMock{semantics: pubsub.AckSemantics{Mode: pubsub.AckModeAuto}}
		appOwned := buildStage3TestApp(t, ownedBroker, Owned)
		require.NoError(t, appOwned.Start(t.Context()))
		require.NoError(t, appOwned.Stop(t.Context()))
		assert.EqualValues(t, 1, ownedBroker.closed.Load())

		borrowedBroker := &stage3BrokerMock{semantics: pubsub.AckSemantics{Mode: pubsub.AckModeAuto}}
		appBorrowed := buildStage3TestApp(t, borrowedBroker, Borrowed)
		require.NoError(t, appBorrowed.Start(t.Context()))
		require.NoError(t, appBorrowed.Stop(t.Context()))
		assert.Zero(t, borrowedBroker.closed.Load())
	})
}

func TestSubscriber_LegacySubscribeSupervisedZeroRegression(t *testing.T) {
	var step atomic.Int32
	goodCommit := &legacyOnlyCommitter{}
	panicCommit := &legacyOnlyCommitter{}
	cancelCommit := &legacyOnlyCommitter{}
	inCancelHandler := make(chan struct{})
	handlerCanceled := make(chan struct{})

	broker := &stage3BrokerMock{
		subFn: func(ctx context.Context, topic string) (*pubsub.Message, error) {
			switch step.Add(1) {
			case 1:
				m := pubsub.NewMessage(ctx)
				m.Topic = topic
				m.Value = []byte("ok")
				m.Committer = goodCommit
				return m, nil
			case 2:
				m := pubsub.NewMessage(ctx)
				m.Topic = topic
				m.Value = []byte("panic")
				m.Committer = panicCommit
				return m, nil
			case 3:
				m := pubsub.NewMessage(ctx)
				m.Topic = topic
				m.Value = []byte("block")
				m.Committer = cancelCommit
				return m, nil
			default:
				<-ctx.Done()
				return nil, ctx.Err()
			}
		},
	}

	app := buildStage3TestApp(t, broker, Owned)
	app.Subscribe("legacy-orders", func(c *Context) error {
		var raw []byte
		_ = c.Bind(&raw)
		switch string(c.Request.(*pubsub.Message).Value) {
		case "ok":
			return nil
		case "panic":
			panic("boom")
		case "block":
			close(inCancelHandler)
			<-c.Done()
			close(handlerCanceled)
			return nil // Swallowing cancellation still must NOT commit!
		}
		return nil
	})

	require.NoError(t, app.Start(t.Context()))
	require.True(t, app.isSubscribersSupervised())

	select {
	case <-inCancelHandler:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for 3rd message handler")
	}

	require.NoError(t, app.Stop(t.Context()))

	select {
	case <-handlerCanceled:
	case <-time.After(time.Second):
		t.Fatal("expected legacy Subscribe handler to be canceled on Stop")
	}

	assert.EqualValues(t, 1, goodCommit.commits.Load())
	assert.Zero(t, panicCommit.commits.Load())
	assert.Zero(t, cancelCommit.commits.Load())
	assert.EqualValues(t, 1, broker.closed.Load())
}

func TestSubscriberDrain_ContextIsolationAndDrainCommit(t *testing.T) {
	type traceKey struct{}

	var (
		pulls                    atomic.Int32
		handlerEntered           = make(chan struct{})
		pullBlocked              = make(chan struct{})
		pullStopErrCh            = make(chan error, 1)
		brokerClosedBeforeCommit atomic.Bool
	)

	committer := &stage3Committer{
		meta: pubsub.MessageMeta{Partition: 0, Offset: 1, HasPartition: true, HasOffset: true},
	}

	var broker *stage3BrokerMock
	committer.commitFn = func(ctx context.Context) error {
		if broker.closed.Load() > 0 {
			brokerClosedBeforeCommit.Store(true)
		}
		return ctx.Err()
	}

	broker = &stage3BrokerMock{
		semantics: pubsub.AckSemantics{
			Mode:                  pubsub.AckModeIndependent,
			SupportsContextCommit: true,
			SupportsNack:          true,
		},
		subFn: func(pullCtx context.Context, topic string) (*pubsub.Message, error) {
			if pulls.Add(1) == 1 {
				// Simulate adapters like kafka.Subscribe that embed pullCtx into NewMessage(pullCtx)!
				m := pubsub.NewMessage(pullCtx)
				m.Topic = topic
				m.Value = []byte("drain-me")
				m.Committer = committer
				return m, nil
			}
			close(pullBlocked)
			<-pullCtx.Done()
			pullStopErrCh <- pullCtx.Err()
			return nil, pullCtx.Err()
		},
	}

	app := buildStage3TestApp(t, broker, Owned)
	var observedTrace any
	var handlerCtxErrDuringDrain error

	require.NoError(t, app.AddSubscriber("drain-orders", SubscribePolicy{
		Concurrency: 1,
		QueueSize:   8,
		Shutdown:    SubscriberDrain,
		Required:    true,
	}, func(c *Context) error {
		observedTrace = c.Value(traceKey{})
		close(handlerEntered)
		time.Sleep(150 * time.Millisecond)
		handlerCtxErrDuringDrain = c.Err()
		return nil
	}))

	startCtx := context.WithValue(t.Context(), traceKey{}, "boot-trace-123")
	require.NoError(t, app.Start(startCtx))
	require.True(t, app.isReady())

	select {
	case <-handlerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	select {
	case <-pullBlocked:
	case <-time.After(2 * time.Second):
		t.Fatal("second Subscribe pull did not block")
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelStop()
	require.NoError(t, app.Stop(stopCtx))

	// Pull loop must have been canceled in Phase A while admitted handler stayed live in Phase B!
	select {
	case pullErr := <-pullStopErrCh:
		require.ErrorIs(t, pullErr, context.Canceled)
	default:
		t.Fatal("expected blocked Subscribe pull to be canceled during Phase A")
	}

	assert.Equal(t, "boot-trace-123", observedTrace)
	require.NoError(t, handlerCtxErrDuringDrain, "admitted SubscriberDrain message context must NOT be canceled by Phase A pull cancellation")
	assert.EqualValues(t, 1, committer.commits.Load(), "admitted message must commit during Phase B drain")
	assert.Zero(t, committer.nacks.Load())
	assert.False(t, brokerClosedBeforeCommit.Load(), "Owned PubSub must close strictly after Phase B drain commit")
	assert.EqualValues(t, 1, broker.closed.Load())
}

func TestSubscriberDrain_MessageLeaseAndDrainTimeoutPreventCommit(t *testing.T) {
	t.Run("genuine message lease expiration cancels handler and blocks commit", func(t *testing.T) {
		leaseCtx, cancelLease := context.WithCancel(context.Background())
		defer cancelLease()

		committer := &stage3Committer{}
		var delivered atomic.Bool
		entered := make(chan struct{})
		done := make(chan struct{})

		broker := &stage3BrokerMock{
			semantics: pubsub.AckSemantics{Mode: pubsub.AckModeIndependent, SupportsContextCommit: true, SupportsNack: true},
			subFn: func(pullCtx context.Context, topic string) (*pubsub.Message, error) {
				if delivered.CompareAndSwap(false, true) {
					m := pubsub.NewMessage(leaseCtx)
					m.Topic = topic
					m.Value = []byte("lease-msg")
					m.Committer = committer
					return m, nil
				}
				<-pullCtx.Done()
				return nil, pullCtx.Err()
			},
		}

		app := buildStage3TestApp(t, broker, Borrowed)
		require.NoError(t, app.AddSubscriber("lease-topic", SubscribePolicy{
			Shutdown: SubscriberDrain,
		}, func(c *Context) error {
			close(entered)
			<-c.Done()
			close(done)
			return nil // Swallow cancellation; runtime still must NOT commit!
		}))

		require.NoError(t, app.Start(t.Context()))
		<-entered
		cancelLease()
		<-done

		require.Eventually(t, func() bool {
			return committer.nacks.Load() == 1
		}, time.Second, 5*time.Millisecond)
		assert.Zero(t, committer.commits.Load())
	})

	t.Run("Phase B drain timeout cancels handler in Phase C, blocks commit, and delays resource close until exit", func(t *testing.T) {
		committer := &stage3Committer{}
		var delivered atomic.Bool
		entered := make(chan struct{})
		handlerRelease := make(chan struct{})
		var closedWhileHandlerRunning atomic.Bool

		var broker *stage3BrokerMock
		broker = &stage3BrokerMock{
			semantics: pubsub.AckSemantics{Mode: pubsub.AckModeIndependent, SupportsContextCommit: true, SupportsNack: true},
			subFn: func(pullCtx context.Context, topic string) (*pubsub.Message, error) {
				if delivered.CompareAndSwap(false, true) {
					m := pubsub.NewMessage(pullCtx)
					m.Topic = topic
					m.Value = []byte("slow-drain")
					m.Committer = committer
					return m, nil
				}
				<-pullCtx.Done()
				return nil, pullCtx.Err()
			},
		}

		app := buildStage3TestApp(t, broker, Owned)
		var handlerRunning atomic.Bool
		broker.closeHook = func() {
			if handlerRunning.Load() {
				closedWhileHandlerRunning.Store(true)
			}
		}

		require.NoError(t, app.AddSubscriber("slow-drain-topic", SubscribePolicy{
			Shutdown: SubscriberDrain,
		}, func(c *Context) error {
			handlerRunning.Store(true)
			defer handlerRunning.Store(false)
			close(entered)
			<-c.Done() // Canceled when Phase C begins after Phase B times out
			<-handlerRelease
			return nil
		}))

		require.NoError(t, app.Start(t.Context()))
		<-entered

		shortStopCtx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		defer cancel()
		stopErr := app.Stop(shortStopCtx)
		require.ErrorIs(t, stopErr, context.DeadlineExceeded)
		// Owned broker must NOT be closed yet because the handler goroutine is still holding handlerRelease!
		assert.Zero(t, broker.closed.Load())

		close(handlerRelease)
		waitCtx, cancelWait := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancelWait()
		waitErr := app.Wait(waitCtx)
		require.ErrorIs(t, waitErr, context.DeadlineExceeded)

		assert.False(t, closedWhileHandlerRunning.Load())
		assert.EqualValues(t, 1, broker.closed.Load())
		assert.Zero(t, committer.commits.Load())
	})
}

func TestSubscriber_TicketLedgerSlotAccountingAndFaults(t *testing.T) {
	t.Run("bounded per-worker backpressure and zero slot leaks", func(t *testing.T) {
		const totalMsgs = 12
		var pulled atomic.Int32
		committers := make([]*stage3Committer, totalMsgs)
		for i := range committers {
			committers[i] = &stage3Committer{
				meta: pubsub.MessageMeta{Key: []byte("hot-key")}, // All hash to same worker shard
			}
		}

		gate := make(chan struct{})
		firstTwoRunning := make(chan struct{})
		var startedCount atomic.Int32

		broker := &stage3BrokerMock{
			semantics: pubsub.AckSemantics{Mode: pubsub.AckModeIndependent, SupportsContextCommit: true, SupportsNack: true},
			subFn: func(ctx context.Context, topic string) (*pubsub.Message, error) {
				idx := int(pulled.Add(1)) - 1
				if idx >= totalMsgs {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				m := pubsub.NewMessage(ctx)
				m.Topic = topic
				m.Value = []byte(fmt.Sprintf("msg-%d", idx))
				m.Committer = committers[idx]
				return m, nil
			},
		}

		app := buildStage3TestApp(t, broker, Borrowed)
		require.NoError(t, app.AddSubscriber("hot-key-topic", SubscribePolicy{
			Concurrency: 2,
			QueueSize:   2,
			Shutdown:    SubscriberDrain,
		}, func(c *Context) error {
			if startedCount.Add(1) == 1 {
				close(firstTwoRunning)
			}
			<-gate
			return nil
		}))

		require.NoError(t, app.Start(t.Context()))
		<-firstTwoRunning

		// QueueSize=2 on the hot worker shard: 1 running + 1 queued in mailbox + 1 held by pump waiting for sem = at most 3 pulled!
		time.Sleep(40 * time.Millisecond)
		assert.LessOrEqual(t, pulled.Load(), int32(3), "bounded per-worker semaphore must backpressure pull pump")

		close(gate)
		require.Eventually(t, func() bool {
			for _, cm := range committers {
				if cm.commits.Load() != 1 {
					return false
				}
			}
			return true
		}, 3*time.Second, 10*time.Millisecond)

		slots := app.subscriberSlots()
		require.Len(t, slots, 1)
		snap := slots[0].snapshot()
		assert.Zero(t, snap.InFlightTickets)
		for i, sem := range slots[0].sem {
			assert.Zero(t, len(sem), "worker %d semaphore must have zero leaked slots", i)
		}
	})

	t.Run("forced SubscriberCancel stop sweeps queued unstarted tickets and releases slots once", func(t *testing.T) {
		const queuedCount = 4
		committers := make([]*stage3Committer, queuedCount)
		for i := range committers {
			committers[i] = &stage3Committer{}
		}
		var idxSeq atomic.Int32
		allPulled := make(chan struct{})
		firstEntered := make(chan struct{})

		broker := &stage3BrokerMock{
			semantics: pubsub.AckSemantics{Mode: pubsub.AckModeIndependent, SupportsContextCommit: true, SupportsNack: true},
			subFn: func(ctx context.Context, topic string) (*pubsub.Message, error) {
				idx := int(idxSeq.Add(1)) - 1
				if idx >= queuedCount {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				if idx == queuedCount-1 {
					defer close(allPulled)
				}
				m := pubsub.NewMessage(ctx)
				m.Topic = topic
				m.Value = []byte(fmt.Sprintf("q-%d", idx))
				m.Committer = committers[idx]
				return m, nil
			},
		}

		app := buildStage3TestApp(t, broker, Borrowed)
		var firstOnce sync.Once
		require.NoError(t, app.AddSubscriber("cancel-sweep-topic", SubscribePolicy{
			Concurrency: 1,
			QueueSize:   queuedCount,
			Shutdown:    SubscriberCancel,
		}, func(c *Context) error {
			firstOnce.Do(func() { close(firstEntered) })
			<-c.Done()
			return c.Err()
		}))

		require.NoError(t, app.Start(t.Context()))
		<-firstEntered
		<-allPulled
		// Give pump a brief moment to enqueue messages 1..3 into mailbox
		time.Sleep(15 * time.Millisecond)

		slot := app.subscriberSlots()[0]
		require.NoError(t, app.Stop(t.Context()))

		assert.Zero(t, len(slot.sem[0]), "all semaphore slots must be reclaimed after forced stop")
		assert.Zero(t, slot.snapshot().InFlightTickets)
		for i, cm := range committers {
			assert.Zero(t, cm.commits.Load(), "msg %d must not commit on SubscriberCancel", i)
			assert.EqualValues(t, 1, cm.nacks.Load(), "msg %d must be Nacked exactly once", i)
		}
	})

	t.Run("Send rejection retains message in coordinator, releases slot once, and retries without broker re-pull", func(t *testing.T) {
		cm0 := &stage3Committer{}
		cm1 := &stage3Committer{}
		var pullCount atomic.Int32
		allowSecondPull := make(chan struct{})
		firstEntered := make(chan struct{})
		unblockFirst := make(chan struct{})

		broker := &stage3BrokerMock{
			semantics: pubsub.AckSemantics{Mode: pubsub.AckModeIndependent, SupportsContextCommit: true, SupportsNack: true},
			subFn: func(ctx context.Context, topic string) (*pubsub.Message, error) {
				switch pullCount.Add(1) {
				case 1:
					m := pubsub.NewMessage(ctx)
					m.Topic = topic
					m.Value = []byte("msg-0")
					m.Committer = cm0
					return m, nil
				case 2:
					<-allowSecondPull
					m := pubsub.NewMessage(ctx)
					m.Topic = topic
					m.Value = []byte("msg-1")
					m.Committer = cm1
					return m, nil
				default:
					<-ctx.Done()
					return nil, ctx.Err()
				}
			},
		}

		app := buildStage3TestApp(t, broker, Borrowed)
		require.NoError(t, app.AddSubscriber("send-rejection-topic", SubscribePolicy{
			Concurrency: 1,
			QueueSize:   2,
			Shutdown:    SubscriberDrain,
		}, func(c *Context) error {
			if string(c.Request.(*pubsub.Message).Value) == "msg-0" {
				close(firstEntered)
				<-unblockFirst
			}
			return nil
		}))

		require.NoError(t, app.Start(t.Context()))
		<-firstEntered

		slot := app.subscriberSlots()[0]
		app.runtimeMu.Lock()
		node := app.actorRT.node
		app.runtimeMu.Unlock()

		// Fill worker 0's actor mailbox with harmless check messages while msg-0 holds Receive()
		workerRef := actor.Ref{Path: "pi-root/subs-sup/" + slot.workerNames[0]}
		for i := 0; i < 64; i++ {
			_ = node.Send(workerRef, subWorkerCheckMsg{})
		}

		// Now release msg-1 from Subscribe: sem[0] has 1 free slot (QueueSize=2), so Send hits ErrMailboxFull!
		close(allowSecondPull)
		require.Eventually(t, func() bool {
			return slot.snapshot().SendRejectedTotal >= 1
		}, 2*time.Second, 2*time.Millisecond)

		// Unblock worker 0 so mailbox drains and coordinator's retained retry succeeds!
		close(unblockFirst)
		require.Eventually(t, func() bool {
			return cm0.commits.Load() == 1 && cm1.commits.Load() == 1
		}, 3*time.Second, 5*time.Millisecond)

		assert.EqualValues(t, 1, cm1.commits.Load(), "retained message must commit exactly once")
		assert.Zero(t, cm1.nacks.Load(), "retained message must not be prematurely Nacked")
		assert.Zero(t, len(slot.sem[0]), "semaphore slot must have zero leaks after Send rejection + retry")
		assert.Zero(t, slot.snapshot().InFlightTickets)
	})

	t.Run("worker and coordinator actor crashes recover via PreserveRef and external ledger without loss or duplication", func(t *testing.T) {
		const msgCount = 16
		committers := make([]*stage3Committer, msgCount)
		for i := range committers {
			committers[i] = &stage3Committer{}
		}
		var pullSeq atomic.Int32
		firstRunning := make(chan struct{})
		unblockFirst := make(chan struct{})
		var firstOnce sync.Once

		broker := &stage3BrokerMock{
			semantics: pubsub.AckSemantics{Mode: pubsub.AckModeIndependent, SupportsContextCommit: true, SupportsNack: true},
			subFn: func(ctx context.Context, topic string) (*pubsub.Message, error) {
				n := int(pullSeq.Add(1)) - 1
				if n >= msgCount {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				m := pubsub.NewMessage(ctx)
				m.Topic = topic
				m.Value = []byte(fmt.Sprintf("crash-msg-%d", n))
				m.Committer = committers[n]
				return m, nil
			},
		}

		app := buildStage3TestApp(t, broker, Borrowed)
		var processed sync.Map
		var duplicateExecutions atomic.Int32

		require.NoError(t, app.AddSubscriber("crash-resilient-topic", SubscribePolicy{
			Concurrency: 2,
			QueueSize:   16,
			Shutdown:    SubscriberDrain,
		}, func(c *Context) error {
			val := string(c.Request.(*pubsub.Message).Value)
			firstOnce.Do(func() {
				close(firstRunning)
				<-unblockFirst
			})
			if _, loaded := processed.LoadOrStore(val, true); loaded {
				duplicateExecutions.Add(1)
			}
			return nil
		}))

		require.NoError(t, app.Start(t.Context()))
		<-firstRunning

		slot := app.subscriberSlots()[0]
		// Wait until several tickets are queued across workers while first message is held.
		require.Eventually(t, func() bool {
			return pullSeq.Load() >= 8
		}, 2*time.Second, 5*time.Millisecond)

		// Inject actor crashes into BOTH a partition worker actor AND the topic coordinator actor!
		app.runtimeMu.Lock()
		node := app.actorRT.node
		app.runtimeMu.Unlock()

		require.NoError(t, node.Send(actor.Ref{Path: "pi-root/subs-sup/" + slot.workerNames[0]}, actorInjectPanicMsg{reason: "worker-crash"}))
		require.NoError(t, node.Send(actor.Ref{Path: "pi-root/subs-sup/" + slot.coordActorName}, actorInjectPanicMsg{reason: "coord-crash"}))
		close(unblockFirst)

		require.Eventually(t, func() bool {
			for i := 0; i < msgCount; i++ {
				if committers[i].commits.Load() != 1 {
					return false
				}
			}
			return true
		}, 4*time.Second, 10*time.Millisecond)

		assert.Zero(t, duplicateExecutions.Load(), "zero duplicate message executions across actor crashes")
		snap := slot.snapshot()
		assert.GreaterOrEqual(t, snap.WorkerRestarts, uint64(1))
		assert.GreaterOrEqual(t, snap.CoordRestarts, uint64(1))
		assert.Zero(t, snap.InFlightTickets)
		for i, sem := range slot.sem {
			assert.Zero(t, len(sem), "worker %d semaphore must have zero leaked slots", i)
		}
	})
}

func TestSubscriber_CumulativeOffsetKafkaSemantics(t *testing.T) {
	type offsetRecord struct {
		partition int
		offset    int64
		payload   string
		commitErr error
	}

	newKafkaMock := func(records []offsetRecord) (*stage3BrokerMock, []*stage3Committer) {
		cms := make([]*stage3Committer, len(records))
		for i, rec := range records {
			r := rec
			cms[i] = &stage3Committer{
				meta: pubsub.MessageMeta{
					Partition:    r.partition,
					Offset:       r.offset,
					HasPartition: true,
					HasOffset:    true,
				},
				commitFn: func(ctx context.Context) error {
					if r.commitErr != nil {
						return r.commitErr
					}
					return ctx.Err()
				},
			}
		}
		var idx atomic.Int32
		b := &stage3BrokerMock{
			semantics: pubsub.AckSemantics{
				Mode:                  pubsub.AckModeCumulativeOffset,
				SupportsContextCommit: true,
			},
			subFn: func(ctx context.Context, topic string) (*pubsub.Message, error) {
				i := int(idx.Add(1)) - 1
				if i >= len(records) {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				m := pubsub.NewMessage(ctx)
				m.Topic = topic
				m.Value = []byte(records[i].payload)
				m.Committer = cms[i]
				return m, nil
			},
		}
		return b, cms
	}

	t.Run("offset 10 failure freezes partition so offset 11 never executes or commits (Required triggers App shutdown)", func(t *testing.T) {
		broker, cms := newKafkaMock([]offsetRecord{
			{partition: 0, offset: 10, payload: "fail-10"},
			{partition: 0, offset: 11, payload: "ok-11"},
			{partition: 0, offset: 12, payload: "ok-12"},
		})

		var attempts10, calls11, calls12 atomic.Int32
		app := buildStage3TestApp(t, broker, Borrowed)
		require.NoError(t, app.AddSubscriber("kafka-orders", SubscribePolicy{
			Concurrency:       2,
			MaxHandlerRetries: 1,
			RetryBackoff:      5 * time.Millisecond,
			Shutdown:          SubscriberDrain,
			Required:          true,
		}, func(c *Context) error {
			switch string(c.Request.(*pubsub.Message).Value) {
			case "fail-10":
				attempts10.Add(1)
				return errors.New("poison offset 10")
			case "ok-11":
				calls11.Add(1)
				return nil
			case "ok-12":
				calls12.Add(1)
				return nil
			}
			return nil
		}))

		require.NoError(t, app.Start(t.Context()))

		waitCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		waitErr := app.Wait(waitCtx)
		require.Error(t, waitErr)
		assert.Contains(t, waitErr.Error(), "partition 0 frozen at offset 10")
		assert.False(t, app.isReady())

		assert.EqualValues(t, 2, attempts10.Load(), "offset 10 should run initial attempt + 1 retry")
		assert.Zero(t, calls11.Load(), "offset 11 MUST NOT execute after offset 10 fails")
		assert.Zero(t, calls12.Load(), "offset 12 MUST NOT execute after offset 10 fails")
		assert.Zero(t, cms[0].commits.Load())
		assert.Zero(t, cms[1].commits.Load())
		assert.Zero(t, cms[2].commits.Load())
	})

	t.Run("optional subscriber degrades locally when CommitContext fails on offset 10 without crashing App", func(t *testing.T) {
		commitBoom := errors.New("broker offset commit rejected")
		broker, cms := newKafkaMock([]offsetRecord{
			{partition: 0, offset: 10, payload: "commit-fail-10", commitErr: commitBoom},
			{partition: 0, offset: 11, payload: "ok-11"},
		})

		var calls11 atomic.Int32
		app := buildStage3TestApp(t, broker, Borrowed)
		require.NoError(t, app.AddSubscriber("optional-analytics", SubscribePolicy{
			Concurrency: 1,
			Shutdown:    SubscriberDrain,
			Required:    false,
		}, func(c *Context) error {
			if string(c.Request.(*pubsub.Message).Value) == "ok-11" {
				calls11.Add(1)
			}
			return nil
		}))

		require.NoError(t, app.Start(t.Context()))
		slot := app.subscriberSlots()[0]

		require.Eventually(t, func() bool {
			return slot.snapshot().State == subStateDegraded
		}, 2*time.Second, 10*time.Millisecond)

		assert.True(t, app.isReady(), "optional degraded subscriber must not revoke App readiness")
		snap := slot.snapshot()
		assert.Equal(t, []int{0}, snap.FrozenPartitions)
		assert.EqualValues(t, 1, snap.CommitFailuresTotal)
		assert.Zero(t, calls11.Load(), "offset 11 must NOT execute after offset 10 commit failure")
		assert.Zero(t, cms[0].commits.Load())
		assert.Zero(t, cms[1].commits.Load())
	})

	t.Run("DLQ with AllowDeadLetterSkip=false still freezes offset 10; AllowDeadLetterSkip=true advances to offset 11", func(t *testing.T) {
		// Case 1: AllowDeadLetterSkip = false
		brokerNoSkip, cmsNoSkip := newKafkaMock([]offsetRecord{
			{partition: 0, offset: 10, payload: "poison-10"},
			{partition: 0, offset: 11, payload: "next-11"},
		})
		var dlqCallsNoSkip, nextCallsNoSkip atomic.Int32
		appNoSkip := buildStage3TestApp(t, brokerNoSkip, Borrowed)
		require.NoError(t, appNoSkip.AddSubscriber("dlq-no-skip", SubscribePolicy{
			DeadLetterHandler: func(_ *Context, m *pubsub.Message, _ error) error {
				dlqCallsNoSkip.Add(1)
				return nil
			},
			AllowDeadLetterSkip: false,
			Shutdown:            SubscriberDrain,
			Required:            false,
		}, func(c *Context) error {
			if string(c.Request.(*pubsub.Message).Value) == "poison-10" {
				return errors.New("bad payload")
			}
			nextCallsNoSkip.Add(1)
			return nil
		}))
		require.NoError(t, appNoSkip.Start(t.Context()))
		require.Eventually(t, func() bool {
			return appNoSkip.subscriberSlots()[0].snapshot().State == subStateDegraded
		}, 2*time.Second, 10*time.Millisecond)
		assert.EqualValues(t, 1, dlqCallsNoSkip.Load())
		assert.Zero(t, cmsNoSkip[0].commits.Load())
		assert.Zero(t, nextCallsNoSkip.Load())
		assert.Zero(t, cmsNoSkip[1].commits.Load())

		// Case 2: AllowDeadLetterSkip = true + successful source commit advances cleanly to offset 11!
		brokerSkip, cmsSkip := newKafkaMock([]offsetRecord{
			{partition: 0, offset: 10, payload: "poison-10"},
			{partition: 0, offset: 11, payload: "next-11"},
		})
		var dlqCallsSkip, nextCallsSkip atomic.Int32
		appSkip := buildStage3TestApp(t, brokerSkip, Borrowed)
		require.NoError(t, appSkip.AddSubscriber("dlq-with-skip", SubscribePolicy{
			DeadLetterHandler: func(_ *Context, m *pubsub.Message, _ error) error {
				dlqCallsSkip.Add(1)
				return nil
			},
			AllowDeadLetterSkip: true,
			Shutdown:            SubscriberDrain,
			Required:            true,
		}, func(c *Context) error {
			if string(c.Request.(*pubsub.Message).Value) == "poison-10" {
				return errors.New("bad payload")
			}
			nextCallsSkip.Add(1)
			return nil
		}))
		require.NoError(t, appSkip.Start(t.Context()))
		require.Eventually(t, func() bool {
			return cmsSkip[1].commits.Load() == 1
		}, 2*time.Second, 10*time.Millisecond)
		assert.EqualValues(t, 1, dlqCallsSkip.Load())
		assert.EqualValues(t, 1, cmsSkip[0].commits.Load(), "poison offset 10 committed after DLQ write when AllowDeadLetterSkip=true")
		assert.EqualValues(t, 1, nextCallsSkip.Load(), "offset 11 processed cleanly after DLQ skip")
		assert.True(t, appSkip.isReady())

		// Case 3: AllowDeadLetterSkip = true BUT source CommitContext fails -> MUST freeze partition!
		brokerCommitFail, cmsCommitFail := newKafkaMock([]offsetRecord{
			{partition: 0, offset: 10, payload: "poison-10", commitErr: errors.New("commit failed")},
			{partition: 0, offset: 11, payload: "next-11"},
		})
		var nextCallsCommitFail atomic.Int32
		appCommitFail := buildStage3TestApp(t, brokerCommitFail, Borrowed)
		require.NoError(t, appCommitFail.AddSubscriber("dlq-commit-fail", SubscribePolicy{
			DeadLetterHandler: func(_ *Context, _ *pubsub.Message, _ error) error {
				return nil
			},
			AllowDeadLetterSkip: true,
			Shutdown:            SubscriberDrain,
			Required:            false,
		}, func(c *Context) error {
			if string(c.Request.(*pubsub.Message).Value) == "poison-10" {
				return errors.New("bad payload")
			}
			nextCallsCommitFail.Add(1)
			return nil
		}))
		require.NoError(t, appCommitFail.Start(t.Context()))
		require.Eventually(t, func() bool {
			return appCommitFail.subscriberSlots()[0].snapshot().State == subStateDegraded
		}, 2*time.Second, 10*time.Millisecond)
		assert.Zero(t, nextCallsCommitFail.Load())
		assert.Zero(t, cmsCommitFail[1].commits.Load())
	})
}

type noopKafkaMetrics struct{}

func (noopKafkaMetrics) IncrementCounter(context.Context, string, ...string) {}

func TestSubscriber_RealBrokerKafkaIntegration(t *testing.T) {
	brokerAddr := os.Getenv("PI_TEST_KAFKA_BROKER")
	if brokerAddr == "" {
		t.Skip("PI_TEST_KAFKA_BROKER not set; skipping real Kafka broker integration test")
	}

	logger := logging.NewMockLogger(logging.ERROR)
	topic := fmt.Sprintf("pi-stage3-orders-%d", time.Now().UnixNano())
	groupID := fmt.Sprintf("pi-stage3-cg-%d", time.Now().UnixNano())

	adminClient := piKafka.New(&piKafka.Config{
		Brokers:         []string{brokerAddr},
		ConsumerGroupID: groupID + "-admin",
		BatchSize:       1,
		BatchBytes:      1048576,
		BatchTimeout:    10,
		OffSet:          -2, // FirstOffset
	}, logger, noopKafkaMetrics{})
	require.NotNil(t, adminClient)
	defer func() { _ = adminClient.Close() }()

	require.NoError(t, adminClient.CreateTopic(t.Context(), topic))

	// Publish 3 ordered records on partition 0: msg-0 (succeeds & commits), msg-1 (fails!), msg-2 (must NOT execute or commit on consumer 1!).
	for _, payload := range []string{"msg-0", "msg-1-fail", "msg-2-after-fail"} {
		require.NoError(t, adminClient.Publish(t.Context(), topic, []byte(payload)))
	}

	consumer1Client := piKafka.New(&piKafka.Config{
		Brokers:         []string{brokerAddr},
		ConsumerGroupID: groupID,
		BatchSize:       1,
		BatchBytes:      1048576,
		BatchTimeout:    10,
		OffSet:          -2, // kafka.FirstOffset (-2)
	}, logger, noopKafkaMetrics{})
	require.NotNil(t, consumer1Client)

	app1 := buildStage3TestApp(t, consumer1Client, Owned)
	var c1Seen []string
	var c1Mu sync.Mutex

	require.NoError(t, app1.AddSubscriber(topic, SubscribePolicy{
		Concurrency: 1,
		Shutdown:    SubscriberDrain,
		Required:    true,
	}, func(c *Context) error {
		val := string(c.Request.(*pubsub.Message).Value)
		c1Mu.Lock()
		c1Seen = append(c1Seen, val)
		c1Mu.Unlock()
		if val == "msg-1-fail" {
			return errors.New("simulated business failure at msg-1")
		}
		return nil
	}))

	require.NoError(t, app1.Start(t.Context()))

	// Wait for consumer 1 to hit msg-1-fail, freeze partition 0, and trigger Required fatal shutdown!
	waitCtx1, cancel1 := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel1()
	waitErr1 := app1.Wait(waitCtx1)
	require.Error(t, waitErr1)
	assert.Contains(t, waitErr1.Error(), "frozen at offset 1")

	c1Mu.Lock()
	assert.Equal(t, []string{"msg-0", "msg-1-fail"}, c1Seen, "msg-2-after-fail MUST NOT have been executed or committed by consumer 1")
	c1Mu.Unlock()

	// Now start Consumer 2 in the SAME ConsumerGroupID:
	// Because msg-0 committed offset 0->1 and msg-1-fail did NOT commit (nor did msg-2 leapfrog it!),
	// Consumer 2 MUST start from msg-1-fail (offset 1) and then process msg-2-after-fail (offset 2)!
	consumer2Client := piKafka.New(&piKafka.Config{
		Brokers:         []string{brokerAddr},
		ConsumerGroupID: groupID,
		BatchSize:       1,
		BatchBytes:      1048576,
		BatchTimeout:    10,
		OffSet:          -2,
	}, logger, noopKafkaMetrics{})
	require.NotNil(t, consumer2Client)

	app2 := buildStage3TestApp(t, consumer2Client, Owned)
	var c2Seen []string
	var c2Mu sync.Mutex
	allRecovered := make(chan struct{})

	require.NoError(t, app2.AddSubscriber(topic, SubscribePolicy{
		Concurrency: 1,
		Shutdown:    SubscriberDrain,
		Required:    true,
	}, func(c *Context) error {
		val := string(c.Request.(*pubsub.Message).Value)
		c2Mu.Lock()
		c2Seen = append(c2Seen, val)
		if len(c2Seen) == 2 {
			select {
			case <-allRecovered:
			default:
				close(allRecovered)
			}
		}
		c2Mu.Unlock()
		return nil
	}))

	require.NoError(t, app2.Start(t.Context()))
	select {
	case <-allRecovered:
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for consumer 2 to re-consume uncommitted offsets; seen=%v", c2Seen)
	}

	require.NoError(t, app2.Stop(t.Context()))

	c2Mu.Lock()
	assert.Equal(t, []string{"msg-1-fail", "msg-2-after-fail"}, c2Seen, "Consumer 2 in same consumer group must receive msg-1-fail again and never skip failed offset")
	c2Mu.Unlock()
}
