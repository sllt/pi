package pi

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/logging"
)

type subscriptionReadProbe struct {
	mockSubscriber
	read func(context.Context, string) (*pubsub.Message, error)
}

func (p subscriptionReadProbe) Subscribe(ctx context.Context, topic string) (*pubsub.Message, error) {
	return p.read(ctx, topic)
}

type commitProbe struct{ count atomic.Int32 }

func (p *commitProbe) Commit() { p.count.Add(1) }

func TestSubscription_CommitOnlySuccessfulActiveMessage(t *testing.T) {
	failure := errors.New("processing failed")
	for _, outcome := range []string{"success", "error", "panic", "nil-panic", "canceled-before-read", "canceled-after-read",
		"canceled-in-handler", "message-canceled", "message-canceled-in-handler", "read-error", "no-message", "no-committer"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			msgCtx, cancelMessage := context.WithCancel(context.Background())
			defer cancelMessage()
			commits := &commitProbe{}
			message := pubsub.NewMessage(msgCtx)
			message.Committer = commits
			if outcome == "no-committer" {
				message.Committer = nil
			}
			if outcome == "canceled-before-read" {
				cancel()
			}
			if outcome == "message-canceled" {
				cancelMessage()
			}
			var reads, calls int
			client := subscriptionReadProbe{read: func(context.Context, string) (*pubsub.Message, error) {
				reads++
				switch outcome {
				case "read-error":
					return nil, failure
				case "no-message":
					return nil, nil
				case "canceled-after-read":
					cancel()
				}
				return message, nil
			}}
			manager := newSubscriptionManager(&infra.Container{Logger: logging.NewLogger(logging.FATAL), PubSub: client})
			err := manager.handleSubscription(ctx, "orders", func(*Context) error {
				calls++
				switch outcome {
				case "error":
					return failure
				case "panic":
					panic("processing panic")
				case "nil-panic":
					panic(nil)
				case "canceled-in-handler":
					cancel()
				case "message-canceled-in-handler":
					cancelMessage()
				}
				return nil
			})
			switch outcome {
			case "success":
				require.NoError(t, err)
				assert.EqualValues(t, 1, commits.count.Load())
			case "no-message", "no-committer":
				require.NoError(t, err)
				assert.Zero(t, commits.count.Load())
			case "panic", "nil-panic":
				require.ErrorIs(t, err, errSubscriptionHandlerPanic)
				assert.Zero(t, commits.count.Load())
			case "error", "read-error":
				require.ErrorIs(t, err, failure)
				assert.Zero(t, commits.count.Load())
			default:
				require.ErrorIs(t, err, context.Canceled)
				assert.Zero(t, commits.count.Load())
			}
			if outcome == "canceled-before-read" {
				assert.Zero(t, reads)
			}
			switch outcome {
			case "canceled-before-read", "canceled-after-read", "message-canceled", "read-error", "no-message":
				assert.Zero(t, calls)
			default:
				assert.Equal(t, 1, calls)
			}
		})
	}
}

func TestSubscription_RuntimeCancelsDetachedMessage(t *testing.T) {
	type key struct{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	message := pubsub.NewMessage(context.WithValue(context.Background(), key{}, "message-trace-value"))
	commits := &commitProbe{}
	message.Committer = commits
	client := subscriptionReadProbe{read: func(context.Context, string) (*pubsub.Message, error) { return message, nil }}
	manager := newSubscriptionManager(&infra.Container{Logger: logging.NewLogger(logging.FATAL), PubSub: client})
	entered, result := make(chan struct{}), make(chan error, 1)
	go func() {
		result <- manager.handleSubscription(ctx, "orders", func(c *Context) error {
			assert.Equal(t, "message-trace-value", c.Value(key{}))
			close(entered)
			<-c.Done()
			return nil // Even a handler that swallows cancellation must not auto-commit.
		})
	}()
	waitForHandlerSignal(t, entered)
	cancel()
	require.ErrorIs(t, lifecycleResult(t, result), context.Canceled)
	assert.Zero(t, commits.count.Load())
}

func TestSubscription_CancelStopsRetryLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commits := &commitProbe{}
	message := pubsub.NewMessage(context.Background())
	message.Committer = commits
	var reads int
	client := subscriptionReadProbe{read: func(context.Context, string) (*pubsub.Message, error) { reads++; return message, nil }}
	manager := newSubscriptionManager(&infra.Container{Logger: logging.NewLogger(logging.FATAL), PubSub: client})
	err := manager.startSubscriber(ctx, "orders", func(*Context) error { cancel(); panic("failed on shutdown") })
	require.NoError(t, err)
	assert.Equal(t, 1, reads)
	assert.Zero(t, commits.count.Load())
}
