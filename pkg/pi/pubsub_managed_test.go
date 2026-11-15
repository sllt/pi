package pi

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
)

func TestManagedPubSub_TwoStageValidation(t *testing.T) {
	t.Run("KAFKA publisher-only succeeds in Build without CONSUMER_ID, subscriber registration requires CONSUMER_ID", func(t *testing.T) {
		app, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED":   "false",
				"GRPC_ENABLED":   "false",
				"PUBSUB_BACKEND": "KAFKA",
				"PUBSUB_BROKER":  "127.0.0.1:9092",
			}),
			WithManagedPubSub(),
		)
		require.NoError(t, err)
		require.NotNil(t, app.PubSub())

		// Stage 2A: AddSubscriber immediately returns error when CONSUMER_ID is missing.
		subErr := app.AddSubscriber("orders", SubscribePolicy{}, func(*Context) error { return nil })
		require.Error(t, subErr)
		assert.Contains(t, subErr.Error(), "CONSUMER_ID")

		// Stage 2B: Legacy void Subscribe records subConfigErr and fails Start before dialing.
		app2, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED":   "false",
				"GRPC_ENABLED":   "false",
				"PUBSUB_BACKEND": "KAFKA",
				"PUBSUB_BROKER":  "127.0.0.1:9092",
			}),
			WithManagedPubSub(),
		)
		require.NoError(t, err)
		app2.Subscribe("orders", func(*Context) error { return nil })

		startErr := app2.Start(context.Background())
		require.Error(t, startErr)
		assert.Contains(t, startErr.Error(), "CONSUMER_ID")
	})

	t.Run("GOOGLE publisher-only succeeds without GOOGLE_SUBSCRIPTION_NAME, subscriber requires it", func(t *testing.T) {
		app, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED":      "false",
				"GRPC_ENABLED":      "false",
				"PUBSUB_BACKEND":    "GOOGLE",
				"GOOGLE_PROJECT_ID": "demo-project",
			}),
			WithManagedPubSub(),
		)
		require.NoError(t, err)

		subErr := app.AddSubscriber("events", SubscribePolicy{}, func(*Context) error { return nil })
		require.Error(t, subErr)
		assert.Contains(t, subErr.Error(), "GOOGLE_SUBSCRIPTION_NAME")
	})

	t.Run("REDIS streams requires REDIS_STREAMS_CONSUMER_GROUP for subscribers, pubsub mode does not", func(t *testing.T) {
		streamsApp, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED":   "false",
				"GRPC_ENABLED":   "false",
				"PUBSUB_BACKEND": "REDIS",
				"REDIS_HOST":     "127.0.0.1",
			}),
			WithManagedPubSub(),
		)
		require.NoError(t, err)

		subErr := streamsApp.AddSubscriber("jobs", SubscribePolicy{}, func(*Context) error { return nil })
		require.Error(t, subErr)
		assert.Contains(t, subErr.Error(), "REDIS_STREAMS_CONSUMER_GROUP")

		pubsubApp, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED":      "false",
				"GRPC_ENABLED":      "false",
				"PUBSUB_BACKEND":    "REDIS",
				"REDIS_HOST":        "127.0.0.1",
				"REDIS_PUBSUB_MODE": "pubsub",
			}),
			WithManagedPubSub(),
		)
		require.NoError(t, err)
		require.NoError(t, pubsubApp.AddSubscriber("jobs", SubscribePolicy{}, func(*Context) error { return nil }))
	})

	t.Run("MQTT requires explicit MQTT_HOST in Build and never falls back to public broker", func(t *testing.T) {
		_, err := Build(
			WithConfig(map[string]string{
				"HTTP_ENABLED":   "false",
				"GRPC_ENABLED":   "false",
				"PUBSUB_BACKEND": "MQTT",
			}),
			WithManagedPubSub(),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "MQTT_HOST")
	})
}

func TestManagedPubSub_StaticAckSemanticsAndLifecycleGuards(t *testing.T) {
	cases := []struct {
		name     string
		cfg      map[string]string
		expected pubsub.AckSemantics
	}{
		{
			name: "KAFKA cumulative offset",
			cfg: map[string]string{
				"PUBSUB_BACKEND": "KAFKA",
				"PUBSUB_BROKER":  "127.0.0.1:9092",
			},
			expected: pubsub.AckSemantics{
				Mode:                  pubsub.AckModeCumulativeOffset,
				SupportsContextCommit: true,
			},
		},
		{
			name: "GOOGLE independent with Nack",
			cfg: map[string]string{
				"PUBSUB_BACKEND":    "GOOGLE",
				"GOOGLE_PROJECT_ID": "demo",
			},
			expected: pubsub.AckSemantics{
				Mode:         pubsub.AckModeIndependent,
				SupportsNack: true,
			},
		},
		{
			name: "REDIS streams independent with ContextCommit",
			cfg: map[string]string{
				"PUBSUB_BACKEND": "REDIS",
				"REDIS_HOST":     "127.0.0.1",
			},
			expected: pubsub.AckSemantics{
				Mode:                  pubsub.AckModeIndependent,
				SupportsContextCommit: true,
			},
		},
		{
			name: "REDIS pubsub auto ack",
			cfg: map[string]string{
				"PUBSUB_BACKEND":    "REDIS",
				"REDIS_HOST":        "127.0.0.1",
				"REDIS_PUBSUB_MODE": "pubsub",
			},
			expected: pubsub.AckSemantics{
				Mode: pubsub.AckModeAuto,
			},
		},
		{
			name: "MQTT auto ack",
			cfg: map[string]string{
				"PUBSUB_BACKEND": "MQTT",
				"MQTT_HOST":      "127.0.0.1",
			},
			expected: pubsub.AckSemantics{
				Mode: pubsub.AckModeAuto,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}
			for k, v := range tc.cfg {
				cfg[k] = v
			}
			app, err := Build(WithConfig(cfg), WithManagedPubSub())
			require.NoError(t, err)

			provider, ok := app.PubSub().(pubsub.AckSemanticsProvider)
			require.True(t, ok)
			assert.Equal(t, tc.expected, provider.AckSemantics())

			// Pre-Start operations fail cleanly without nil-pointer panics.
			ctx := context.Background()
			assert.ErrorIs(t, app.PubSub().Publish(ctx, "t", []byte("x")), errPubSubNotStarted)
			_, err = app.PubSub().Subscribe(ctx, "t")
			assert.ErrorIs(t, err, errPubSubNotStarted)
			assert.ErrorIs(t, app.PubSub().CreateTopic(ctx, "t"), errPubSubNotStarted)
			assert.ErrorIs(t, app.PubSub().DeleteTopic(ctx, "t"), errPubSubNotStarted)
			_, err = app.PubSub().Query(ctx, "q")
			assert.ErrorIs(t, err, errPubSubNotStarted)
			assert.Equal(t, datasource.StatusDown, app.PubSub().Health().Status)

			require.NoError(t, app.PubSub().Close())
			assert.ErrorIs(t, app.PubSub().Publish(ctx, "t", []byte("x")), errPubSubClosed)
			require.NoError(t, app.PubSub().Close())
		})
	}
}
