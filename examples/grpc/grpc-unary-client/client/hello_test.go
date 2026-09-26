package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/testutil"
)

// createTestContext creates a test pi.Context
func createTestContext() *pi.Context {
	container := &infra.Container{}
	return &pi.Context{
		Context:   context.Background(),
		Container: container,
	}
}

func TestPiHelloClientWrapper_Creation(t *testing.T) {
	configs := testutil.NewServerConfigs(t)

	t.Run("NewHelloPiClient", func(t *testing.T) {
		// Test Pi's NewHelloPiClient function
		conn, err := grpc.Dial(configs.GRPCHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err, "Connection creation should not fail immediately")
		defer conn.Close()

		app := pi.New()
		helloClient, err := NewHelloPiClient(configs.GRPCHost, app.Metrics())
		require.NoError(t, err, "Pi hello client creation should not fail")
		assert.NotNil(t, helloClient, "Pi hello client should not be nil")

		// Test that it implements the Pi interface
		var _ HelloPiClient = helloClient
	})

	t.Run("HelloClientWrapperInterface", func(t *testing.T) {
		// Test Pi's interface compliance
		conn, err := grpc.Dial(configs.GRPCHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err, "Connection creation should not fail immediately")
		defer conn.Close()

		app := pi.New()
		helloClient, err := NewHelloPiClient(configs.GRPCHost, app.Metrics())
		require.NoError(t, err, "Pi hello client creation should not fail")

		// Test HelloPiClient interface compliance
		var _ HelloPiClient = helloClient

		// Test that wrapper has the correct Pi type
		wrapper, ok := helloClient.(*HelloClientWrapper)
		assert.True(t, ok, "Should be able to cast to Pi HelloClientWrapper")
		assert.NotNil(t, wrapper.client, "Underlying hello client should not be nil")
	})
}

func TestPiHelloClientWrapper_Methods(t *testing.T) {
	configs := testutil.NewServerConfigs(t)

	// Test Pi's wrapper methods without actual gRPC calls
	app := pi.New()
	helloClient, err := NewHelloPiClient(configs.GRPCHost, app.Metrics())
	require.NoError(t, err, "Pi hello client creation should not fail")
	ctx := createTestContext()

	t.Run("SayHelloMethodExists", func(t *testing.T) {
		// Test that Pi's SayHello method exists and accepts correct parameters
		req := &HelloRequest{
			Name: "test-name",
		}

		// This will fail due to connection, but we're testing Pi's method signature
		_, err := helloClient.SayHello(ctx, req)
		assert.Error(t, err, "Should fail with invalid connection, but method should exist")
	})

	t.Run("HealthClientEmbedded", func(t *testing.T) {
		// Test that Pi's HelloPiClient embeds HealthClient
		// The HelloPiClient interface should include HealthClient methods
		var _ HealthClient = helloClient
	})
}

func TestPiHelloClientWrapper_ContextIntegration(t *testing.T) {
	configs := testutil.NewServerConfigs(t)

	// Test Pi's context integration
	app := pi.New()
	helloClient, err := NewHelloPiClient(configs.GRPCHost, app.Metrics())
	require.NoError(t, err, "Pi hello client creation should not fail")

	t.Run("ContextParameter", func(t *testing.T) {
		// Test that Pi's methods accept *pi.Context
		ctx := createTestContext()
		req := &HelloRequest{
			Name: "test-name",
		}

		// Test that the method signature is correct for Pi context
		_, err := helloClient.SayHello(ctx, req)
		assert.Error(t, err, "Should fail with invalid connection")

		// Test that context is properly passed (even though call fails)
		assert.NotNil(t, ctx, "Pi context should not be nil")
	})

	t.Run("ContextTypeCompliance", func(t *testing.T) {
		// Test that Pi's methods expect *pi.Context specifically
		ctx := createTestContext()
		req := &HelloRequest{
			Name: "test-name",
		}

		// Verify the method signature expects *pi.Context
		var _ func(*pi.Context, *HelloRequest, ...grpc.CallOption) (*HelloResponse, error) = helloClient.SayHello

		// Ensure the call compiles (even if it fails at runtime)
		_, _ = helloClient.SayHello(ctx, req)
	})
}

func TestPiHelloClientWrapper_MultipleInstances(t *testing.T) {
	configs := testutil.NewServerConfigs(t)

	// Test Pi's client creation with multiple instances
	t.Run("MultipleHelloClients", func(t *testing.T) {
		app := pi.New()

		client1, err := NewHelloPiClient(configs.GRPCHost, app.Metrics())
		require.NoError(t, err, "First Pi hello client creation should not fail")

		client2, err := NewHelloPiClient(configs.GRPCHost, app.Metrics())
		require.NoError(t, err, "Second Pi hello client creation should not fail")

		assert.NotNil(t, client1, "First Pi hello client should not be nil")
		assert.NotNil(t, client2, "Second Pi hello client should not be nil")
		assert.NotEqual(t, client1, client2, "Pi hello client instances should be different")
	})
}

func TestPiHelloClientWrapper_ErrorHandling(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	// Test Pi's error handling patterns
	t.Run("InvalidAddressHandling", func(t *testing.T) {
		// Test Pi's handling of invalid addresses
		app := pi.New()
		helloClient, err := NewHelloPiClient("invalid:address", app.Metrics())
		require.NoError(t, err, "Client creation should not fail immediately")

		ctx := createTestContext()
		req := &HelloRequest{
			Name: "test-name",
		}

		// Test Pi's error handling
		_, err = helloClient.SayHello(ctx, req)
		assert.Error(t, err, "Pi should handle invalid address errors")
	})

	t.Run("EmptyAddressHandling", func(t *testing.T) {
		// Test Pi's handling of empty addresses
		app := pi.New()
		helloClient, err := NewHelloPiClient("", app.Metrics())
		require.NoError(t, err, "Client creation should not fail immediately")

		ctx := createTestContext()
		req := &HelloRequest{
			Name: "test-name",
		}

		// Test Pi's error handling
		_, err = helloClient.SayHello(ctx, req)
		assert.Error(t, err, "Pi should handle empty address errors")
	})
}

func TestPiHelloClientWrapper_ConcurrentAccess(t *testing.T) {
	configs := testutil.NewServerConfigs(t)

	// Test Pi's concurrent access patterns
	t.Run("ConcurrentSayHelloCalls", func(t *testing.T) {
		app := pi.New()
		helloClient, err := NewHelloPiClient(configs.GRPCHost, app.Metrics())
		require.NoError(t, err, "Pi hello client creation should not fail")

		numGoroutines := 5
		done := make(chan bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				ctx := createTestContext()
				req := &HelloRequest{
					Name: "concurrent-test",
				}

				// This will fail due to connection, but we're testing Pi's concurrency
				_, err := helloClient.SayHello(ctx, req)
				assert.Error(t, err, "Should fail with invalid connection")
				done <- true
			}(i)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}
	})
}
