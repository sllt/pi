package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func TestPiHealthClientWrapper_Creation(t *testing.T) {
	t.Setenv("PI_TELEMETRY", "false")

	configs := testutil.NewServerConfigs(t)

	t.Run("NewHealthClient", func(t *testing.T) {
		// Test Pi's NewHealthClient function
		conn, err := grpc.Dial(configs.GRPCHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err, "Connection creation should not fail immediately")
		defer conn.Close()

		healthClient := NewHealthClient(conn)
		assert.NotNil(t, healthClient, "Pi health client should not be nil")

		// Test that it implements the Pi interface
		var _ HealthClient = healthClient
	})

	t.Run("HealthClientWrapperInterface", func(t *testing.T) {
		// Test Pi's interface compliance
		conn, err := grpc.Dial(configs.GRPCHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err, "Connection creation should not fail immediately")
		defer conn.Close()

		healthClient := NewHealthClient(conn)

		// Test HealthClient interface compliance
		var _ HealthClient = healthClient

		// Test that wrapper has the correct Pi type
		wrapper, ok := healthClient.(*HealthClientWrapper)
		assert.True(t, ok, "Should be able to cast to Pi HealthClientWrapper")
		assert.NotNil(t, wrapper.client, "Underlying health client should not be nil")
	})
}

func TestPiHealthClientWrapper_Methods(t *testing.T) {
	t.Setenv("PI_TELEMETRY", "false")

	configs := testutil.NewServerConfigs(t)

	// Test Pi's wrapper methods without actual gRPC calls
	conn, err := grpc.Dial(configs.GRPCHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "Connection creation should not fail immediately")
	defer conn.Close()

	healthClient := NewHealthClient(conn)
	ctx := createTestContext()

	t.Run("CheckMethodExists", func(t *testing.T) {
		// Test that Pi's Check method exists and accepts correct parameters
		req := &healthpb.HealthCheckRequest{
			Service: "test-service",
		}

		// This will fail due to connection, but we're testing Pi's method signature
		_, err := healthClient.Check(ctx, req)
		assert.Error(t, err, "Should fail with invalid connection, but method should exist")
	})

	t.Run("WatchMethodExists", func(t *testing.T) {
		// Test that Pi's Watch method exists and accepts correct parameters
		req := &healthpb.HealthCheckRequest{
			Service: "test-service",
		}

		// This will fail due to connection, but we're testing Pi's method signature
		_, err := healthClient.Watch(ctx, req)
		assert.Error(t, err, "Should fail with invalid connection, but method should exist")
	})
}

func TestPiHealthClientWrapper_ContextIntegration(t *testing.T) {
	t.Setenv("PI_TELEMETRY", "false")

	configs := testutil.NewServerConfigs(t)

	// Test Pi's context integration
	conn, err := grpc.Dial(configs.GRPCHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "Connection creation should not fail immediately")
	defer conn.Close()

	healthClient := NewHealthClient(conn)

	t.Run("ContextParameter", func(t *testing.T) {
		// Test that Pi's methods accept *pi.Context
		ctx := createTestContext()
		req := &healthpb.HealthCheckRequest{
			Service: "test-service",
		}

		// Test that the method signature is correct for Pi context
		_, err := healthClient.Check(ctx, req)
		assert.Error(t, err, "Should fail with invalid connection")

		// Test that context is properly passed (even though call fails)
		assert.NotNil(t, ctx, "Pi context should not be nil")
	})

	t.Run("ContextTypeCompliance", func(t *testing.T) {
		// Test that Pi's methods expect *pi.Context specifically
		ctx := createTestContext()
		req := &healthpb.HealthCheckRequest{
			Service: "test-service",
		}

		// Verify the method signature expects *pi.Context
		var _ func(*pi.Context, *healthpb.HealthCheckRequest, ...grpc.CallOption) (*healthpb.HealthCheckResponse, error) = healthClient.Check
		var _ func(*pi.Context, *healthpb.HealthCheckRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[healthpb.HealthCheckResponse], error) = healthClient.Watch

		// Ensure the call compiles (even if it fails at runtime)
		_, _ = healthClient.Check(ctx, req)
		_, _ = healthClient.Watch(ctx, req)
	})
}

func TestPiHealthClientWrapper_ErrorHandling(t *testing.T) {
	t.Setenv("PI_TELEMETRY", "false")

	// Test Pi's error handling patterns
	t.Run("InvalidConnectionHandling", func(t *testing.T) {
		// Test Pi's handling of invalid connections
		conn, err := grpc.Dial("invalid:address", grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err, "Connection creation should not fail immediately")
		defer conn.Close()

		healthClient := NewHealthClient(conn)
		ctx := createTestContext()

		req := &healthpb.HealthCheckRequest{
			Service: "test-service",
		}

		// Test Pi's error handling
		_, err = healthClient.Check(ctx, req)
		assert.Error(t, err, "Pi should handle invalid connection errors")
	})
}
