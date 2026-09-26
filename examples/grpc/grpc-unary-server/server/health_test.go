package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

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

func TestPiHealthServer_Creation(t *testing.T) {
	t.Run("GetOrCreateHealthServer", func(t *testing.T) {
		// Test Pi's getOrCreateHealthServer function
		healthServer := getOrCreateHealthServer()
		assert.NotNil(t, healthServer, "Pi health server should not be nil")

		// Test that it implements the Pi interface (not the standard gRPC interface)
		// The Pi health server has different method signatures
		assert.NotNil(t, healthServer, "Health server should not be nil")
	})

	t.Run("HealthServerSingleton", func(t *testing.T) {
		// Test Pi's singleton pattern for health server
		healthServer1 := getOrCreateHealthServer()
		healthServer2 := getOrCreateHealthServer()

		assert.Equal(t, healthServer1, healthServer2, "Pi health server should be singleton")
	})
}

func TestPiHealthServer_Methods(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	// Test Pi's health server methods
	healthServer := getOrCreateHealthServer()
	ctx := createTestContext()

	t.Run("CheckMethodExists", func(t *testing.T) {
		// Test that Pi's Check method exists and accepts correct parameters
		req := &healthpb.HealthCheckRequest{
			Service: "test-service",
		}

		// Test Pi's Check method signature - this will fail with "unknown service" which is expected
		resp, err := healthServer.Check(ctx, req)
		assert.Error(t, err, "Health check should fail for unknown service")
		assert.Nil(t, resp, "Health check response should be nil for unknown service")
		assert.Contains(t, err.Error(), "unknown service", "Error should indicate unknown service")
	})

	t.Run("WatchMethodExists", func(t *testing.T) {
		// Test that Pi's Watch method exists and accepts correct parameters
		req := &healthpb.HealthCheckRequest{
			Service: "test-service",
		}

		// Test Pi's Watch method signature - this will panic with nil stream, but we're testing method existence
		assert.Panics(t, func() {
			healthServer.Watch(ctx, req, nil)
		}, "Watch should panic with nil stream, but method should exist")
	})
}

func TestPiHealthServer_SetServingStatus(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	// Test Pi's SetServingStatus functionality
	healthServer := getOrCreateHealthServer()
	ctx := createTestContext()

	t.Run("SetServingStatus", func(t *testing.T) {
		// Test Pi's SetServingStatus method
		healthServer.SetServingStatus(ctx, "test-service", healthpb.HealthCheckResponse_SERVING)

		// Verify the status was set
		req := &healthpb.HealthCheckRequest{
			Service: "test-service",
		}
		resp, err := healthServer.Check(ctx, req)
		require.NoError(t, err, "Health check should not fail")
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status, "Service should be serving")
	})

	t.Run("SetNotServingStatus", func(t *testing.T) {
		// Test Pi's SetServingStatus with NOT_SERVING
		healthServer.SetServingStatus(ctx, "test-service-not-serving", healthpb.HealthCheckResponse_NOT_SERVING)

		// Verify the status was set
		req := &healthpb.HealthCheckRequest{
			Service: "test-service-not-serving",
		}
		resp, err := healthServer.Check(ctx, req)
		require.NoError(t, err, "Health check should not fail")
		assert.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, resp.Status, "Service should not be serving")
	})
}

func TestPiHealthServer_Shutdown(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	// Test Pi's Shutdown functionality
	healthServer := getOrCreateHealthServer()
	ctx := createTestContext()

	t.Run("Shutdown", func(t *testing.T) {
		// Test Pi's Shutdown method
		healthServer.Shutdown(ctx)

		// After shutdown, all services should return NOT_SERVING
		req := &healthpb.HealthCheckRequest{
			Service: "any-service",
		}
		resp, err := healthServer.Check(ctx, req)
		// After shutdown, health checks should fail with "unknown service"
		assert.Error(t, err, "Health check should fail after shutdown")
		assert.Nil(t, resp, "Health check response should be nil after shutdown")
		assert.Contains(t, err.Error(), "unknown service", "Error should indicate unknown service after shutdown")
	})
}

func TestPiHealthServer_Resume(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	// Test Pi's Resume functionality
	healthServer := getOrCreateHealthServer()
	ctx := createTestContext()

	t.Run("Resume", func(t *testing.T) {
		// Test Pi's Resume method
		healthServer.Resume(ctx)

		// After resume, services should return to their previous status
		healthServer.SetServingStatus(ctx, "test-service-resume", healthpb.HealthCheckResponse_SERVING)

		req := &healthpb.HealthCheckRequest{
			Service: "test-service-resume",
		}
		resp, err := healthServer.Check(ctx, req)
		require.NoError(t, err, "Health check should not fail")
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status, "Service should be serving after resume")
	})
}

func TestPiHealthServer_MultipleInstances(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	// Test Pi's singleton pattern
	t.Run("SingletonPattern", func(t *testing.T) {
		healthServer1 := getOrCreateHealthServer()
		healthServer2 := getOrCreateHealthServer()
		ctx := createTestContext()

		assert.Equal(t, healthServer1, healthServer2, "Pi health server should be singleton")

		// Test that operations on one affect the other
		healthServer1.SetServingStatus(ctx, "singleton-test", healthpb.HealthCheckResponse_SERVING)

		req := &healthpb.HealthCheckRequest{
			Service: "singleton-test",
		}
		resp, err := healthServer2.Check(ctx, req)
		require.NoError(t, err, "Health check should not fail")
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status, "Singleton should share state")
	})
}
