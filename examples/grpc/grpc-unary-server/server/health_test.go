package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func createTestApp() *pi.App {
	app, _ := pi.Build(pi.WithConfig(map[string]string{
		"HTTP_ENABLED": "false",
		"GRPC_ENABLED": "false",
	}))
	return app
}

// createTestContext creates a test pi.Context
func createTestContext() *pi.Context {
	return createTestApp().NewContext(context.Background())
}

func TestPiHealthServer_Creation(t *testing.T) {
	t.Run("GetOrCreateHealthServer", func(t *testing.T) {
		healthServer := getOrCreateHealthServer()
		assert.NotNil(t, healthServer, "Pi health server should not be nil")
	})

	t.Run("DistinctHealthServerInstances", func(t *testing.T) {
		healthServer1 := getOrCreateHealthServer()
		healthServer2 := getOrCreateHealthServer()

		assert.NotSame(t, healthServer1, healthServer2, "Each compatibility constructor call owns a distinct server")
	})
}

func TestPiHealthServer_Methods(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	healthServer := getOrCreateHealthServer()
	ctx := createTestContext()

	t.Run("CheckMethodExists", func(t *testing.T) {
		req := &healthpb.HealthCheckRequest{
			Service: "test-service",
		}

		resp, err := healthServer.Check(ctx, req)
		assert.Error(t, err, "Health check should fail for unknown service")
		assert.Nil(t, resp, "Health check response should be nil for unknown service")
		assert.Contains(t, err.Error(), "unknown service", "Error should indicate unknown service")
	})

	t.Run("WatchMethodExists", func(t *testing.T) {
		req := &healthpb.HealthCheckRequest{
			Service: "test-service",
		}

		assert.Panics(t, func() {
			_ = healthServer.Watch(ctx, req, nil)
		}, "Watch should panic with nil stream, but method should exist")
	})
}

func TestPiHealthServer_SetServingStatus(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	healthServer := getOrCreateHealthServer()
	ctx := createTestContext()

	t.Run("SetServingStatus", func(t *testing.T) {
		healthServer.SetServingStatus(ctx, "test-service", healthpb.HealthCheckResponse_SERVING)

		req := &healthpb.HealthCheckRequest{
			Service: "test-service",
		}
		resp, err := healthServer.Check(ctx, req)
		require.NoError(t, err, "Health check should not fail")
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status, "Service should be serving")
	})

	t.Run("SetNotServingStatus", func(t *testing.T) {
		healthServer.SetServingStatus(ctx, "test-service-not-serving", healthpb.HealthCheckResponse_NOT_SERVING)

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

	healthServer := getOrCreateHealthServer()
	ctx := createTestContext()

	t.Run("Shutdown", func(t *testing.T) {
		healthServer.Shutdown(ctx)

		req := &healthpb.HealthCheckRequest{
			Service: "any-service",
		}
		resp, err := healthServer.Check(ctx, req)
		assert.Error(t, err, "Health check should fail after shutdown")
		assert.Nil(t, resp, "Health check response should be nil after shutdown")
		assert.Contains(t, err.Error(), "unknown service", "Error should indicate unknown service after shutdown")
	})
}

func TestPiHealthServer_Resume(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	healthServer := getOrCreateHealthServer()
	ctx := createTestContext()

	t.Run("Resume", func(t *testing.T) {
		healthServer.Resume(ctx)

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

	t.Run("AppScopedIsolation", func(t *testing.T) {
		healthServer1 := getOrCreateHealthServer()
		healthServer2 := getOrCreateHealthServer()
		ctx := createTestContext()

		healthServer1.SetServingStatus(ctx, "isolated-service", healthpb.HealthCheckResponse_SERVING)

		req := &healthpb.HealthCheckRequest{
			Service: "isolated-service",
		}
		resp1, err1 := healthServer1.Check(ctx, req)
		require.NoError(t, err1)
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp1.Status)

		_, err2 := healthServer2.Check(ctx, req)
		assert.Error(t, err2, "Separate healthServer instance should stay isolated")
	})
}
