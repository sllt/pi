package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func TestPiHelloServer_Creation(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	t.Run("HelloPiServerCreation", func(t *testing.T) {
		app := createTestApp()
		helloServer := &HelloPiServer{}

		assert.NotNil(t, helloServer, "Pi hello server should not be nil")
		assert.NotNil(t, app, "Pi app should not be nil")

		var _ HelloServerWithPi = helloServer
	})

	t.Run("HelloServerWrapperCreation", func(t *testing.T) {
		app := createTestApp()
		helloServer := &HelloPiServer{}
		wrapper := &HelloServerWrapper{
			app:    app,
			server: helloServer,
		}

		assert.NotNil(t, wrapper, "Pi hello server wrapper should not be nil")
		assert.Equal(t, helloServer, wrapper.server, "Wrapper should contain the server")
		assert.NotNil(t, app, "Pi app should not be nil")
	})
}

func TestPiHelloServer_Methods(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	helloServer := &HelloPiServer{}
	ctx := createTestContext()

	t.Run("SayHelloMethodExists", func(t *testing.T) {
		ctx.Request = &HelloRequestWrapper{
			HelloRequest: &HelloRequest{
				Name: "test-name",
			},
		}

		resp, err := helloServer.SayHello(ctx)
		require.NoError(t, err, "Pi SayHello should not fail")
		assert.NotNil(t, resp, "SayHello response should not be nil")

		helloResp, ok := resp.(*HelloResponse)
		assert.True(t, ok, "Response should be HelloResponse")
		assert.Contains(t, helloResp.Message, "test-name", "Response should contain the name")
	})

	t.Run("SayHelloWithEmptyName", func(t *testing.T) {
		ctx.Request = &HelloRequestWrapper{
			HelloRequest: &HelloRequest{
				Name: "",
			},
		}

		resp, err := helloServer.SayHello(ctx)
		require.NoError(t, err, "Pi SayHello with empty name should not fail")
		assert.NotNil(t, resp, "SayHello response should not be nil")

		helloResp, ok := resp.(*HelloResponse)
		assert.True(t, ok, "Response should be HelloResponse")
		assert.Contains(t, helloResp.Message, "World", "Empty name should default to World")
	})
}

func TestPiHelloServer_ContextIntegration(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	helloServer := &HelloPiServer{}

	t.Run("ContextBinding", func(t *testing.T) {
		ctx := createTestContext()
		ctx.Request = &HelloRequestWrapper{
			HelloRequest: &HelloRequest{
				Name: "context-test",
			},
		}

		resp, err := helloServer.SayHello(ctx)
		require.NoError(t, err, "Pi SayHello should not fail")
		assert.NotNil(t, resp, "SayHello response should not be nil")

		helloResp, ok := resp.(*HelloResponse)
		assert.True(t, ok, "Response should be HelloResponse")
		assert.Contains(t, helloResp.Message, "context-test", "Response should contain the context name")
	})

	t.Run("ContextTypeCompliance", func(t *testing.T) {
		ctx := createTestContext()
		ctx.Request = &HelloRequestWrapper{
			HelloRequest: &HelloRequest{
				Name: "type-test",
			},
		}

		var _ func(*pi.Context) (any, error) = helloServer.SayHello
		_, _ = helloServer.SayHello(ctx)
	})
}

func TestPiHelloServer_Registration(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	t.Run("RegisterHelloServerWithPi", func(t *testing.T) {
		app := pi.New()
		helloServer := &HelloPiServer{}

		assert.NotPanics(t, func() {
			RegisterHelloServerWithPi(app, helloServer)
		}, "RegisterHelloServerWithPi should not panic")
	})
}

func TestPiHelloServer_HealthIntegration(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	t.Run("HealthIntegration", func(t *testing.T) {
		app := pi.New()
		helloServer := &HelloPiServer{}

		RegisterHelloServerWithPi(app, helloServer)

		hs := &healthServer{app.GRPCHealthServer()}
		ctx := app.NewContext(context.Background())

		req := &healthpb.HealthCheckRequest{
			Service: "Hello",
		}
		resp, err := hs.Check(ctx, req)
		require.NoError(t, err, "Health check should not fail")
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status, "Hello service should be serving")
	})
}

func TestPiHelloServer_MultipleInstances(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	t.Run("MultipleHelloServers", func(t *testing.T) {
		app := createTestApp()

		server1 := &HelloPiServer{}
		server2 := &HelloPiServer{}

		assert.NotNil(t, server1)
		assert.NotNil(t, server2)
		assert.True(t, server1 != server2, "Pi hello server instances should be different objects")
		assert.NotNil(t, app)
	})
}

func TestNewHelloPiServer(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	t.Run("NewHelloPiServerCreation", func(t *testing.T) {
		server := NewHelloPiServer()

		assert.NotNil(t, server, "NewHelloPiServer should not return nil")
		assert.NotNil(t, server.health, "Health server should be initialized")

		var _ HelloServerWithPi = server
	})
}

func TestHelloServerWrapper_SayHello(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	t.Run("SayHelloWrapper", func(t *testing.T) {
		wrapper := &HelloServerWrapper{
			server:       &mockHelloServer{},
			healthServer: getOrCreateHealthServer(),
			app:          createTestApp(),
		}

		ctx := context.Background()
		req := &HelloRequest{Name: "test"}

		resp, err := wrapper.SayHello(ctx, req)

		require.NoError(t, err, "SayHello should not fail")
		assert.NotNil(t, resp, "Response should not be nil")
		assert.Equal(t, "Hello test!", resp.Message, "Response message should match")
	})

	t.Run("SayHelloWithError", func(t *testing.T) {
		wrapper := &HelloServerWrapper{
			server:       &mockHelloServerWithError{},
			healthServer: getOrCreateHealthServer(),
			app:          createTestApp(),
		}

		ctx := context.Background()
		req := &HelloRequest{Name: "error"}

		resp, err := wrapper.SayHello(ctx, req)

		assert.Error(t, err, "SayHello should return error")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "internal server error", "Internal error should be sanitized")
	})

	t.Run("SayHelloWithWrongResponseType", func(t *testing.T) {
		wrapper := &HelloServerWrapper{
			server:       &mockHelloServerWrongType{},
			healthServer: getOrCreateHealthServer(),
			app:          createTestApp(),
		}

		ctx := context.Background()
		req := &HelloRequest{Name: "wrong"}

		resp, err := wrapper.SayHello(ctx, req)

		assert.Error(t, err, "SayHello should return error for wrong response type")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "internal server error", "Error message should indicate internal server error")
	})
}

func TestHelloServerWrapper_getPiContext(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	t.Run("getPiContext", func(t *testing.T) {
		app := createTestApp()
		wrapper := &HelloServerWrapper{
			app: app,
		}

		ctx := context.Background()
		req := &HelloRequestWrapper{
			HelloRequest: &HelloRequest{Name: "test"},
		}

		piCtx := wrapper.getPiContext(ctx, req)

		assert.NotNil(t, piCtx, "Pi context should not be nil")
		assert.Equal(t, ctx, piCtx.Context, "Context should match")
		assert.Equal(t, req, piCtx.Request, "Request should match")
		assert.NotNil(t, piCtx.Logger(), "Logger should not be nil")
	})
}

// Mock implementations for testing
type mockHelloServer struct{}

func (m *mockHelloServer) SayHello(ctx *pi.Context) (any, error) {
	req := &HelloRequest{}
	err := ctx.Bind(req)
	if err != nil {
		return nil, err
	}
	return &HelloResponse{Message: "Hello " + req.Name + "!"}, nil
}

type mockHelloServerWithError struct{}

func (m *mockHelloServerWithError) SayHello(*pi.Context) (any, error) {
	return nil, fmt.Errorf("test error")
}

type mockHelloServerWrongType struct{}

func (m *mockHelloServerWrongType) SayHello(*pi.Context) (any, error) {
	return "wrong type", nil
}
