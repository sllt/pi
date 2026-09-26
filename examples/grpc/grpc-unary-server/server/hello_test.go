package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func TestPiHelloServer_Creation(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	t.Run("HelloPiServerCreation", func(t *testing.T) {
		// Test Pi's HelloPiServer creation
		app := pi.New()
		helloServer := &HelloPiServer{}

		assert.NotNil(t, helloServer, "Pi hello server should not be nil")
		assert.NotNil(t, app, "Pi app should not be nil")

		// Test that it implements the Pi interface
		var _ HelloServerWithPi = helloServer
	})

	t.Run("HelloServerWrapperCreation", func(t *testing.T) {
		// Test Pi's HelloServerWrapper creation
		app := pi.New()
		helloServer := &HelloPiServer{}
		wrapper := &HelloServerWrapper{
			server: helloServer,
		}

		assert.NotNil(t, wrapper, "Pi hello server wrapper should not be nil")
		assert.Equal(t, helloServer, wrapper.server, "Wrapper should contain the server")
		assert.NotNil(t, app, "Pi app should not be nil")
	})
}

func TestPiHelloServer_Methods(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	// Test Pi's hello server methods
	helloServer := &HelloPiServer{}
	ctx := createTestContext()

	t.Run("SayHelloMethodExists", func(t *testing.T) {
		// Test that Pi's SayHello method exists and accepts correct parameters
		// Create a mock request in the context using the wrapper
		ctx.Request = &HelloRequestWrapper{
			HelloRequest: &HelloRequest{
				Name: "test-name",
			},
		}

		// Test Pi's SayHello method signature
		resp, err := helloServer.SayHello(ctx)
		require.NoError(t, err, "Pi SayHello should not fail")
		assert.NotNil(t, resp, "SayHello response should not be nil")

		// Verify the response type
		helloResp, ok := resp.(*HelloResponse)
		assert.True(t, ok, "Response should be HelloResponse")
		assert.Contains(t, helloResp.Message, "test-name", "Response should contain the name")
	})

	t.Run("SayHelloWithEmptyName", func(t *testing.T) {
		// Test Pi's SayHello with empty name (should default to "World")
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
		// Test Pi's context binding functionality
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
		// Test that Pi's methods expect *pi.Context specifically
		ctx := createTestContext()
		ctx.Request = &HelloRequestWrapper{
			HelloRequest: &HelloRequest{
				Name: "type-test",
			},
		}

		// Verify the method signature expects *pi.Context
		var _ func(*pi.Context) (any, error) = helloServer.SayHello

		// Ensure the call compiles (even if it fails at runtime)
		_, _ = helloServer.SayHello(ctx)
	})
}

func TestPiHelloServer_Registration(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	// Test Pi's server registration functionality
	t.Run("RegisterHelloServerWithPi", func(t *testing.T) {
		// Test Pi's RegisterHelloServerWithPi function
		app := pi.New()
		helloServer := &HelloPiServer{}

		// This should not panic and should register the server
		assert.NotPanics(t, func() {
			RegisterHelloServerWithPi(app, helloServer)
		}, "RegisterHelloServerWithPi should not panic")
	})
}

func TestPiHelloServer_HealthIntegration(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	// Test Pi's health integration
	t.Run("HealthIntegration", func(t *testing.T) {
		app := pi.New()

		helloServer := &HelloPiServer{}

		// Register the server to set up health checks
		RegisterHelloServerWithPi(app, helloServer)

		// Test that health server is properly integrated
		healthServer := getOrCreateHealthServer()
		assert.NotNil(t, healthServer, "Health server should be available")

		// Create a context for the health check
		ctx := createTestContext()

		// Check that Hello service is registered as serving
		req := &healthpb.HealthCheckRequest{
			Service: "Hello",
		}
		resp, err := healthServer.Check(ctx, req)
		require.NoError(t, err, "Health check should not fail")
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status, "Hello service should be serving")
	})
}

func TestPiHelloServer_MultipleInstances(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	// Test Pi's multiple server instances
	t.Run("MultipleHelloServers", func(t *testing.T) {
		app := pi.New()

		server1 := &HelloPiServer{}
		server2 := &HelloPiServer{}

		assert.NotNil(t, server1, "First Pi hello server should not be nil")
		assert.NotNil(t, server2, "Second Pi hello server should not be nil")
		// Check that they are different objects (different memory addresses)
		assert.True(t, server1 != server2, "Pi hello server instances should be different objects")
		assert.NotNil(t, app, "Pi app should not be nil")

		// Test that both can be created (but not registered to avoid duplicate service error)
		assert.NotNil(t, server1, "First server should be valid")
		assert.NotNil(t, server2, "Second server should be valid")
	})
}

func TestNewHelloPiServer(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	t.Run("NewHelloPiServerCreation", func(t *testing.T) {
		// Test Pi's NewHelloPiServer function
		server := NewHelloPiServer()

		assert.NotNil(t, server, "NewHelloPiServer should not return nil")
		assert.NotNil(t, server.health, "Health server should be initialized")

		// Test that it implements the Pi interface
		var _ HelloServerWithPi = server
	})
}

func TestHelloServerWrapper_SayHello(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	t.Run("SayHelloWrapper", func(t *testing.T) {
		// Create a mock server implementation
		mockServer := &mockHelloServer{}

		// Create wrapper
		wrapper := &HelloServerWrapper{
			server:       mockServer,
			healthServer: getOrCreateHealthServer(),
			Container:    &infra.Container{},
		}

		// Test SayHello method
		ctx := context.Background()
		req := &HelloRequest{Name: "test"}

		resp, err := wrapper.SayHello(ctx, req)

		require.NoError(t, err, "SayHello should not fail")
		assert.NotNil(t, resp, "Response should not be nil")
		assert.Equal(t, "Hello test!", resp.Message, "Response message should match")
	})

	t.Run("SayHelloWithError", func(t *testing.T) {
		// Create a mock server that returns an error
		mockServer := &mockHelloServerWithError{}

		// Create wrapper
		wrapper := &HelloServerWrapper{
			server:       mockServer,
			healthServer: getOrCreateHealthServer(),
			Container:    &infra.Container{},
		}

		// Test SayHello method with error
		ctx := context.Background()
		req := &HelloRequest{Name: "error"}

		resp, err := wrapper.SayHello(ctx, req)

		assert.Error(t, err, "SayHello should return error")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "test error", "Error message should match")
	})

	t.Run("SayHelloWithWrongResponseType", func(t *testing.T) {
		// Create a mock server that returns wrong type
		mockServer := &mockHelloServerWrongType{}

		// Create wrapper
		wrapper := &HelloServerWrapper{
			server:       mockServer,
			healthServer: getOrCreateHealthServer(),
			Container:    &infra.Container{},
		}

		// Test SayHello method with wrong response type
		ctx := context.Background()
		req := &HelloRequest{Name: "wrong"}

		resp, err := wrapper.SayHello(ctx, req)

		assert.Error(t, err, "SayHello should return error for wrong response type")
		assert.Nil(t, resp, "Response should be nil on error")
		assert.Contains(t, err.Error(), "unexpected response type", "Error message should indicate wrong type")
	})
}

func TestHelloServerWrapper_getPiContext(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	t.Run("getPiContext", func(t *testing.T) {
		// Create wrapper
		wrapper := &HelloServerWrapper{
			Container: &infra.Container{},
		}

		// Test getPiContext method
		ctx := context.Background()
		req := &HelloRequestWrapper{
			HelloRequest: &HelloRequest{Name: "test"},
		}

		piCtx := wrapper.getPiContext(ctx, req)

		assert.NotNil(t, piCtx, "Pi context should not be nil")
		assert.Equal(t, ctx, piCtx.Context, "Context should match")
		assert.Equal(t, &infra.Container{}, piCtx.Container, "Container should match")
		assert.Equal(t, req, piCtx.Request, "Request should match")
	})
}

func TestInstrumentedStream(t *testing.T) {
	_ = testutil.NewServerConfigs(t)

	t.Run("InstrumentedStreamContext", func(t *testing.T) {
		// Create a mock server stream
		mockStream := &mockServerStream{}

		// Create instrumented stream
		piCtx := createTestContext()
		stream := &instrumentedStream{
			ServerStream: mockStream,
			ctx:          piCtx,
			method:       "/Hello/Test",
		}

		// Test Context method
		ctx := stream.Context()
		assert.Equal(t, piCtx, ctx, "Context should match Pi context")
	})

	t.Run("InstrumentedStreamSendMsg", func(t *testing.T) {
		// Create a mock server stream
		mockStream := &mockServerStream{}

		// Create instrumented stream
		piCtx := createTestContext()
		stream := &instrumentedStream{
			ServerStream: mockStream,
			ctx:          piCtx,
			method:       "/Hello/Test",
		}

		// Test SendMsg method
		msg := &HelloResponse{Message: "test"}
		err := stream.SendMsg(msg)

		assert.NoError(t, err, "SendMsg should not fail")
		assert.True(t, mockStream.sendMsgCalled, "SendMsg should be called on underlying stream")
	})

	t.Run("InstrumentedStreamRecvMsg", func(t *testing.T) {
		// Create a mock server stream
		mockStream := &mockServerStream{}

		// Create instrumented stream
		piCtx := createTestContext()
		stream := &instrumentedStream{
			ServerStream: mockStream,
			ctx:          piCtx,
			method:       "/Hello/Test",
		}

		// Test RecvMsg method
		msg := &HelloRequest{}
		err := stream.RecvMsg(msg)

		assert.NoError(t, err, "RecvMsg should not fail")
		assert.True(t, mockStream.recvMsgCalled, "RecvMsg should be called on underlying stream")
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

func (m *mockHelloServerWithError) SayHello(ctx *pi.Context) (any, error) {
	return nil, fmt.Errorf("test error")
}

type mockHelloServerWrongType struct{}

func (m *mockHelloServerWrongType) SayHello(ctx *pi.Context) (any, error) {
	return "wrong type", nil
}

type mockServerStream struct {
	sendMsgCalled bool
	recvMsgCalled bool
}

func (m *mockServerStream) SendMsg(msg interface{}) error {
	m.sendMsgCalled = true
	return nil
}

func (m *mockServerStream) RecvMsg(msg interface{}) error {
	m.recvMsgCalled = true
	return nil
}

func (m *mockServerStream) SetHeader(metadata.MD) error {
	return nil
}

func (m *mockServerStream) SendHeader(metadata.MD) error {
	return nil
}

func (m *mockServerStream) SetTrailer(metadata.MD) {
}

func (m *mockServerStream) Context() context.Context {
	return context.Background()
}
