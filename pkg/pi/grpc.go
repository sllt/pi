package pi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/sllt/pi/pkg/pi/config"
	pi_grpc "github.com/sllt/pi/pkg/pi/grpc"
)

type pendingService struct {
	desc *grpc.ServiceDesc
	impl any
}

type grpcServer struct {
	listenerState
	mu                 sync.Mutex
	server             *grpc.Server
	interceptors       []grpc.UnaryServerInterceptor
	streamInterceptors []grpc.StreamServerInterceptor
	options            []grpc.ServerOption
	port               int
	config             config.Config
	serverCreated      bool
	pendingServices    []pendingService
	serviceNames       []string
	health             *health.Server
}

var (
	errInvalidPort        = errors.New("invalid port number")
	errFailedCreateServer = errors.New("failed to create gRPC server")
)

// AddGRPCServerOptions allows users to add custom gRPC server options such as TLS configuration,
// timeouts, interceptors, and other server-specific settings in a single call.
func (a *App) AddGRPCServerOptions(grpcOpts ...grpc.ServerOption) {
	if a == nil || a.grpcServer == nil {
		return
	}
	if len(grpcOpts) == 0 {
		a.Logger().Debug("no gRPC server options provided")
		return
	}

	if a.grpcServer.serverCreated {
		a.Logger().Error("cannot add server options after gRPC server has been created - call this before RegisterService or Run")
		return
	}

	a.Logger().Debugf("adding %d gRPC server options", len(grpcOpts))
	a.grpcServer.options = append(a.grpcServer.options, grpcOpts...)
}

// AddGRPCUnaryInterceptors allows users to add custom gRPC interceptors.
func (a *App) AddGRPCUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) {
	if a == nil || a.grpcServer == nil {
		return
	}
	if len(interceptors) == 0 {
		a.Logger().Debug("no unary interceptors provided")
		return
	}

	if a.grpcServer.serverCreated {
		a.Logger().Error("cannot add interceptors after gRPC server has been created - call this before RegisterService or Run")
		return
	}

	a.Logger().Debugf("adding %d valid unary interceptors", len(interceptors))
	a.grpcServer.interceptors = append(a.grpcServer.interceptors, interceptors...)
}

func (a *App) AddGRPCServerStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) {
	if a == nil || a.grpcServer == nil {
		return
	}
	if len(interceptors) == 0 {
		a.Logger().Debug("no stream interceptors provided")
		return
	}

	if a.grpcServer.serverCreated {
		a.Logger().Error("cannot add stream interceptors after gRPC server has been created - call this before RegisterService or Run")
		return
	}

	a.Logger().Debugf("adding %d stream interceptors", len(interceptors))
	a.grpcServer.streamInterceptors = append(a.grpcServer.streamInterceptors, interceptors...)
}

func newGRPCServer(rt *runtimeKernel, port int, cfg config.Config) (*grpcServer, error) {
	if port <= 0 || port > 65535 {
		return nil, fmt.Errorf("%w: %d", errInvalidPort, port)
	}

	registerGRPCMetrics(rt)

	logger := rt.getLogger()
	m := rt.getMetrics()

	middleware := make([]grpc.UnaryServerInterceptor, 0)
	middleware = append(middleware,
		grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandlerContext(func(_ context.Context, p any) error {
			logger.Errorf("gRPC handler panic: %v", p)
			return status.Error(codes.Internal, "internal server error")
		})),
		pi_grpc.ObservabilityInterceptorWithProvider(logger, m, rt.getTracerProvider))

	streamMiddleware := make([]grpc.StreamServerInterceptor, 0)
	streamMiddleware = append(streamMiddleware,
		grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandlerContext(func(_ context.Context, p any) error {
			logger.Errorf("gRPC stream panic: %v", p)
			return status.Error(codes.Internal, "internal server error")
		})),
		pi_grpc.StreamObservabilityInterceptorWithProvider(logger, m, rt.getTracerProvider))

	return &grpcServer{
		port:               port,
		interceptors:       middleware,
		streamInterceptors: streamMiddleware,
		config:             cfg,
	}, nil
}

// registerGRPCMetrics registers essential gRPC metrics.
func registerGRPCMetrics(rt *runtimeKernel) {
	m := rt.getMetrics()
	if m == nil {
		return
	}
	m.NewGauge("grpc_server_status", "gRPC server status (1=running, 0=stopped)")
	m.NewCounter("grpc_server_errors_total", "Total gRPC server errors")
	m.NewCounter("grpc_services_registered_total", "Total gRPC services registered")
}

func (g *grpcServer) createServer() error {
	if g.serverCreated {
		return nil
	}
	tlsConfig, err := serverTLS(g.config.Get("GRPC_CERT_FILE"), g.config.Get("GRPC_KEY_FILE"))
	if err != nil {
		return err
	}
	if tlsConfig != nil {
		g.options = append(g.options, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	interceptorOption := grpc.ChainUnaryInterceptor(g.interceptors...)
	streamOpt := grpc.ChainStreamInterceptor(g.streamInterceptors...)
	g.options = append(g.options, interceptorOption, streamOpt)

	g.server = grpc.NewServer(g.options...)
	if g.server == nil {
		return errFailedCreateServer
	}

	enabled := strings.ToLower(g.config.GetOrDefault("GRPC_ENABLE_REFLECTION", "false"))
	if enabled == "true" { //nolint:goconst // standard boolean string
		reflection.Register(g.server)
	}

	g.serverCreated = true

	return nil
}

func (g *grpcServer) Run(rt *runtimeKernel) error {
	logger := rt.getLogger()
	m := rt.getMetrics()
	err := g.start(rt, func(err error) {
		logger.Errorf("error in gRPC server: %v", err)
		if m != nil {
			m.IncrementCounter(context.Background(), "grpc_server_errors_total")
			m.SetGauge("grpc_server_status", 0)
		}
	})
	if err != nil {
		logger.Errorf("error in starting gRPC server: %v", err)
	}

	return err
}

func (g *grpcServer) start(rt *runtimeKernel, onError func(error)) error {
	logger := rt.getLogger()
	m := rt.getMetrics()

	if g.server == nil {
		if err := g.createServer(); err != nil {
			if m != nil {
				m.IncrementCounter(context.Background(), "grpc_server_errors_total")
			}

			return fmt.Errorf("failed to create gRPC server: %w", err)
		}

		// Register all pending services after server creation
		for _, pending := range g.pendingServices {
			logger.Infof("registering pending gRPC Service: %s", pending.desc.ServiceName)
			g.server.RegisterService(pending.desc, pending.impl)
			if m != nil {
				m.IncrementCounter(context.Background(), "grpc_services_registered_total")
			}
			logger.Infof("successfully registered gRPC service: %s", pending.desc.ServiceName)
		}
		g.pendingServices = nil
	}

	addr := g.listenAddress(g.port)

	logger.Infof("starting gRPC server at %s", addr)

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		if m != nil {
			m.IncrementCounter(context.Background(), "grpc_server_errors_total")
			m.SetGauge("grpc_server_status", 0)
		}

		return fmt.Errorf("error in starting gRPC server at %s: %w", addr, err)
	}
	g.boundTo(listener.Addr())

	if m != nil {
		m.SetGauge("grpc_server_status", 1)
	}
	logger.Infof("gRPC server started successfully on %s", listener.Addr())

	go func() {
		if err := g.server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			onError(err)
			return
		}

		logger.Infof("gRPC server stopped on %s", addr)
		if m != nil {
			m.SetGauge("grpc_server_status", 0)
		}
	}()

	return nil
}

func (g *grpcServer) Shutdown(ctx context.Context) error {
	if g == nil || (g.server == nil && g.health == nil) {
		return nil
	}
	if g.health != nil {
		g.health.Shutdown()
	}
	return ShutdownWithContext(ctx, func(_ context.Context) error {
		if g.server != nil {
			g.server.GracefulStop()
		}

		return nil
	}, func() error {
		if g.server != nil {
			g.server.Stop()
		}

		return nil
	})
}

// RegisterService adds a gRPC service to the Pi application.
func (a *App) RegisterService(desc *grpc.ServiceDesc, impl any) {
	if a == nil || a.grpcServer == nil || desc == nil {
		return
	}
	a.Logger().Infof("queuing gRPC Service for registration: %s", desc.ServiceName)

	a.grpcServer.mu.Lock()
	a.grpcServer.pendingServices = append(a.grpcServer.pendingServices, pendingService{
		desc: desc,
		impl: impl,
	})
	if desc.ServiceName != "" && desc.ServiceName != healthpb.Health_ServiceDesc.ServiceName {
		if !contains(a.grpcServer.serviceNames, desc.ServiceName) {
			a.grpcServer.serviceNames = append(a.grpcServer.serviceNames, desc.ServiceName)
		}
	}
	a.grpcServer.mu.Unlock()

	a.grpcRegistered = true
	a.syncGRPCHealth()
	a.Logger().Infof("gRPC service %s queued for registration", desc.ServiceName)
}

// GRPCHealthServer registers one health service per application. Call during
// composition, before Start, as with other service registration methods.
func (a *App) GRPCHealthServer() *health.Server {
	if a == nil || a.grpcServer == nil {
		return nil
	}
	a.grpcServer.mu.Lock()
	if a.grpcServer.health != nil {
		h := a.grpcServer.health
		a.grpcServer.mu.Unlock()
		return h
	}
	if a.grpcServer.serverCreated {
		a.grpcServer.mu.Unlock()
		panic("register gRPC health before Start")
	}
	h := health.NewServer()
	a.grpcServer.health = h
	a.grpcServer.mu.Unlock()

	a.grpcHealthEnabled.Store(true)
	a.RegisterService(&healthpb.Health_ServiceDesc, h)
	a.syncGRPCHealth()
	a.runtimeMu.Lock()
	active := a.runtimeState == lifecycleStarting || a.runtimeState == lifecycleRunning
	a.runtimeMu.Unlock()
	if active {
		a.startGRPCHealthSyncWorker()
	}
	buckets := []float64{0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10}
	if m := a.Metrics(); m != nil {
		m.NewHistogram("app_gRPC-Server_stats", "gRPC server response duration", buckets...)
		m.NewHistogram("app_gRPC-Stream_stats", "gRPC stream duration", buckets...)
	}
	return h
}

func (a *App) ensureDefaultGRPCHealth() {
	if a == nil || a.grpcDisabled || !a.grpcRegistered || a.grpcServer == nil {
		return
	}
	a.grpcServer.mu.Lock()
	if a.grpcServer.health != nil || a.grpcServer.serverCreated {
		a.grpcServer.mu.Unlock()
		return
	}
	for _, pending := range a.grpcServer.pendingServices {
		if pending.desc != nil && pending.desc.ServiceName == healthpb.Health_ServiceDesc.ServiceName {
			a.grpcServer.mu.Unlock()
			return
		}
	}
	h := health.NewServer()
	a.grpcServer.health = h
	a.grpcServer.pendingServices = append(a.grpcServer.pendingServices, pendingService{
		desc: &healthpb.Health_ServiceDesc,
		impl: h,
	})
	a.grpcServer.mu.Unlock()

	a.grpcHealthEnabled.Store(true)
}

func (g *grpcServer) addServerOptions(opts ...grpc.ServerOption) {
	g.options = append(g.options, opts...)
}

func (g *grpcServer) addUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) {
	g.interceptors = append(g.interceptors, interceptors...)
}

func (g *grpcServer) addStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) {
	g.streamInterceptors = append(g.streamInterceptors, interceptors...)
}
