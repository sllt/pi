package pi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"

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
	"github.com/sllt/pi/pkg/pi/infra"
)

type pendingService struct {
	desc *grpc.ServiceDesc
	impl any
}

type grpcServer struct {
	listenerState
	server             *grpc.Server
	interceptors       []grpc.UnaryServerInterceptor
	streamInterceptors []grpc.StreamServerInterceptor
	options            []grpc.ServerOption
	port               int
	config             config.Config
	serverCreated      bool
	pendingServices    []pendingService
	health             *health.Server
}

var (
	errNonAddressable     = errors.New("cannot inject container as it is not addressable or is nil")
	errInvalidPort        = errors.New("invalid port number")
	errFailedCreateServer = errors.New("failed to create gRPC server")
)

// AddGRPCServerOptions allows users to add custom gRPC server options such as TLS configuration,
// timeouts, interceptors, and other server-specific settings in a single call.
//
// Example:
//
//	// Add TLS credentials and connection timeout in one call
//	creds, _ := credentials.NewServerTLSFromFile("server-cert.pem", "server-key.pem")
//	app.AddGRPCServerOptions(
//		grpc.Creds(creds),
//		grpc.ConnectionTimeout(10 * time.Second),
//	)
//
// This function accepts a variadic list of gRPC server options (grpc.ServerOption) and appends them
// to the server's configuration. It allows fine-tuning of the gRPC server's behavior during its initialization.
func (a *App) AddGRPCServerOptions(grpcOpts ...grpc.ServerOption) {
	if len(grpcOpts) == 0 {
		a.container.Logger.Debug("no gRPC server options provided")
		return
	}

	if a.grpcServer.serverCreated {
		a.container.Logger.Error("cannot add server options after gRPC server has been created - call this before RegisterService or Run")
		return
	}

	a.container.Logger.Debugf("adding %d gRPC server options", len(grpcOpts))
	a.grpcServer.options = append(a.grpcServer.options, grpcOpts...)
}

// AddGRPCUnaryInterceptors allows users to add custom gRPC interceptors.
// Example:
//
//	func loggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
//	handler grpc.UnaryHandler) (interface{}, error) {
//		log.Printf("Received gRPC request: %s", info.FullMethod)
//		return handler(ctx, req)
//	}
//	app.AddGRPCUnaryInterceptors(loggingInterceptor)
func (a *App) AddGRPCUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) {
	if len(interceptors) == 0 {
		a.container.Logger.Debug("no unary interceptors provided")
		return
	}

	if a.grpcServer.serverCreated {
		a.container.Logger.Error("cannot add interceptors after gRPC server has been created - call this before RegisterService or Run")
		return
	}

	a.container.Logger.Debugf("adding %d valid unary interceptors", len(interceptors))
	a.grpcServer.interceptors = append(a.grpcServer.interceptors, interceptors...)
}

func (a *App) AddGRPCServerStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) {
	if len(interceptors) == 0 {
		a.container.Logger.Debug("no stream interceptors provided")
		return
	}

	if a.grpcServer.serverCreated {
		a.container.Logger.Error("cannot add stream interceptors after gRPC server has been created - call this before RegisterService or Run")
		return
	}

	a.container.Logger.Debugf("adding %d stream interceptors", len(interceptors))
	a.grpcServer.streamInterceptors = append(a.grpcServer.streamInterceptors, interceptors...)
}

func newGRPCServer(c *infra.Container, port int, cfg config.Config) (*grpcServer, error) {
	if port <= 0 || port > 65535 {
		return nil, fmt.Errorf("%w: %d", errInvalidPort, port)
	}

	registerGRPCMetrics(c)

	middleware := make([]grpc.UnaryServerInterceptor, 0)
	middleware = append(middleware,
		grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandlerContext(func(_ context.Context, p any) error {
			c.Errorf("gRPC handler panic: %v", p)
			return status.Error(codes.Internal, "internal server error")
		})),
		pi_grpc.ObservabilityInterceptor(c.Logger, c.Metrics()))

	streamMiddleware := make([]grpc.StreamServerInterceptor, 0)
	streamMiddleware = append(streamMiddleware,
		grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandlerContext(func(_ context.Context, p any) error {
			c.Errorf("gRPC stream panic: %v", p)
			return status.Error(codes.Internal, "internal server error")
		})),
		pi_grpc.StreamObservabilityInterceptor(c.Logger, c.Metrics()))

	return &grpcServer{
		port:               port,
		interceptors:       middleware,
		streamInterceptors: streamMiddleware,
		config:             cfg,
	}, nil
}

// registerGRPCMetrics registers essential gRPC metrics.
func registerGRPCMetrics(c *infra.Container) {
	c.Metrics().NewGauge("grpc_server_status", "gRPC server status (1=running, 0=stopped)")
	c.Metrics().NewCounter("grpc_server_errors_total", "Total gRPC server errors")
	c.Metrics().NewCounter("grpc_services_registered_total", "Total gRPC services registered")
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

func (g *grpcServer) Run(c *infra.Container) error {
	err := g.start(c, func(err error) {
		c.Logger.Errorf("error in gRPC server: %v", err)
		c.Metrics().IncrementCounter(context.Background(), "grpc_server_errors_total")
		c.Metrics().SetGauge("grpc_server_status", 0)
	})
	if err != nil {
		c.Logger.Errorf("error in starting gRPC server: %v", err)
	}

	return err
}

func (g *grpcServer) start(c *infra.Container, onError func(error)) error {
	if g.server == nil {
		if err := g.createServer(); err != nil {
			c.Metrics().IncrementCounter(context.Background(), "grpc_server_errors_total")

			return fmt.Errorf("failed to create gRPC server: %w", err)
		}

		// Register all pending services after server creation
		for _, pending := range g.pendingServices {
			c.Logger.Infof("registering pending gRPC Service: %s", pending.desc.ServiceName)
			g.server.RegisterService(pending.desc, pending.impl)

			err := injectContainer(pending.impl, c)
			if err != nil {
				c.Metrics().IncrementCounter(context.Background(), "grpc_server_errors_total")
				return fmt.Errorf("failed to inject container into gRPC service %s: %w", pending.desc.ServiceName, err)
			}

			c.Metrics().IncrementCounter(context.Background(), "grpc_services_registered_total")
			c.Logger.Infof("successfully registered gRPC service: %s", pending.desc.ServiceName)
		}
		g.pendingServices = nil
	}

	addr := g.listenAddress(g.port)

	c.Logger.Infof("starting gRPC server at %s", addr)

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
	if err != nil {
		c.Metrics().IncrementCounter(context.Background(), "grpc_server_errors_total")
		c.Metrics().SetGauge("grpc_server_status", 0)

		return fmt.Errorf("error in starting gRPC server at %s: %w", addr, err)
	}
	g.boundTo(listener.Addr())

	c.Metrics().SetGauge("grpc_server_status", 1)
	c.Logger.Infof("gRPC server started successfully on %s", listener.Addr())

	go func() {
		if err := g.server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			onError(err)
			return
		}

		c.Logger.Infof("gRPC server stopped on %s", addr)
		c.Metrics().SetGauge("grpc_server_status", 0)
	}()

	return nil
}

func (g *grpcServer) Shutdown(ctx context.Context) error {
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
	a.container.Logger.Infof("queuing gRPC Service for registration: %s", desc.ServiceName)

	a.grpcServer.pendingServices = append(a.grpcServer.pendingServices, pendingService{
		desc: desc,
		impl: impl,
	})

	a.grpcRegistered = true
	a.container.Logger.Infof("gRPC service %s queued for registration", desc.ServiceName)
}

// GRPCHealthServer registers one health service per application. Call during
// composition, before Start, as with other service registration methods.
func (a *App) GRPCHealthServer() *health.Server {
	if a.grpcServer.health != nil {
		return a.grpcServer.health
	}
	if a.grpcServer.serverCreated {
		panic("register gRPC health before Start")
	}
	h := health.NewServer()
	a.grpcServer.health = h
	a.RegisterService(&healthpb.Health_ServiceDesc, h)
	h.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	buckets := []float64{0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10}
	a.Metrics().NewHistogram("app_gRPC-Server_stats", "gRPC server response duration", buckets...)
	a.Metrics().NewHistogram("app_gRPC-Stream_stats", "gRPC stream duration", buckets...)
	return h
}

func injectContainer(impl any, c *infra.Container) error {
	val := reflect.ValueOf(impl)

	// Note: returning nil for the cases where user does not want to inject the container altogether and
	// not to break any existing implementation for the users that are using gRPC server. If users are
	// expecting the container to be injected and are passing non-addressable server struct, we have the
	// DEBUG log for the same.
	if val.Kind() != reflect.Pointer {
		c.Logger.Debugf("cannot inject container into non-addressable implementation of `%s`, consider using pointer",
			val.Type().Name())

		return nil
	}

	val = val.Elem()
	tVal := val.Type()

	for i := 0; i < val.NumField(); i++ {
		f := tVal.Field(i)
		v := val.Field(i)

		if f.Type == reflect.TypeOf(c) {
			if !v.CanSet() {
				c.Logger.Error(errNonAddressable)
				return errNonAddressable
			}

			v.Set(reflect.ValueOf(c))

			// early return expecting only one container field necessary for one gRPC implementation
			return nil
		}

		if f.Type == reflect.TypeOf(*c) {
			if !v.CanSet() {
				c.Logger.Error(errNonAddressable)
				return errNonAddressable
			}

			v.Set(reflect.ValueOf(*c))

			// early return expecting only one container field necessary for one gRPC implementation
			return nil
		}
	}

	return nil
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
