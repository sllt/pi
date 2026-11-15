package pi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/middleware"
	"github.com/sllt/pi/pkg/pi/websocket"
)

type idempotentListener struct {
	net.Listener
	closeOnce sync.Once
	closeErr  error
}

func (l *idempotentListener) Close() error {
	l.closeOnce.Do(func() {
		l.closeErr = l.Listener.Close()
	})
	return l.closeErr
}

type httpServer struct {
	listenerState
	router      *piHTTP.Router
	registry    *RouteRegistry
	port        int
	ws          *websocket.Manager
	srv         *http.Server
	listener    atomic.Pointer[idempotentListener]
	certFile    string
	keyFile     string
	staticFiles map[string]string
}

var (
	errInvalidCertificateFile = errors.New("invalid certificate file")
	errInvalidKeyFile         = errors.New("invalid key file")
)

func newHTTPServer(rt *runtimeKernel, port int, middlewareConfigs middleware.Config) *httpServer {
	r := piHTTP.NewRouter()
	var wsManager *websocket.Manager
	if rt != nil {
		r.SetTelemetry(rt.getTracerProvider, rt.isolated)
		wsManager = rt.wsManager
		if wsManager == nil {
			wsManager = websocket.New()
			rt.wsManager = wsManager
		}
	} else {
		wsManager = websocket.New()
	}

	r.Use(
		middleware.TracerWithProvider(rt.getTracerProvider),
		middleware.Logging(middlewareConfigs.LogProbes, rt.getLogger()),
		middleware.CORS(middlewareConfigs.CorsHeaders, r.RegisteredRoutes),
		middleware.Metrics(rt.getMetrics()),
		middleware.WSHandlerUpgrade(rt.getLogger(), wsManager),
	)

	return &httpServer{
		router:      r,
		registry:    newRouteRegistry(),
		port:        port,
		ws:          wsManager,
		staticFiles: make(map[string]string),
	}
}

func (s *httpServer) run(rt *runtimeKernel) error {
	err := s.start(rt, func(err error) {
		rt.getLogger().Errorf("error while listening to http server, err: %v", err)
	})
	if err != nil {
		rt.getLogger().Errorf("error while starting http server, err: %v", err)
	}

	return err
}

func (s *httpServer) start(rt *runtimeKernel, onError func(error)) error {
	logger := rt.getLogger()
	if s.srv != nil {
		logger.Logf("Server already running on %s", s.srv.Addr)
		return nil
	}

	tlsConfig, err := serverTLS(s.certFile, s.keyFile)
	if err != nil {
		return err
	}
	addr := s.listenAddress(s.port)
	logger.Debugf("Starting HTTP server on %s", addr)
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           s.router,
		ReadHeaderTimeout: 5 * time.Second,
		TLSConfig:         tlsConfig,
	}
	rawListener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on HTTP address %s: %w", addr, err)
	}
	listener := &idempotentListener{Listener: rawListener}
	s.listener.Store(listener)

	s.boundTo(listener.Addr())
	logger.Infof("HTTP server listening on %s", formatClickableBaseURL(listener.Addr().String(), tlsConfig != nil))
	if tlsConfig != nil {
		go func() {
			if err := s.srv.ServeTLS(listener, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
				onError(err)
			}
		}()

		return nil
	}

	go func() {
		if err := s.srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			onError(err)
		}
	}()

	return nil
}

func (s *httpServer) Shutdown(ctx context.Context) error {
	if s.srv == nil {
		return nil
	}

	err := ShutdownWithContext(ctx, func(ctx context.Context) error {
		return s.srv.Shutdown(ctx)
	}, func() error {
		if err := s.srv.Close(); err != nil {
			return err
		}

		return nil
	})
	if l := s.listener.Load(); l != nil {
		_ = l.Close()
	}
	return err
}

func validateCertificateAndKeyFiles(certificateFile, keyFile string) error {
	if _, err := os.Stat(certificateFile); os.IsNotExist(err) {
		return fmt.Errorf("%w : %v", errInvalidCertificateFile, certificateFile)
	}

	if _, err := os.Stat(keyFile); os.IsNotExist(err) {
		return fmt.Errorf("%w : %v", errInvalidKeyFile, keyFile)
	}

	return nil
}

func formatClickableBaseURL(addr string, tlsEnabled bool) string {
	scheme := "http"
	if tlsEnabled {
		scheme = "https"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return scheme + "://" + addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return scheme + "://" + net.JoinHostPort(host, port)
}
