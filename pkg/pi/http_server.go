package pi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/middleware"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/websocket"
)

type httpServer struct {
	listenerState
	router      *piHTTP.Router
	registry    *RouteRegistry
	port        int
	ws          *websocket.Manager
	srv         *http.Server
	certFile    string
	keyFile     string
	staticFiles map[string]string
}

var (
	errInvalidCertificateFile = errors.New("invalid certificate file")
	errInvalidKeyFile         = errors.New("invalid key file")
)

func newHTTPServer(c *infra.Container, port int, middlewareConfigs middleware.Config) *httpServer {
	r := piHTTP.NewRouter()
	wsManager := websocket.New()

	r.Use(
		middleware.Tracer,
		middleware.Logging(middlewareConfigs.LogProbes, c.Logger),
		middleware.CORS(middlewareConfigs.CorsHeaders, r.RegisteredRoutes),
		middleware.Metrics(c.Metrics()),
		middleware.WSHandlerUpgrade(c, wsManager),
	)

	return &httpServer{
		router:      r,
		registry:    newRouteRegistry(),
		port:        port,
		ws:          wsManager,
		staticFiles: make(map[string]string),
	}
}

func (s *httpServer) run(c *infra.Container) error {
	err := s.start(c, func(err error) {
		c.Errorf("error while listening to http server, err: %v", err)
	})
	if err != nil {
		c.Errorf("error while starting http server, err: %v", err)
	}

	return err
}

func (s *httpServer) start(c *infra.Container, onError func(error)) error {
	if s.srv != nil {
		c.Logf("Server already running on port: %d", s.port)
		return nil
	}

	c.Logf("Starting server on port: %d", s.port)

	tlsConfig, err := serverTLS(s.certFile, s.keyFile)
	if err != nil {
		return err
	}
	addr := s.listenAddress(s.port)
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           s.router,
		ReadHeaderTimeout: 5 * time.Second,
		TLSConfig:         tlsConfig,
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on HTTP address %s: %w", addr, err)
	}

	s.boundTo(listener.Addr())
	c.Infof("HTTP server listening on %s", listener.Addr())
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

	return ShutdownWithContext(ctx, func(ctx context.Context) error {
		return s.srv.Shutdown(ctx)
	}, func() error {
		if err := s.srv.Close(); err != nil {
			return err
		}

		return nil
	})
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
