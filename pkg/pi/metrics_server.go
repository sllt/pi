package pi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/metrics"
)

type metricServer struct {
	listenerState
	port int
	srv  *http.Server
}

func newMetricServer(port int) *metricServer {
	return &metricServer{port: port}
}

func (m *metricServer) Run(c *infra.Container) error {
	err := m.start(c, func(err error) {
		c.Errorf("error while listening to metrics server, err: %v", err)
	})
	if err != nil {
		c.Errorf("error while starting metrics server, err: %v", err)
	}

	return err
}

func (m *metricServer) start(c *infra.Container, onError func(error)) error {
	if m != nil {
		c.Logf("Starting metrics server on port: %d", m.port)

		addr := m.listenAddress(m.port)
		m.srv = &http.Server{
			Addr:              addr,
			Handler:           metrics.GetHandler(c.Metrics()),
			ReadHeaderTimeout: 5 * time.Second,
		}

		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("failed to listen on metrics address %s: %w", addr, err)
		}
		m.boundTo(listener.Addr())
		c.Infof("Metrics server listening on %s", listener.Addr())

		go func() {
			if err := m.srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				onError(err)
			}
		}()
	}

	return nil
}

func (m *metricServer) Shutdown(ctx context.Context) error {
	if m.srv == nil {
		return nil
	}

	return ShutdownWithContext(ctx, func(ctx context.Context) error {
		return m.srv.Shutdown(ctx)
	}, m.srv.Close)
}
