package pi

import (
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/sllt/pi/pkg/pi/config"
)

// listenerState publishes the actual bound address, including an allocated port.
type listenerState struct {
	address string
	mu      sync.RWMutex
	bound   string
}

func (s *listenerState) boundAddress() string  { s.mu.RLock(); defer s.mu.RUnlock(); return s.bound }
func (s *listenerState) boundTo(addr net.Addr) { s.mu.Lock(); s.bound = addr.String(); s.mu.Unlock() }
func (s *listenerState) listenAddress(port int) string {
	if s.address != "" {
		return s.address
	}
	return net.JoinHostPort("", strconv.Itoa(port))
}

func endpointConfig(c config.Config, prefix string, defaultPort int) (address string, enabled bool, err error) {
	enabled = true
	if value := c.Get(prefix + "_ENABLED"); value != "" {
		enabled, err = strconv.ParseBool(value)
		if err != nil {
			return "", false, fmt.Errorf("%s_ENABLED: %w", prefix, err)
		}
	}
	if !enabled {
		return "", false, nil
	}
	address = c.Get(prefix + "_ADDR")
	if address != "" {
		_, port, splitErr := net.SplitHostPort(address)
		if splitErr != nil {
			return "", false, fmt.Errorf("%s_ADDR: %w", prefix, splitErr)
		}
		n, parseErr := strconv.Atoi(port)
		if parseErr != nil || n < 0 || n > 65535 {
			return "", false, fmt.Errorf("%s_ADDR: invalid numeric port", prefix)
		}
		return address, true, nil
	}
	value := c.Get(prefix + "_PORT")
	if prefix == "METRICS" && value == "0" && c.Get(prefix+"_ENABLED") == "" {
		return "", false, nil
	}
	port := defaultPort
	if value != "" {
		n, parseErr := strconv.Atoi(value)
		if parseErr != nil || n < 0 || n > 65535 {
			return "", false, fmt.Errorf("%s_PORT: invalid numeric port", prefix)
		}
		// Preserve legacy PORT=0 defaulting. ADDR=host:0 requests allocation.
		if n > 0 {
			port = n
		}
	}
	host := strings.Trim(c.Get(prefix+"_HOST"), "[]")
	return net.JoinHostPort(host, strconv.Itoa(port)), true, nil
}

func serverTLS(certFile, keyFile string) (*tls.Config, error) {
	if certFile == "" && keyFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("TLS requires both certificate and key")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS certificate and key: %w", err)
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}, nil
}

// HTTPAddress returns the actual bound HTTP address, or empty before listening.
func (a *App) HTTPAddress() string {
	if a.httpServer == nil {
		return ""
	}
	return a.httpServer.boundAddress()
}
func (a *App) GRPCAddress() string {
	if a.grpcServer == nil {
		return ""
	}
	return a.grpcServer.boundAddress()
}
func (a *App) MetricsAddress() string {
	if a.metricServer == nil {
		return ""
	}
	return a.metricServer.boundAddress()
}
