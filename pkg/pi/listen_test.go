package pi

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/stretchr/testify/require"
)

func isolatedApp(t *testing.T, overrides map[string]string) *App {
	t.Helper()
	t.Chdir(t.TempDir())
	require.NoError(t, os.Mkdir("configs", 0700))
	values := map[string]string{"HTTP_ADDR": "127.0.0.1:0", "HTTP_ENABLED": "true", "GRPC_ADDR": "127.0.0.1:0", "GRPC_ENABLED": "true", "METRICS_ADDR": "127.0.0.1:0", "METRICS_ENABLED": "true", "PI_TELEMETRY": "false", "KITE_TELEMETRY": "false", "LOG_LEVEL": "ERROR", "DB_DIALECT": "", "DB_HOST": "", "REDIS_HOST": "", "PUBSUB_BACKEND": "", "TRACE_EXPORTER": "", "TRACER_URL": "", "TRACER_HOST": "", "REMOTE_LOG_URL": "", "CERT_FILE": "", "KEY_FILE": "", "GRPC_CERT_FILE": "", "GRPC_KEY_FILE": "", "CORS_ALLOWED_ORIGINS": "", "ACCESS_CONTROL_ALLOW_ORIGIN": "", "ACCESS_CONTROL_ALLOW_CREDENTIALS": "", "CORS_ALLOW_CREDENTIALS": "false"}
	for k, v := range overrides {
		values[k] = v
	}
	for k, v := range values {
		t.Setenv(k, v)
	}
	a := New()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = a.Stop(ctx)
	})
	return a
}

func TestEndpointConfiguration(t *testing.T) {
	for _, tc := range []struct {
		cfg     map[string]string
		addr    string
		enabled bool
		bad     bool
	}{
		{map[string]string{"HTTP_HOST": "::1", "HTTP_PORT": "1234"}, "[::1]:1234", true, false},
		{map[string]string{"HTTP_ADDR": "127.0.0.1:0", "HTTP_PORT": "broken"}, "127.0.0.1:0", true, false},
		{map[string]string{"HTTP_PORT": "0"}, ":8000", true, false},
		{map[string]string{"HTTP_ENABLED": "false"}, "", false, false},
		{map[string]string{"HTTP_PORT": "invalid"}, "", false, true},
		{map[string]string{"HTTP_ADDR": "::1:80"}, "", false, true},
	} {
		a, e, err := endpointConfig(config.NewMockConfig(tc.cfg), "HTTP", 8000)
		require.Equal(t, tc.bad, err != nil)
		require.Equal(t, tc.addr, a)
		require.Equal(t, tc.enabled, e)
	}
}

func TestEndpointsRealListeners(t *testing.T) {
	a := isolatedApp(t, nil)
	a.GET("/ready", func(*Context) (any, error) { return "ready", nil })
	a.grpcRegistered = true
	require.NoError(t, a.Start(t.Context()))
	for _, addr := range []string{a.HTTPAddress(), a.GRPCAddress(), a.MetricsAddress()} {
		host, port, err := net.SplitHostPort(addr)
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1", host)
		require.NotEqual(t, "0", port)
		c, err := net.DialTimeout("tcp", addr, time.Second)
		require.NoError(t, err)
		require.NoError(t, c.Close())
	}
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	resp, err := client.Get("http://" + a.HTTPAddress() + "/ready")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	require.NotEmpty(t, resp.Header.Get("X-Request-ID"))
}

func TestEndpointsDisabled(t *testing.T) {
	a := isolatedApp(t, map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false", "METRICS_ENABLED": "false"})
	a.GET("/", func(*Context) (any, error) { return nil, nil })
	a.grpcRegistered = true
	require.NoError(t, a.Start(t.Context()))
	require.Empty(t, a.HTTPAddress())
	require.Empty(t, a.GRPCAddress())
	require.Empty(t, a.MetricsAddress())
}

func TestEndpointOccupiedReturnsError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	a := isolatedApp(t, map[string]string{"METRICS_ADDR": listener.Addr().String()})
	err = a.Start(t.Context())
	require.Error(t, err)
	var op *net.OpError
	require.ErrorAs(t, err, &op)
}

func TestEndpointIPv6(t *testing.T) {
	probe, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 unavailable: %v", err)
	}
	probe.Close()
	a := isolatedApp(t, map[string]string{"HTTP_ADDR": "[::1]:0", "METRICS_ENABLED": "false"})
	a.GET("/", func(*Context) (any, error) { return nil, nil })
	require.NoError(t, a.Start(t.Context()))
	host, _, err := net.SplitHostPort(a.HTTPAddress())
	require.NoError(t, err)
	require.Equal(t, "::1", host)
}

func testCertificate(t *testing.T) (string, string, *x509.CertPool) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, pub, key)
	require.NoError(t, err)
	private, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	dir := t.TempDir()
	crt, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	require.NoError(t, os.WriteFile(crt, certPEM, 0600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}), 0600))
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(certPEM))
	return crt, keyPath, pool
}

func TestEndpointTLS(t *testing.T) {
	crt, key, pool := testCertificate(t)
	a := isolatedApp(t, map[string]string{"CERT_FILE": crt, "KEY_FILE": key, "GRPC_CERT_FILE": crt, "GRPC_KEY_FILE": key})
	a.GET("/", func(*Context) (any, error) { return "tls", nil })
	a.grpcRegistered = true
	require.NoError(t, a.Start(t.Context()))
	for _, addr := range []string{a.HTTPAddress(), a.GRPCAddress()} {
		c, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", addr, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})
		require.NoError(t, err)
		require.NoError(t, c.Close())
	}
}

func TestEndpointInvalidTLSFailsStartup(t *testing.T) {
	for _, mode := range []string{"half", "invalid", "mismatch", "grpc"} {
		t.Run(mode, func(t *testing.T) {
			crt, key, _ := testCertificate(t)
			cfg := map[string]string{"CERT_FILE": crt, "KEY_FILE": key}
			switch mode {
			case "half":
				cfg["KEY_FILE"] = ""
			case "invalid":
				require.NoError(t, os.WriteFile(crt, []byte("invalid"), 0600))
			case "mismatch":
				_, cfg["KEY_FILE"], _ = testCertificate(t)
			case "grpc":
				cfg = map[string]string{"GRPC_CERT_FILE": crt}
			}
			a := isolatedApp(t, cfg)
			a.GET("/", func(*Context) (any, error) { return nil, nil })
			a.grpcRegistered = true
			require.Error(t, a.Start(t.Context()))
		})
	}
}
