package pi

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/config"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func TestRun_ServerStartsListening(t *testing.T) {
	port := testutil.GetFreePort(t)

	// Create a mock router and add a new route
	router := piHTTP.NewRouter()
	router.Add(http.MethodGet, "/", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// adding registered routes for applying middlewares
	var registeredMethods []string

	_ = router.Walk(func(method, route string) error {
		if !contains(registeredMethods, method) {
			registeredMethods = append(registeredMethods, method)
		}
		return nil
	})

	router.RegisteredRoutes = &registeredMethods

	// Create a mock container
	c := infra.NewContainer(getConfigs(t))

	// Create an instance of httpServer
	server := &httpServer{
		router: router,
		port:   port,
	}

	// Start the server
	require.NoError(t, server.run(c))
	t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })

	var netClient = &http.Client{
		Timeout: 200 * time.Millisecond,
	}

	// Send a GET request to the server
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet,
		fmt.Sprintf("http://localhost:%d", port), http.NoBody)
	resp, err := netClient.Do(req)

	require.NoError(t, err, "TEST Failed.\n")

	assert.Equal(t, http.StatusOK, resp.StatusCode, "TEST Failed.\n")

	resp.Body.Close()
}

func getConfigs(t *testing.T) config.Config {
	t.Helper()

	var configLocation string

	if _, err := os.Stat("./configs"); err == nil {
		configLocation = "./configs"
	}

	return config.NewEnvFile(configLocation, logging.NewLogger(logging.INFO))
}

func TestShutdown_ServerStopsListening(t *testing.T) {
	// Create a mock router and add a new route
	router := piHTTP.NewRouter()
	router.Add(http.MethodGet, "/", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Create a mock container
	c := &infra.Container{
		Logger: logging.NewLogger(logging.INFO),
	}

	// Create an instance of httpServer
	server := &httpServer{
		router: router,
		port:   testutil.GetFreePort(t),
	}

	// Startup is synchronous, as it is in App.Start. Sleeping after a goroutine
	// launch does not synchronize publication of server.srv with Shutdown.
	require.NoError(t, server.start(c, func(err error) { c.Logger.Error(err) }))

	// Create a context with a timeout to test the shutdown
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := server.Shutdown(ctx)

	require.NoError(t, err, "TEST Failed.\n")
}

func TestShutdown_ServerContextDeadline(t *testing.T) {
	// Create a mock router and add a new route
	router := piHTTP.NewRouter()
	router.Add(http.MethodGet, "/", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Create a mock container
	c := &infra.Container{
		Logger: logging.NewLogger(logging.INFO),
	}

	// Create an instance of httpServer
	server := &httpServer{
		router: router,
		port:   testutil.GetFreePort(t),
	}

	require.NoError(t, server.start(c, func(err error) { c.Logger.Error(err) }))

	// Create a context with a timeout to test the shutdown
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()

	err := server.Shutdown(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded, "Expected context deadline exceeded error")
}

func TestValidateCertificateAndKeyFiles_Success(t *testing.T) {
	certFile := createTempCertFile(t)
	defer os.Remove(certFile)

	keyFile := createTempKeyFile(t)
	defer os.Remove(keyFile)

	err := validateCertificateAndKeyFiles(certFile, keyFile)

	require.NoError(t, err, "TestValidateCertificateAndKeyFiles_Success Failed!")
}

func TestValidateCertificateAndKeyFiles_Error(t *testing.T) {
	tests := []struct {
		name          string
		certFilePath  string
		keyFilePath   string
		expectedError error
	}{
		{
			name:          "Certificate file does not exist",
			certFilePath:  "non-existent-cert.pem",
			keyFilePath:   createTempKeyFile(t),
			expectedError: fmt.Errorf("%w : %v", errInvalidCertificateFile, "non-existent-cert.pem"),
		},
		{
			name:          "Key file does not exist",
			certFilePath:  createTempCertFile(t),
			keyFilePath:   "non-existent-key.pem",
			expectedError: fmt.Errorf("%w : %v", errInvalidKeyFile, "non-existent-key.pem"),
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateCertificateAndKeyFiles(tc.certFilePath, tc.keyFilePath)

			require.Equal(t, tc.expectedError.Error(), err.Error(),
				"TestValidateCertificateAndKeyFiles_Error [%d] : %v Failed!", i, tc.name)
		})
	}
}

// Helper function to create a temporary key file.
func createTempKeyFile(t *testing.T) string {
	t.Helper()

	f, err := os.CreateTemp(t.TempDir(), "key-*.pem")
	if err != nil {
		t.Fatalf("could not create temp key file: %v", err)
	}

	t.Cleanup(func() {
		_ = f.Close()
	})

	return f.Name()
}

// Helper function to create a temporary certificate file.
func createTempCertFile(t *testing.T) string {
	t.Helper()

	f, err := os.CreateTemp(t.TempDir(), "cert-*.pem")
	if err != nil {
		t.Fatalf("could not create temp cert file: %v", err)
	}

	t.Cleanup(func() {
		_ = f.Close()
	})

	return f.Name()
}
