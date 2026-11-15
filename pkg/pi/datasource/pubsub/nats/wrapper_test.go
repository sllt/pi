package nats

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNATSConnWrapper(t *testing.T) {
	// Start an embedded NATS server
	opts := &server.Options{
		Host: "127.0.0.1",
		Port: -1, // Random available port
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("Error starting NATS server: %v", err)
	}

	go ns.Start()
	defer ns.Shutdown()

	if !ns.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS server not ready for connections")
	}

	// Get the server's listen address
	addr := ns.Addr().(*net.TCPAddr)
	url := fmt.Sprintf("nats://%s:%d", addr.IP.String(), addr.Port)

	t.Run("Status", func(t *testing.T) {
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()

		wrapper := &natsConnWrapper{conn: nc}
		status := wrapper.Status()
		expectedStatus := nats.CONNECTED

		if status != expectedStatus {
			t.Errorf("Expected status %v, got %v", expectedStatus, status)
		}
	})

	t.Run("Close", func(t *testing.T) {
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatal(err)
		}

		wrapper := &natsConnWrapper{conn: nc}
		wrapper.Close()

		status := wrapper.Status()
		expectedStatus := nats.CLOSED

		if status != expectedStatus {
			t.Errorf("Expected status %v, got %v", expectedStatus, status)
		}
	})

	t.Run("NATSConn", func(t *testing.T) {
		nc, err := nats.Connect(url)
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()

		wrapper := &natsConnWrapper{conn: nc}
		returnedConn := wrapper.NATSConn()

		if returnedConn != nc {
			t.Errorf("Expected NATSConn to return the original connection")
		}
	})
}

func TestPubSubWrapper_ConnectContext_Shutdown_FromEnv(t *testing.T) {
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	ns, err := server.NewServer(opts)
	require.NoError(t, err)
	go ns.Start()
	defer ns.Shutdown()
	require.True(t, ns.ReadyForConnections(10*time.Second))

	addr := ns.Addr().(*net.TCPAddr)
	url := fmt.Sprintf("nats://%s:%d", addr.IP.String(), addr.Port)

	env := map[string]string{
		"NATS_SERVER":   url,
		"NATS_STREAM":   "orders",
		"NATS_SUBJECTS": "orders.created, orders.updated",
		"NATS_CONSUMER": "worker",
	}
	w, err := FromEnv(func(k string) string { return env[k] })
	require.NoError(t, err)
	assert.Equal(t, datasource.StatusDown, w.Health().Status)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, w.ConnectContext(ctx))
	// Idempotent second ConnectContext.
	require.NoError(t, w.ConnectContext(ctx))
	assert.Equal(t, datasource.StatusUp, w.Health().Status)

	require.NoError(t, w.Shutdown(ctx))
}
