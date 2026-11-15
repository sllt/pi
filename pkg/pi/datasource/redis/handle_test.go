package redis

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/sllt/pi/pkg/pi/datasource"
)

type mapConfig map[string]string

func (m mapConfig) Get(key string) string { return m[key] }
func (m mapConfig) GetOrDefault(key, def string) string {
	if v, ok := m[key]; ok && v != "" {
		return v
	}
	return def
}

// startMiniRESPServer runs an in-process minimal RESP2 server sufficient for Ping, Get, Set,
// Echo/Do, Info, Watch, Multi/Exec, and Pipeline tests without external Redis dependencies.
func startMiniRESPServer(t *testing.T) (host string, port string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var (
		mu    sync.Mutex
		store = map[string]string{}
		conns []net.Conn
	)

	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
	})

	execSingle := func(args []string) string {
		if len(args) == 0 {
			return "-ERR empty command\r\n"
		}
		cmd := strings.ToUpper(args[0])
		switch cmd {
		case "PING":
			if len(args) > 1 {
				return fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
			}
			return "+PONG\r\n"
		case "HELLO":
			return "-ERR unknown command 'HELLO'\r\n"
		case "CLIENT", "SELECT", "WATCH", "UNWATCH":
			return "+OK\r\n"
		case "SET":
			if len(args) >= 3 {
				mu.Lock()
				store[args[1]] = args[2]
				mu.Unlock()
			}
			return "+OK\r\n"
		case "GET":
			if len(args) >= 2 {
				mu.Lock()
				val, ok := store[args[1]]
				mu.Unlock()
				if !ok {
					return "$-1\r\n"
				}
				return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
			}
			return "$-1\r\n"
		case "ECHO":
			if len(args) >= 2 {
				return fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
			}
			return "$0\r\n\r\n"
		case "INFO":
			payload := "# Stats\r\ntotal_connections_received:1\r\n"
			return fmt.Sprintf("$%d\r\n%s\r\n", len(payload), payload)
		default:
			return "+OK\r\n"
		}
	}

	go func() {
		for {
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()

			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				var inMulti bool
				var multiQueue [][]string

				for {
					args, readErr := readRESPCommand(r)
					if readErr != nil {
						return
					}
					if len(args) == 0 {
						continue
					}
					upper := strings.ToUpper(args[0])
					if upper == "QUIT" {
						_, _ = io.WriteString(c, "+OK\r\n")
						return
					}
					if upper == "MULTI" {
						inMulti = true
						multiQueue = nil
						_, _ = io.WriteString(c, "+OK\r\n")
						continue
					}
					if upper == "DISCARD" {
						inMulti = false
						multiQueue = nil
						_, _ = io.WriteString(c, "+OK\r\n")
						continue
					}
					if upper == "EXEC" {
						inMulti = false
						var sb strings.Builder
						sb.WriteString(fmt.Sprintf("*%d\r\n", len(multiQueue)))
						for _, q := range multiQueue {
							sb.WriteString(execSingle(q))
						}
						multiQueue = nil
						_, _ = io.WriteString(c, sb.String())
						continue
					}
					if inMulti {
						multiQueue = append(multiQueue, args)
						_, _ = io.WriteString(c, "+QUEUED\r\n")
						continue
					}
					_, _ = io.WriteString(c, execSingle(args))
				}
			}(conn)
		}
	}()

	addrHost, addrPort, err := net.SplitHostPort(ln.Addr().String())
	require.NoError(t, err)
	return addrHost, addrPort
}

func readRESPCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return nil, nil
	}
	if line[0] != '*' {
		return strings.Fields(line), nil
	}
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, count)
	for range count {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimRight(hdr, "\r\n")
		if len(hdr) == 0 || hdr[0] != '$' {
			return nil, fmt.Errorf("unexpected bulk header %q", hdr)
		}
		n, err := strconv.Atoi(hdr[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:n]))
	}
	return args, nil
}

func TestValidateConfig_PureAndStrict(t *testing.T) {
	// Missing host fails
	require.Error(t, ValidateConfig(mapConfig{}))
	// Bad port fails
	require.Error(t, ValidateConfig(mapConfig{"REDIS_HOST": "127.0.0.1", "REDIS_PORT": "70000"}))
	// Negative DB fails
	require.Error(t, ValidateConfig(mapConfig{"REDIS_HOST": "127.0.0.1", "REDIS_DB": "-1"}))
	// TLS cert XOR key fails
	require.Error(t, ValidateConfig(mapConfig{"REDIS_HOST": "127.0.0.1", "REDIS_TLS_CERT": "/missing/cert.pem"}))
	// Non-existent TLS files MUST succeed during pure ValidateConfig (zero disk I/O!)
	require.NoError(t, ValidateConfig(mapConfig{
		"REDIS_HOST":        "127.0.0.1",
		"REDIS_TLS_ENABLED": "true",
		"REDIS_TLS_CA_CERT": "/definitely/does/not/exist/ca.pem",
		"REDIS_TLS_CERT":    "/definitely/does/not/exist/client.pem",
		"REDIS_TLS_KEY":     "/definitely/does/not/exist/client.key",
	}))
	// OpenContextWithTracerProvider fails strictly when TLS file is unreadable
	_, err := OpenContextWithTracerProvider(context.Background(), mapConfig{
		"REDIS_HOST":        "127.0.0.1",
		"REDIS_TLS_ENABLED": "true",
		"REDIS_TLS_CA_CERT": "/definitely/does/not/exist/ca.pem",
	}, nil, nil, noop.NewTracerProvider())
	require.Error(t, err)

	// PubSub validation
	require.Error(t, ValidatePubSubConfig(mapConfig{
		"REDIS_HOST":        "127.0.0.1",
		"REDIS_PUBSUB_MODE": "invalid-mode",
	}))
	require.Error(t, ValidatePubSubConfig(mapConfig{
		"REDIS_HOST":              "127.0.0.1",
		"REDIS_STREAMS_PEL_RATIO": "1.5",
	}))
	require.NoError(t, ValidatePubSubConfig(mapConfig{
		"REDIS_HOST":              "127.0.0.1",
		"REDIS_PUBSUB_MODE":       "streams",
		"REDIS_STREAMS_PEL_RATIO": "0.5",
	}))
}

func TestHandle_LifecycleAndPipelineTraps(t *testing.T) {
	host, port := startMiniRESPServer(t)
	cfg := mapConfig{
		"REDIS_HOST": host,
		"REDIS_PORT": port,
	}

	h := NewHandleWithTracerProvider(cfg, nil, nil, noop.NewTracerProvider())
	ctx := context.Background()

	// 1. Unstarted state
	assert.ErrorIs(t, h.Get(ctx, "k").Err(), ErrNotStarted)
	assert.ErrorIs(t, h.Do(ctx, "ECHO", "hi").Err(), ErrNotStarted)
	assert.ErrorIs(t, h.Watch(ctx, func(tx *redis.Tx) error { return nil }, "k"), ErrNotStarted)

	// Empty Pipeline.Exec MUST return ErrNotStarted (not nil!)
	cmds, err := h.Pipeline().Exec(ctx)
	assert.Nil(t, cmds)
	assert.ErrorIs(t, err, ErrNotStarted)

	// Non-empty Pipeline.Exec sets error on queued commands AND returns ErrNotStarted
	pipe := h.Pipeline()
	queuedGet := pipe.Get(ctx, "k")
	cmds, err = pipe.Exec(ctx)
	assert.Len(t, cmds, 1)
	assert.ErrorIs(t, err, ErrNotStarted)
	assert.ErrorIs(t, queuedGet.Err(), ErrNotStarted)

	// Pipelined & TxPipelined populate caller's Cmder variable BEFORE returning ErrNotStarted
	var cbGet *redis.StringCmd
	_, err = h.Pipelined(ctx, func(p redis.Pipeliner) error {
		cbGet = p.Get(ctx, "k")
		return nil
	})
	require.NotNil(t, cbGet)
	assert.ErrorIs(t, err, ErrNotStarted)
	assert.ErrorIs(t, cbGet.Err(), ErrNotStarted)

	var txGet *redis.StringCmd
	_, err = h.TxPipelined(ctx, func(p redis.Pipeliner) error {
		txGet = p.Get(ctx, "k")
		return nil
	})
	require.NotNil(t, txGet)
	assert.ErrorIs(t, err, ErrNotStarted)
	assert.ErrorIs(t, txGet.Err(), ErrNotStarted)

	hc := h.HealthCheckContext(ctx)
	assert.Equal(t, datasource.StatusDown, hc.Status)

	// 2. Start & active operations
	require.NoError(t, h.Start(ctx))
	require.NoError(t, h.Start(ctx)) // idempotent

	require.NoError(t, h.Set(ctx, "greeting", "hello-pi", 0).Err())
	val, err := h.Get(ctx, "greeting").Result()
	require.NoError(t, err)
	assert.Equal(t, "hello-pi", val)

	echoVal, err := h.Do(ctx, "ECHO", "ping-do").Text()
	require.NoError(t, err)
	assert.Equal(t, "ping-do", echoVal)

	var pipeGet *redis.StringCmd
	_, err = h.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "p-key", "p-val", 0)
		pipeGet = p.Get(ctx, "p-key")
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, "p-val", pipeGet.Val())

	err = h.Watch(ctx, func(tx *redis.Tx) error {
		_, txErr := tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, "watch-key", "watch-val", 0)
			return nil
		})
		return txErr
	}, "watch-key")
	require.NoError(t, err)
	assert.Equal(t, "watch-val", h.Get(ctx, "watch-key").Val())

	hc = h.HealthCheckContext(ctx)
	assert.Equal(t, datasource.StatusUp, hc.Status)

	// 3. Concurrent access + Close under -race
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 25 {
				_ = h.Get(ctx, "greeting").Err()
				_ = h.HealthCheckContext(ctx)
			}
		}()
	}
	wg.Wait()

	require.NoError(t, h.Close())
	require.NoError(t, h.Close()) // idempotent

	// 4. Closed state
	assert.ErrorIs(t, h.Get(ctx, "greeting").Err(), ErrClosed)
	assert.ErrorIs(t, h.Do(ctx, "ECHO", "x").Err(), ErrClosed)
	assert.ErrorIs(t, h.Watch(ctx, func(tx *redis.Tx) error { return nil }, "k"), ErrClosed)
	_, err = h.Pipeline().Exec(ctx)
	assert.ErrorIs(t, err, ErrClosed)
	assert.ErrorIs(t, h.Start(ctx), ErrClosed)
}
