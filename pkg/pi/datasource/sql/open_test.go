package sql

import (
	"context"
	"io"
	"path/filepath"
	"testing"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/stretchr/testify/require"
)

func TestOpenContextOwnsConnection(t *testing.T) {
	cfg := config.NewMockConfig(map[string]string{"DB_DIALECT": "sqlite", "DB_NAME": filepath.Join(t.TempDir(), "app.db")})
	l := logging.NewWriterLogger(logging.INFO, io.Discard, io.Discard)
	db, err := OpenContext(t.Context(), cfg, l, nil)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(t.Context()))
	require.NoError(t, db.Close())
	require.Error(t, db.PingContext(t.Context()))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	db, err = OpenContext(ctx, cfg, l, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, db)
}

func TestQuotePostgresValue(t *testing.T) {
	for input, want := range map[string]string{"": "''", "plain": "plain", "two words": "'two words'", "a'b": "'a\\'b'", `a\b`: `'a\\b'`} {
		require.Equal(t, want, quotePostgresValue(input))
	}
}

func TestMySQLConnectorUsesSnapshotTLS(t *testing.T) {
	t.Setenv("DB_TLS_CA_CERT", "/must-not-read-ambient-certificate.pem")
	cfg := &DBConfig{Dialect: "mysql", HostName: "127.0.0.1", Port: "3306", Database: "test", SSLMode: "verify-full"}
	connector, err := snapshotMySQLConnector(cfg, config.NewSnapshot(nil))
	require.NoError(t, err)
	require.NotNil(t, connector)
	_, err = snapshotMySQLConnector(cfg, config.NewSnapshot(map[string]string{"DB_TLS_CLIENT_CERT": "half.pem"}))
	require.ErrorContains(t, err, "must be configured together")
}
