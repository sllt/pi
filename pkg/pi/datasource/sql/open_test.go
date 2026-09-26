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
