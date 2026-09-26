package migration

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/stretchr/testify/require"
)

type testConfig map[string]string

func (c testConfig) Get(k string) string { return c[k] }
func (c testConfig) GetOrDefault(k, v string) string {
	if c[k] != "" {
		return c[k]
	}
	return v
}

func openTestSQL(t *testing.T, cfg testConfig) (*infra.Container, *piSQL.DB) {
	t.Helper()
	l := logging.NewWriterLogger(logging.ERROR, io.Discard, io.Discard)
	db, err := piSQL.OpenContext(t.Context(), cfg, l, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return &infra.Container{SQL: db, Logger: l}, db
}

// External backends are opt-in and MUST point at an isolated, disposable database.
// The test owns the three named tables below; the release harness creates the DB.
func TestMigrationSQLBackends(t *testing.T) {
	for _, dialect := range []string{"sqlite", "mysql", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			cfg := testConfig{"DB_DIALECT": dialect, "DB_NAME": filepath.Join(t.TempDir(), "migration.db"), "DB_MAX_OPEN_CONNECTION": "2"}
			if dialect != "sqlite" {
				prefix := "PI_MIGRATION_TEST_" + dialect + "_"
				if os.Getenv(prefix+"PORT") == "" {
					t.Skip("isolated backend not configured")
				}
				for _, key := range []string{"HOST", "PORT", "USER", "PASSWORD", "NAME"} {
					cfg["DB_"+key] = os.Getenv(prefix + key)
				}
			}
			c, db := openTestSQL(t, cfg)
			for _, table := range []string{"kite_migrations", "kite_migration_locks", "v030_effects"} {
				_, err := db.Exec("DROP TABLE IF EXISTS " + table)
				require.NoError(t, err)
			}
			_, err := db.Exec("CREATE TABLE v030_effects (id BIGINT PRIMARY KEY)")
			require.NoError(t, err)
			defs := map[int64]Migrate{
				1: {Name: "old_up", UP: func(d Datasource) error { _, e := d.SQL.Exec("INSERT INTO v030_effects (id) VALUES (1)"); return e }},
				2: {Name: "context_up", UpContext: func(ctx context.Context, d Datasource) error {
					_, e := d.SQL.ExecContext(ctx, "INSERT INTO v030_effects (id) VALUES (2)")
					return e
				}},
			}
			plan, err := Plan(t.Context(), defs, c)
			require.NoError(t, err)
			require.Len(t, plan.Items, 2)
			result, err := Run(t.Context(), defs, c, WithDryRun(), WithLock())
			require.NoError(t, err)
			require.Len(t, result.Skipped, 2)
			result, err = Run(t.Context(), defs, c, WithTarget(1), WithLock())
			require.NoError(t, err)
			require.Equal(t, []int64{1}, result.AppliedVersions())
			result, err = Run(t.Context(), defs, c, WithLock())
			require.NoError(t, err)
			require.Equal(t, []int64{2}, result.AppliedVersions())
			result, err = Run(t.Context(), defs, c, WithLock())
			require.NoError(t, err)
			require.Len(t, result.Skipped, 2)
			status, err := Status(t.Context(), defs, c)
			require.NoError(t, err)
			require.True(t, status.StatePrecise)
			require.Equal(t, StateSourceSQL, status.StateSource)
			require.Len(t, status.Applied, 2)
			cause := errors.New("user panic")
			defs[3] = Migrate{Name: "panic", UpContext: func(ctx context.Context, d Datasource) error {
				_, e := d.SQL.ExecContext(ctx, "INSERT INTO v030_effects (id) VALUES (3)")
				if e != nil {
					return e
				}
				panic(cause)
			}}
			result, err = Run(t.Context(), defs, c, WithLock())
			require.ErrorIs(t, err, ErrMigrationPanic)
			require.ErrorIs(t, err, cause)
			require.ErrorIs(t, err, ErrMigrationFailed)
			var versionErr *VersionError
			require.ErrorAs(t, err, &versionErr)
			require.Equal(t, "up", versionErr.Op)
			require.Equal(t, int64(3), result.Failed.Version)
			var count int
			require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM v030_effects WHERE id = 3").Scan(&count))
			require.Zero(t, count)
			ctx, cancel := context.WithCancel(t.Context())
			defs[3] = Migrate{UpContext: func(ctx context.Context, d Datasource) error {
				_, e := d.SQL.ExecContext(ctx, "INSERT INTO v030_effects (id) VALUES (3)")
				cancel()
				return e
			}}
			_, err = Run(ctx, defs, c, WithLock())
			require.ErrorIs(t, err, context.Canceled)
			require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM v030_effects WHERE id = 3").Scan(&count))
			require.Zero(t, count)
			defs[3] = Migrate{UP: func(d Datasource) error { _, e := d.SQL.Exec("INSERT INTO v030_effects (id) VALUES (3)"); return e }}
			result, err = Run(t.Context(), defs, c, WithLock())
			require.NoError(t, err)
			require.Equal(t, []int64{3}, result.AppliedVersions())
			_, err = db.Exec("DELETE FROM kite_migrations WHERE version = 2")
			require.NoError(t, err)
			_, err = Run(t.Context(), defs, c, WithLock())
			require.ErrorIs(t, err, ErrMigrationGap)
			status, err = Status(t.Context(), defs, c)
			require.NoError(t, err)
			require.Equal(t, []int64{2}, status.Gaps)
			// A late owner must not delete a lease that has already been replaced.
			release, err := acquireSQLMigrationLock(t.Context(), c, time.Minute)
			require.NoError(t, err)
			_, err = db.Exec("UPDATE kite_migration_locks SET owner = 'replacement'")
			require.NoError(t, err)
			require.NoError(t, release(t.Context()))
			require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM kite_migration_locks").Scan(&count))
			require.Equal(t, 1, count)
			_, err = db.Exec("DELETE FROM kite_migration_locks")
			require.NoError(t, err)
			oldRelease, err := acquireSQLMigrationLock(t.Context(), c, time.Minute)
			require.NoError(t, err)
			_, err = db.Exec("UPDATE kite_migration_locks SET expires_at = '2000-01-01 00:00:00'")
			require.NoError(t, err)
			newRelease, err := acquireSQLMigrationLock(t.Context(), c, time.Minute)
			require.NoError(t, err)
			require.NoError(t, oldRelease(t.Context()))
			_, err = acquireSQLMigrationLock(t.Context(), c, time.Minute)
			require.ErrorIs(t, err, ErrMigrationLocked)
			require.NoError(t, newRelease(t.Context()))
		})
	}
}

func TestMigrationLockProcesses(t *testing.T) {
	if path := os.Getenv("PI_MIGRATION_LOCK_CHILD"); path != "" {
		c, _ := openTestSQL(t, testConfig{"DB_DIALECT": "sqlite", "DB_NAME": path})
		_, err := Run(t.Context(), map[int64]Migrate{1: {UP: func(Datasource) error { t.Fatal("contender executed migration"); return nil }}}, c, WithLock())
		require.ErrorIs(t, err, ErrMigrationLocked)
		return
	}
	path := filepath.Join(t.TempDir(), "shared.db")
	c, _ := openTestSQL(t, testConfig{"DB_DIALECT": "sqlite", "DB_NAME": path})
	_, err := Run(t.Context(), map[int64]Migrate{1: {UP: func(Datasource) error {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMigrationLockProcesses$")
		cmd.Env = append(os.Environ(), "PI_MIGRATION_LOCK_CHILD="+path)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		return nil
	}}}, c, WithLock())
	require.NoError(t, err)
}

func TestMigrationBeginContextCancelsPoolWait(t *testing.T) {
	c, db := openTestSQL(t, testConfig{"DB_DIALECT": "sqlite", "DB_NAME": filepath.Join(t.TempDir(), "db"), "DB_MAX_OPEN_CONNECTION": "1"})
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	_, err = (sqlMigrator{migrator: &Datasource{}}).beginTransaction(ctx, c)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestMigrationInvalidVersions(t *testing.T) {
	for _, v := range []int64{-1, 0} {
		defs := map[int64]Migrate{v: {UP: func(Datasource) error { return nil }}}
		_, err := Run(t.Context(), defs, nil)
		require.ErrorIs(t, err, ErrInvalidMigration)
		_, err = Plan(t.Context(), defs, nil)
		require.ErrorIs(t, err, ErrInvalidMigration)
		_, err = Status(t.Context(), defs, nil)
		require.ErrorIs(t, err, ErrInvalidMigration)
	}
}
