package migration

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/alicebob/miniredis/v2"
	goRedis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	piSql "github.com/sllt/pi/pkg/pi/datasource/sql"
)

type sampleArchiveAdapter struct {
	tag string
}

type sampleArchiveWriter interface {
	WriteArchive(name string) error
}

func (s *sampleArchiveAdapter) WriteArchive(string) error { return nil }

func TestMigrationResolve_GuardsAndAdapters(t *testing.T) {
	_, db := openTestSQL(t, testConfig{
		"DB_DIALECT":             "sqlite",
		"DB_NAME":                filepath.Join(t.TempDir(), "tx_guard.db"),
		"DB_MAX_OPEN_CONNECTION": "1",
	})
	analyticsDB, _, _ := piSql.NewSQLMocksWithConfig(t, &piSql.DBConfig{Dialect: "mysql"})

	archive := &sampleArchiveAdapter{tag: "primary-archive"}
	lookup := &fakeAdapterLookup{
		items: map[string]any{
			"archive":       archive,
			"sql-analytics": analyticsDB,
		},
	}

	deps := Deps{
		SQL:      db,
		Adapters: lookup,
	}

	var ran bool
	_, err := Run(t.Context(), map[int64]Migrate{
		1: {
			Name: "guard_test",
			UpContext: func(ctx context.Context, d Datasource) error {
				ran = true

				// 1. Empty interface any must be rejected immediately.
				_, errAny := Resolve[any](d)
				require.ErrorIs(t, errAny, ErrInvalidAdapterType)
				_, errNamedAny := ResolveNamed[any](d, "archive")
				require.ErrorIs(t, errNamedAny, ErrInvalidAdapterType)

				// 2. Requesting non-transactional SQL pool types must return ErrTransactionalDatasource
				// even when "sql-analytics" is mounted in external adapters!
				_, errPool := Resolve[piSql.Client](d)
				require.ErrorIs(t, errPool, ErrTransactionalDatasource)
				_, errDB := Resolve[*piSql.DB](d)
				require.ErrorIs(t, errDB, ErrTransactionalDatasource)
				_, errNamedPool := ResolveNamed[piSql.Client](d, "sql")
				require.ErrorIs(t, errNamedPool, ErrTransactionalDatasource)

				// 3. Explicit named lookup for secondary SQL pool succeeds!
				secPool, errSec := ResolveNamed[piSql.Client](d, "sql-analytics")
				require.NoError(t, errSec)
				require.Same(t, analyticsDB, secPool)

				// 4. Ambiguity between active SQL tx ("sql") and "sql-analytics" when querying piSql.Executor.
				_, errAmb := Resolve[piSql.Executor](d)
				require.ErrorIs(t, errAmb, ErrAmbiguousAdapter)

				// 5. Accessing active *piSql.Tx works cleanly.
				tx, ok := Use[*piSql.Tx](d)
				require.True(t, ok)
				require.NotNil(t, tx)
				_, execErr := tx.ExecContext(ctx, "CREATE TABLE guard_items (id INT PRIMARY KEY)")
				require.NoError(t, execErr)

				// 6. Concrete pointer & narrow interface lookup on external adapter.
				gotPtr := MustUse[*sampleArchiveAdapter](d)
				require.Same(t, archive, gotPtr)
				gotIface, ok := Use[sampleArchiveWriter](d)
				require.True(t, ok)
				require.Same(t, archive, gotIface)

				// 7. Struct value pointer hint.
				_, errStruct := Resolve[sampleArchiveAdapter](d)
				require.ErrorIs(t, errStruct, ErrInvalidAdapterType)
				assert.Contains(t, errStruct.Error(), "did you mean *")

				// 8. Named mismatch vs missing.
				_, errMismatch := ResolveNamed[piSql.Executor](d, "archive")
				require.ErrorIs(t, errMismatch, ErrInvalidAdapterType)
				_, errMissing := ResolveNamed[*sampleArchiveAdapter](d, "does-not-exist")
				require.ErrorIs(t, errMissing, ErrAdapterNotFound)

				return nil
			},
		},
	}, deps)
	require.NoError(t, err)
	require.True(t, ran)
}

func TestMigrationResolve_RedisPoolGuardAndTypedNilTarget(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := goRedis.NewClient(&goRedis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })

	var typedNilDeps *Deps
	_, err := Run(t.Context(), map[int64]Migrate{
		1: {UP: func(Datasource) error { return nil }},
	}, typedNilDeps)
	require.ErrorIs(t, err, ErrNoDatasource)

	deps := Deps{Redis: testRedis{rc}}
	_, err = Run(t.Context(), map[int64]Migrate{
		1: {
			UpContext: func(ctx context.Context, d Datasource) error {
				_, errPool := Resolve[piRedis.Client](d)
				require.ErrorIs(t, errPool, ErrTransactionalDatasource)
				_, errRaw := Resolve[*goRedis.Client](d)
				require.ErrorIs(t, errRaw, ErrTransactionalDatasource)
				_, errNamed := ResolveNamed[RedisSource](d, "redis")
				require.ErrorIs(t, errNamed, ErrTransactionalDatasource)

				pipe, ok := Use[goRedis.Pipeliner](d)
				require.True(t, ok)
				require.NotNil(t, pipe)
				return d.Redis.Set(ctx, "k", "v", 0).Err()
			},
		},
	}, deps)
	require.NoError(t, err)
}
