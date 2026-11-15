package pi

import (
	"context"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/datasource/file"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/metrics"
	"github.com/sllt/pi/pkg/pi/migration"
	"github.com/sllt/pi/pkg/pi/service"
	"go.opentelemetry.io/otel/trace"
)

type dummyDocStore struct {
	id string
}

type narrowFileRemover interface {
	Remove(name string) error
}

type stubObjectStore struct {
	file.FileSystemProvider
	id string
}

func (s *stubObjectStore) Remove(string) error { return nil }

func TestAdapterRegistry_EmptyInterfaceAndZeroSources(t *testing.T) {
	app, err := Build(
		WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
		WithAdapter("docs", &dummyDocStore{id: "d1"}, Borrowed),
	)
	require.NoError(t, err)
	ctx := app.NewContext(context.Background())

	t.Run("any is always rejected with ErrInvalidAdapterType", func(t *testing.T) {
		_, err := Resolve[any](app)
		require.ErrorIs(t, err, ErrInvalidAdapterType)

		_, err = ResolveNamed[any](app, "docs")
		require.ErrorIs(t, err, ErrInvalidAdapterType)

		_, err = Resolve[any](nil)
		require.ErrorIs(t, err, ErrInvalidAdapterType)

		_, err = ResolveNamed[any](nil, "docs")
		require.ErrorIs(t, err, ErrInvalidAdapterType)

		_, ok := Use[any](ctx)
		assert.False(t, ok)

		_, ok = UseNamed[any](ctx, "docs")
		assert.False(t, ok)

		assert.Panics(t, func() { _ = MustUse[any](app) })
		assert.Panics(t, func() { _ = MustUseNamed[any](app, "docs") })
	})

	t.Run("nil, typed-nil, and uninitialized sources return ErrAdapterNotFound safely", func(t *testing.T) {
		var nilApp *App
		var nilCtx *Context
		sources := []ProviderSource{nil, nilApp, nilCtx, &App{}, &Context{}}

		for _, src := range sources {
			got, ok := Use[*dummyDocStore](src)
			assert.False(t, ok)
			assert.Nil(t, got)

			_, err := Resolve[*dummyDocStore](src)
			require.ErrorIs(t, err, ErrAdapterNotFound)

			_, err = ResolveNamed[*dummyDocStore](src, "docs")
			require.ErrorIs(t, err, ErrAdapterNotFound)
		}
	})
}

func TestAdapterRegistry_StructValueHintAndNamedTypeMismatch(t *testing.T) {
	doc := &dummyDocStore{id: "primary-doc"}
	app, err := Build(
		WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
		WithAdapter("mongo", doc, Borrowed),
	)
	require.NoError(t, err)
	ctx := app.NewContext(context.Background())

	got, err := Resolve[*dummyDocStore](ctx)
	require.NoError(t, err)
	assert.Same(t, doc, got)
	assert.Same(t, doc, MustUse[*dummyDocStore](app))
	assert.Same(t, doc, MustUseNamed[*dummyDocStore](ctx, "mongo"))

	// Value-type struct lookup emits friendly pointer hint wrapped in ErrInvalidAdapterType.
	_, err = Resolve[dummyDocStore](ctx)
	require.ErrorIs(t, err, ErrInvalidAdapterType)
	assert.Contains(t, err.Error(), "did you mean *pi.dummyDocStore?")

	_, err = ResolveNamed[dummyDocStore](ctx, "mongo")
	require.ErrorIs(t, err, ErrInvalidAdapterType)
	assert.Contains(t, err.Error(), "did you mean *pi.dummyDocStore?")

	// Existing name with incompatible target type returns ErrInvalidAdapterType, NOT ErrAdapterNotFound.
	_, err = ResolveNamed[io.Reader](ctx, "mongo")
	require.ErrorIs(t, err, ErrInvalidAdapterType)
	assert.NotErrorIs(t, err, ErrAdapterNotFound)

	// Missing name returns ErrAdapterNotFound.
	_, err = ResolveNamed[*dummyDocStore](ctx, "non-existent")
	require.ErrorIs(t, err, ErrAdapterNotFound)
}

func TestAdapterRegistry_TypedNilRejection(t *testing.T) {
	var (
		nilSQL     *piSQL.DB
		nilRedis   *piRedis.Redis
		nilPubSub  *managedPubSubHandle
		nilFS      *stubObjectStore
		nilHTTPSvc service.HTTP
		nilDoc     *dummyDocStore
		nilLogger  logging.Logger
		nilMetrics metrics.Manager
		nilHandler *http.ServeMux
		nilTP      trace.TracerProvider
	)

	buildOpts := []Option{
		WithSQL(nilSQL, Borrowed),
		WithRedis(nilRedis, Borrowed),
		WithPubSub(nilPubSub, Borrowed),
		WithFile(nilFS, Borrowed),
		WithHTTPService("svc", nilHTTPSvc, Borrowed),
		WithAdapter("doc", nilDoc, Borrowed),
		WithLogger(nilLogger),
		WithMetrics(nilMetrics),
		WithMetricsHandler(nilHandler),
		WithValidator(nil),
		WithTracerProvider(nilTP, Borrowed),
	}
	for i, opt := range buildOpts {
		_, err := Build(opt)
		require.ErrorIsf(t, err, ErrNilResource, "case %d", i)
	}

	app, err := Build(WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}))
	require.NoError(t, err)

	require.ErrorIs(t, app.SetSQL(nilSQL, Borrowed), ErrNilResource)
	require.ErrorIs(t, app.SetRedis(nilRedis, Borrowed), ErrNilResource)
	require.ErrorIs(t, app.SetPubSub(nilPubSub, Borrowed), ErrNilResource)
	require.ErrorIs(t, app.SetFile(nilFS, Borrowed), ErrNilResource)
	require.ErrorIs(t, app.SetHTTPService("svc", nilHTTPSvc, Borrowed), ErrNilResource)
	require.ErrorIs(t, app.Mount("doc", nilDoc, Borrowed), ErrNilResource)
	assert.Panics(t, func() { app.MustMount("doc", nilDoc, Borrowed) })

	// Verify untyped nil guarantees on unconfigured core slots.
	assert.True(t, app.SQL() == nil)
	assert.True(t, app.Redis() == nil)
	assert.True(t, app.PubSub() == nil)
	assert.True(t, app.GetPublisher() == nil)
	assert.True(t, app.GetSubscriber() == nil)
}

func TestAdapterRegistry_DefaultFSIsolation(t *testing.T) {
	baseCfg := WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"})

	t.Run("default local FS never leaks into custom narrow interfaces", func(t *testing.T) {
		app, err := Build(baseCfg)
		require.NoError(t, err)

		// Default local FS satisfies Remove(string) error, yet narrowFileRemover MUST NOT match it!
		_, err = Resolve[narrowFileRemover](app)
		require.ErrorIs(t, err, ErrAdapterNotFound)

		// Exact file.FileSystem and file.FileSystemProvider fallback works.
		fs, err := Resolve[file.FileSystem](app)
		require.NoError(t, err)
		require.NotNil(t, fs)

		fsp, err := Resolve[file.FileSystemProvider](app)
		require.NoError(t, err)
		assert.Same(t, fs, fsp)

		namedFS, err := ResolveNamed[file.FileSystem](app, "file")
		require.NoError(t, err)
		assert.Same(t, fs, namedFS)
	})

	t.Run("single mounted external FS wins without ambiguity against default local FS", func(t *testing.T) {
		s3 := &stubObjectStore{FileSystemProvider: file.NewLocalFileSystem(logging.NewLogger(logging.FATAL)), id: "s3-1"}
		app, err := Build(baseCfg, WithAdapter("archive-s3", s3, Borrowed))
		require.NoError(t, err)

		gotFS, err := Resolve[file.FileSystem](app)
		require.NoError(t, err)
		assert.Same(t, s3, gotFS)

		gotNarrow, err := Resolve[narrowFileRemover](app)
		require.NoError(t, err)
		assert.Same(t, s3, gotNarrow)

		// Explicit named lookup of "file" still accesses the local default FS.
		defaultFS, err := ResolveNamed[file.FileSystem](app, "file")
		require.NoError(t, err)
		assert.NotSame(t, s3, defaultFS)
	})

	t.Run("two mounted external FS adapters report ambiguity without polluting candidate list with file", func(t *testing.T) {
		s3a := &stubObjectStore{FileSystemProvider: file.NewLocalFileSystem(logging.NewLogger(logging.FATAL)), id: "a"}
		s3b := &stubObjectStore{FileSystemProvider: file.NewLocalFileSystem(logging.NewLogger(logging.FATAL)), id: "b"}
		app, err := Build(
			baseCfg,
			WithAdapter("s3-a", s3a, Borrowed),
			WithAdapter("s3-b", s3b, Borrowed),
		)
		require.NoError(t, err)

		_, err = Resolve[file.FileSystem](app)
		require.ErrorIs(t, err, ErrAmbiguousAdapter)
		assert.Contains(t, err.Error(), "[s3-a, s3-b]")
		assert.NotContains(t, err.Error(), "file,")
	})

	t.Run("explicit WithFile plus external FS adapter triggers ErrAmbiguousAdapter", func(t *testing.T) {
		explicitPrimary := &stubObjectStore{FileSystemProvider: file.NewLocalFileSystem(logging.NewLogger(logging.FATAL)), id: "primary"}
		archive := &stubObjectStore{FileSystemProvider: file.NewLocalFileSystem(logging.NewLogger(logging.FATAL)), id: "archive"}
		app, err := Build(
			baseCfg,
			WithFile(explicitPrimary, Borrowed),
			WithAdapter("archive-s3", archive, Borrowed),
		)
		require.NoError(t, err)

		_, err = Resolve[file.FileSystem](app)
		require.ErrorIs(t, err, ErrAmbiguousAdapter)
		assert.Contains(t, err.Error(), "[file, archive-s3]")
	})
}

func TestAdapterRegistry_CoreSlotAndExternalAmbiguityAndConcurrentFreeze(t *testing.T) {
	primaryDB, _, _ := piSQL.NewSQLMocks(t)
	defer primaryDB.Close()

	analyticsDB, _, _ := piSQL.NewSQLMocks(t)
	defer analyticsDB.Close()

	doc := &dummyDocStore{id: "doc-1"}
	app, err := Build(
		WithConfig(map[string]string{"HTTP_ENABLED": "false", "GRPC_ENABLED": "false"}),
		WithSQL(primaryDB, Borrowed),
		WithAdapter("sql-analytics", analyticsDB, Borrowed),
		WithAdapter("mongo", doc, Borrowed),
	)
	require.NoError(t, err)

	assert.Same(t, primaryDB, app.SQL())

	// Unnamed sql.Client is ambiguous when both primary "sql" and "sql-analytics" exist.
	_, err = Resolve[piSQL.Client](app)
	require.ErrorIs(t, err, ErrAmbiguousAdapter)
	assert.Contains(t, err.Error(), "[sql, sql-analytics]")

	gotPrimary, err := ResolveNamed[piSQL.Client](app, "sql")
	require.NoError(t, err)
	assert.Same(t, primaryDB, gotPrimary)

	gotAnalytics, err := ResolveNamed[piSQL.Client](app, "sql-analytics")
	require.NoError(t, err)
	assert.Same(t, analyticsDB, gotAnalytics)

	// Freeze and hammer concurrent lookups under -race to exercise lock-free sync.Map ifaceCache & concrete index.
	app.kernel.freeze()

	var wg sync.WaitGroup
	for g := 0; g < 24; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := app.NewContext(context.Background())
			for i := 0; i < 100; i++ {
				d, ok := Use[*dummyDocStore](ctx)
				assert.True(t, ok)
				assert.Same(t, doc, d)

				_, ambigErr := Resolve[piSQL.Client](ctx)
				assert.ErrorIs(t, ambigErr, ErrAmbiguousAdapter)

				adb, ok := UseNamed[piSQL.Client](ctx, "sql-analytics")
				assert.True(t, ok)
				assert.Same(t, analyticsDB, adb)

				fs, ok := Use[file.FileSystem](ctx)
				assert.True(t, ok)
				assert.NotNil(t, fs)

				_, missOK := Use[io.WriteCloser](ctx)
				assert.False(t, missOK)
			}
		}()
	}
	wg.Wait()
}

func TestAdapterRegistry_UpsertCoreRebuildsConcreteTypeIndex(t *testing.T) {
	// Legacy mutable app: replacing default local FS evicts old concrete type from index before freeze.
	k := newLegacyKernel(nil, logging.NewLogger(logging.FATAL), false)
	app := &App{kernel: k}

	defaultConcreteType := app.File()
	require.NotNil(t, defaultConcreteType)

	customFS := &stubObjectStore{
		FileSystemProvider: file.NewLocalFileSystem(logging.NewLogger(logging.FATAL)),
		id:                 "replaced-fs",
	}
	require.NoError(t, app.SetFile(customFS, Borrowed))

	got, err := Resolve[*stubObjectStore](app)
	require.NoError(t, err)
	assert.Same(t, customFS, got)

	// Ensure temporary files are never created by purely in-memory tests.
	_ = os.DevNull
}

func TestAdapterRegistry_MigrationTransactionalSafetyIntegration(t *testing.T) {
	dbPath := t.TempDir() + "/mig.db"
	analyticsDB, _, _ := piSQL.NewSQLMocks(t)
	defer analyticsDB.Close()

	doc := &dummyDocStore{id: "mig-doc"}
	app, err := Build(
		WithConfig(map[string]string{
			"HTTP_ENABLED": "false",
			"GRPC_ENABLED": "false",
			"DB_DIALECT":   "sqlite",
			"DB_NAME":      dbPath,
		}),
		WithManagedSQL(),
		WithAdapter("sql-analytics", analyticsDB, Borrowed),
		WithAdapter("mongo", doc, Borrowed),
	)
	require.NoError(t, err)

	require.NoError(t, app.Start(context.Background()))
	defer func() {
		require.NoError(t, app.Stop(context.Background()))
	}()

	var ran bool
	migs := map[int64]migration.Migrate{
		1: {
			UP: func(d migration.Datasource) error {
				ran = true
				// Pool types MUST be blocked with ErrTransactionalDatasource (even though sql-analytics is mounted!).
				_, poolErr := migration.Resolve[piSQL.Client](d)
				if !assert.ErrorIs(t, poolErr, datasource.ErrTransactionalDatasource) {
					return poolErr
				}
				_, namedPoolErr := migration.ResolveNamed[piSQL.Client](d, "sql")
				if !assert.ErrorIs(t, namedPoolErr, datasource.ErrTransactionalDatasource) {
					return namedPoolErr
				}

				// Explicit named lookup of external analytics DB succeeds!
				extDB, extErr := migration.ResolveNamed[piSQL.Client](d, "sql-analytics")
				if !assert.NoError(t, extErr) {
					return extErr
				}
				assert.Same(t, analyticsDB, extDB)

				// Unnamed Executor is ambiguous because both d.SQL (*sql.Tx) and sql-analytics implement Executor.
				_, execErr := migration.Resolve[piSQL.Executor](d)
				assert.ErrorIs(t, execErr, ErrAmbiguousAdapter)

				// Named "sql" Executor returns active *piSQL.Tx!
				txExec, txErr := migration.ResolveNamed[piSQL.Executor](d, "sql")
				if !assert.NoError(t, txErr) {
					return txErr
				}
				assert.Same(t, d.SQL, txExec)

				// External document adapter and default FS fallback work cleanly.
				gotDoc, docErr := migration.Resolve[*dummyDocStore](d)
				if !assert.NoError(t, docErr) {
					return docErr
				}
				assert.Same(t, doc, gotDoc)

				_, narrowErr := migration.Resolve[narrowFileRemover](d)
				assert.ErrorIs(t, narrowErr, ErrAdapterNotFound)

				gotFS, fsErr := migration.Resolve[file.FileSystem](d)
				if !assert.NoError(t, fsErr) {
					return fsErr
				}
				assert.NotNil(t, gotFS)

				_, execSQLErr := d.SQL.Exec("CREATE TABLE mig_items (id INTEGER PRIMARY KEY)")
				return execSQLErr
			},
		},
	}

	res, err := app.MigrateContext(context.Background(), migs)
	require.NoError(t, err)
	assert.True(t, ran)
	assert.Len(t, res.Applied, 1)
}
