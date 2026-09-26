package pi

import (
	"context"
	"testing"

	"github.com/sllt/pi/pkg/pi/migration"
	"github.com/stretchr/testify/require"
)

type migrationV2App interface {
	Migrate(map[int64]migration.Migrate, ...migration.Option) (migration.Result, error)
	MigrateContext(context.Context, map[int64]migration.Migrate, ...migration.Option) (migration.Result, error)
	MigrationPlanContext(context.Context, map[int64]migration.Migrate, ...migration.Option) (migration.PlanResult, error)
	MigrationStatusContext(context.Context, map[int64]migration.Migrate, ...migration.Option) (migration.StatusResult, error)
}

var _ migrationV2App = (*App)(nil)

func TestMigrationV2APISignatures(t *testing.T) {
	app := &App{}
	var migrate func(map[int64]migration.Migrate, ...migration.Option) (migration.Result, error) = app.Migrate
	var run func(context.Context, map[int64]migration.Migrate, ...migration.Option) (migration.Result, error) = app.MigrateContext
	defs := map[int64]migration.Migrate{1: {UP: func(migration.Datasource) error { return nil }}}
	_, err := migrate(defs)
	require.ErrorIs(t, err, migration.ErrNoDatasource)
	_, err = run(t.Context(), defs)
	require.ErrorIs(t, err, migration.ErrNoDatasource)
	_, err = app.MigrationPlan(defs)
	require.ErrorIs(t, err, migration.ErrNoDatasource)
	_, err = app.MigrationStatus(defs)
	require.ErrorIs(t, err, migration.ErrNoDatasource)
	require.Panics(t, func() { app.MustMigrate(defs) })
}
