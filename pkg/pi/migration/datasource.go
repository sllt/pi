package migration

import "context"

type Datasource struct {
	Logger

	SQL      SQL
	Redis    Redis
	PubSub   PubSub
	adapters AdapterLookup
}

func (*Datasource) checkAndCreateMigrationTable(context.Context, *Deps) error {
	return nil
}

func (*Datasource) listApplied(context.Context, *Deps) ([]Record, error) {
	return nil, nil
}

func (*Datasource) getLastMigration(context.Context, *Deps) (int64, error) {
	return 0, nil
}

func (*Datasource) beginTransaction(context.Context, *Deps) (transactionData, error) {
	return transactionData{}, nil
}

func (d *Datasource) commitMigration(_ context.Context, deps *Deps, data transactionData) error {
	if deps != nil && !isNil(deps.Logger) {
		deps.Logger.Infof("Migration %v ran successfully", data.MigrationNumber)
	} else if d != nil && !isNil(d.Logger) {
		d.Logger.Infof("Migration %v ran successfully", data.MigrationNumber)
	}

	return nil
}

func (*Datasource) rollback(context.Context, *Deps, transactionData) error {
	return nil
}
