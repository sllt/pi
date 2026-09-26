package migration

import "github.com/sllt/pi/pkg/pi/infra"

type Datasource struct {
	// TODO Logger should not be embedded rather it should be a field.
	// Need to think it through as it will bring breaking changes.
	Logger

	SQL           SQL
	Redis         Redis
	PubSub        PubSub
	Clickhouse    Clickhouse
	Oracle        Oracle
	Cassandra     Cassandra
	Mongo         Mongo
	ArangoDB      ArangoDB
	SurrealDB     SurrealDB
	DGraph        DGraph
	ScyllaDB      ScyllaDB
	Elasticsearch Elasticsearch
	OpenTSDB      OpenTSDB
}

// It is a base implementation for migration manager, on this other database drivers have been wrapped.

func (*Datasource) checkAndCreateMigrationTable(*infra.Container) error {
	return nil
}

func (*Datasource) getLastMigration(*infra.Container) (int64, error) {
	return 0, nil
}

func (*Datasource) beginTransaction(*infra.Container) transactionData {
	return transactionData{}
}

func (*Datasource) commitMigration(c *infra.Container, data transactionData) error {
	c.Infof("Migration %v ran successfully", data.MigrationNumber)

	return nil
}

func (*Datasource) rollback(*infra.Container, transactionData) {}
