package pi

import (
	"go.opentelemetry.io/otel"

	"github.com/sllt/pi/pkg/pi/datasource/file"
	"github.com/sllt/pi/pkg/pi/infra"
)

// AddMongo sets the Mongo datasource in the app's infra.
func (a *App) AddMongo(db infra.MongoProvider) {
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-mongo")

	db.UseTracer(tracer)

	db.Connect()

	a.container.Mongo = db
}

// AddFTP sets the FTP datasource in the app's infra.
// Deprecated: Use the AddFile method instead.
func (a *App) AddFTP(fs file.FileSystemProvider) {
	fs.UseLogger(a.Logger())
	fs.UseMetrics(a.Metrics())

	fs.Connect()

	a.container.File = fs
}

// AddPubSub sets the PubSub client in the app's infra.
func (a *App) AddPubSub(pubsub infra.PubSubProvider) {
	pubsub.UseLogger(a.Logger())
	pubsub.UseMetrics(a.Metrics())

	pubsub.Connect()

	a.container.PubSub = pubsub
}

// AddFileStore sets the FTP, SFTP, S3, GCS, or Azure File Storage datasource in the app's infra.
func (a *App) AddFileStore(fs file.FileSystemProvider) {
	fs.UseLogger(a.Logger())
	fs.UseMetrics(a.Metrics())

	fs.Connect()

	a.container.File = fs
}

// AddClickhouse initializes the clickhouse client.
// Official implementation is available in the package : github.com/sllt/pi/pkg/pi/datasource/clickhouse .
func (a *App) AddClickhouse(db infra.ClickhouseProvider) {
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-clickhouse")

	db.UseTracer(tracer)

	db.Connect()

	a.container.Clickhouse = db
}

// AddOracle initializes the OracleDB client.
// Official implementation is available in the package: github.com/sllt/pi/pkg/pi/datasource/oracle.
func (a *App) AddOracle(db infra.OracleProvider) {
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-oracle")

	db.UseTracer(tracer)

	db.Connect()

	a.container.Oracle = db
}

// UseMongo sets the Mongo datasource in the app's infra.
// Deprecated: Use the AddMongo method instead.
func (a *App) UseMongo(db infra.Mongo) {
	a.container.Mongo = db
}

// AddCassandra sets the Cassandra datasource in the app's infra.
func (a *App) AddCassandra(db infra.CassandraProvider) {
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-cassandra")

	db.UseTracer(tracer)

	db.Connect()

	a.container.Cassandra = db
}

// AddKVStore sets the KV-Store datasource in the app's infra.
func (a *App) AddKVStore(db infra.KVStoreProvider) {
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-kvstore")

	db.UseTracer(tracer)

	db.Connect()

	a.container.KVStore = db
}

// AddSolr sets the Solr datasource in the app's infra.
func (a *App) AddSolr(db infra.SolrProvider) {
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-solr")

	db.UseTracer(tracer)

	db.Connect()

	a.container.Solr = db
}

// AddDgraph sets the Dgraph datasource in the app's infra.
func (a *App) AddDgraph(db infra.DgraphProvider) {
	// Create the Dgraph client with the provided configuration
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-dgraph")

	db.UseTracer(tracer)

	db.Connect()

	a.container.DGraph = db
}

// AddOpenTSDB sets the OpenTSDB datasource in the app's infra.
func (a *App) AddOpenTSDB(db infra.OpenTSDBProvider) {
	// Create the Opentsdb client with the provided configuration
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-opentsdb")

	db.UseTracer(tracer)

	db.Connect()

	a.container.OpenTSDB = db
}

// AddScyllaDB sets the ScyllaDB datasource in the app's infra.
func (a *App) AddScyllaDB(db infra.ScyllaDBProvider) {
	// Create the ScyllaDB client with the provided configuration
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-scylladb")
	db.UseTracer(tracer)
	db.Connect()
	a.container.ScyllaDB = db
}

// AddArangoDB sets the ArangoDB datasource in the app's infra.
func (a *App) AddArangoDB(db infra.ArangoDBProvider) {
	// Set up logger, metrics, and tracer
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	// Get tracer from OpenTelemetry
	tracer := otel.GetTracerProvider().Tracer("pi-arangodb")
	db.UseTracer(tracer)

	// Connect to ArangoDB
	db.Connect()

	// Add the ArangoDB provider to the container
	a.container.ArangoDB = db
}

func (a *App) AddSurrealDB(db infra.SurrealBDProvider) {
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-surrealdb")
	db.UseTracer(tracer)
	db.Connect()
	a.container.SurrealDB = db
}

func (a *App) AddElasticsearch(db infra.ElasticsearchProvider) {
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-elasticsearch")
	db.UseTracer(tracer)
	db.Connect()

	a.container.Elasticsearch = db
}

func (a *App) AddCouchbase(db infra.CouchbaseProvider) {
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-couchbase")
	db.UseTracer(tracer)
	db.Connect()

	a.container.Couchbase = db
}

// AddDBResolver sets up database resolver with read/write splitting.
func (a *App) AddDBResolver(resolver infra.DBResolverProvider) {
	// Validate primary SQL exists
	if a.container.SQL == nil {
		a.Logger().Fatal("Primary SQL connection must be configured before adding DBResolver")
		return
	}

	resolver.UseLogger(a.Logger())
	resolver.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-dbresolver")
	resolver.UseTracer(tracer)

	resolver.Connect()

	// Replace the SQL connection with the resolver
	a.container.SQL = resolver.GetResolver()

	a.Logger().Logf("DB Resolver initialized successfully")
}

func (a *App) AddInfluxDB(db infra.InfluxDBProvider) {
	db.UseLogger(a.Logger())
	db.UseMetrics(a.Metrics())

	tracer := otel.GetTracerProvider().Tracer("pi-influxdb")
	db.UseTracer(tracer)
	db.Connect()

	a.container.InfluxDB = db
}

func (a *App) GetSQL() infra.DB {
	return a.container.SQL
}
