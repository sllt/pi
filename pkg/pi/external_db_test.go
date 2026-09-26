package pi

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.uber.org/mock/gomock"

	"github.com/sllt/pi/pkg/pi/datasource/file"
	"github.com/sllt/pi/pkg/pi/infra"
	"github.com/sllt/pi/pkg/pi/testutil"
)

func TestApp_AddKVStore(t *testing.T) {
	t.Run("Adding KV-Store", func(t *testing.T) {
		testutil.NewServerConfigs(t)

		app := New()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := infra.NewMockKVStoreProvider(ctrl)

		mock.EXPECT().UseLogger(app.Logger())
		mock.EXPECT().UseMetrics(app.Metrics())
		mock.EXPECT().UseTracer(otel.GetTracerProvider().Tracer("pi-kvstore"))
		mock.EXPECT().Connect()

		app.AddKVStore(mock)

		assert.Equal(t, mock, app.container.KVStore)
	})
}

func TestApp_AddMongo(t *testing.T) {
	t.Run("Adding MongoDB", func(t *testing.T) {
		testutil.NewServerConfigs(t)

		app := New()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := infra.NewMockMongoProvider(ctrl)

		mock.EXPECT().UseLogger(app.Logger())
		mock.EXPECT().UseMetrics(app.Metrics())
		mock.EXPECT().UseTracer(gomock.Any())
		mock.EXPECT().Connect()

		app.AddMongo(mock)

		assert.Equal(t, mock, app.container.Mongo)
	})
}

func TestApp_AddCassandra(t *testing.T) {
	t.Run("Adding Cassandra", func(t *testing.T) {
		testutil.NewServerConfigs(t)

		app := New()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := infra.NewMockCassandraProvider(ctrl)

		mock.EXPECT().UseLogger(app.Logger())
		mock.EXPECT().UseMetrics(app.Metrics())
		mock.EXPECT().UseTracer(otel.GetTracerProvider().Tracer("pi-cassandra"))
		mock.EXPECT().Connect()

		app.AddCassandra(mock)

		assert.Equal(t, mock, app.container.Cassandra)
	})
}

func TestApp_AddClickhouse(t *testing.T) {
	t.Run("Adding Clickhouse", func(t *testing.T) {
		testutil.NewServerConfigs(t)

		app := New()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := infra.NewMockClickhouseProvider(ctrl)

		mock.EXPECT().UseLogger(app.Logger())
		mock.EXPECT().UseMetrics(app.Metrics())
		mock.EXPECT().UseTracer(otel.GetTracerProvider().Tracer("pi-clickhouse"))
		mock.EXPECT().Connect()

		app.AddClickhouse(mock)

		assert.Equal(t, mock, app.container.Clickhouse)
	})
}

func TestApp_AddOracle(t *testing.T) {
	t.Run("Adding OracleDB", func(t *testing.T) {
		testutil.NewServerConfigs(t)

		app := New()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := infra.NewMockOracleProvider(ctrl)

		mock.EXPECT().UseLogger(app.Logger())
		mock.EXPECT().UseMetrics(app.Metrics())
		mock.EXPECT().UseTracer(otel.GetTracerProvider().Tracer("pi-oracle"))
		mock.EXPECT().Connect()

		app.AddOracle(mock)

		assert.Equal(t, mock, app.container.Oracle)
	})
}

func TestApp_AddFTP(t *testing.T) {
	t.Run("Adding FTP", func(t *testing.T) {
		testutil.NewServerConfigs(t)

		app := New()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := file.NewMockFileSystemProvider(ctrl)

		mock.EXPECT().UseLogger(app.Logger())
		mock.EXPECT().UseMetrics(app.Metrics())
		mock.EXPECT().Connect()

		app.AddFTP(mock)

		assert.Equal(t, mock, app.container.File)
	})

	t.Run("Adding FTP", func(t *testing.T) {
		testutil.NewServerConfigs(t)

		app := New()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := file.NewMockFileSystemProvider(ctrl)

		mock.EXPECT().UseLogger(app.Logger())
		mock.EXPECT().UseMetrics(app.Metrics())
		mock.EXPECT().Connect()

		app.AddFileStore(mock)

		assert.Equal(t, mock, app.container.File)
	})
}

func TestApp_AddS3(t *testing.T) {
	t.Run("Adding S3", func(t *testing.T) {
		testutil.NewServerConfigs(t)

		app := New()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := file.NewMockFileSystemProvider(ctrl)

		mock.EXPECT().UseLogger(app.Logger())
		mock.EXPECT().UseMetrics(app.Metrics())
		mock.EXPECT().Connect()

		app.AddFileStore(mock)

		assert.Equal(t, mock, app.container.File)
	})
}

func TestApp_AddOpenTSDB(t *testing.T) {
	t.Run("Adding OpenTSDB", func(t *testing.T) {
		testutil.NewServerConfigs(t)

		app := New()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := infra.NewMockOpenTSDBProvider(ctrl)

		mock.EXPECT().UseLogger(app.Logger())
		mock.EXPECT().UseMetrics(app.Metrics())
		mock.EXPECT().UseTracer(gomock.Any())
		mock.EXPECT().Connect()

		app.AddOpenTSDB(mock)

		assert.Equal(t, mock, app.container.OpenTSDB)
	})
}

func TestApp_AddScyllaDB(t *testing.T) {
	t.Run("Adding ScyllaDB", func(t *testing.T) {
		testutil.NewServerConfigs(t)

		app := New()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := infra.NewMockScyllaDBProvider(ctrl)

		mock.EXPECT().UseLogger(app.Logger())
		mock.EXPECT().UseMetrics(app.Metrics())
		mock.EXPECT().UseTracer(gomock.Any())
		mock.EXPECT().Connect()

		app.AddScyllaDB(mock)

		assert.Equal(t, mock, app.container.ScyllaDB)
	})
}

func TestApp_AddArangoDB(t *testing.T) {
	t.Run("Adding ArangoDB", func(t *testing.T) {
		port := testutil.GetFreePort(t)
		t.Setenv("METRICS_PORT", strconv.Itoa(port))

		app := New()

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mock := infra.NewMockArangoDBProvider(ctrl)

		mock.EXPECT().UseLogger(app.Logger())
		mock.EXPECT().UseMetrics(app.Metrics())
		mock.EXPECT().UseTracer(otel.GetTracerProvider().Tracer("pi-arangodb"))
		mock.EXPECT().Connect()

		app.AddArangoDB(mock)

		assert.Equal(t, mock, app.container.ArangoDB)
	})
}
