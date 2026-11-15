package migration

import (
	"context"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	goRedis "github.com/redis/go-redis/v9"

	piSql "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/logging"
)

func newTestSQLDeps(t *testing.T, dialect string) (*Deps, *piSql.DB, sqlmock.Sqlmock) {
	t.Helper()
	if dialect == "" {
		dialect = "mysql"
	}
	db, mock, _ := piSql.NewSQLMocksWithConfig(t, &piSql.DBConfig{Dialect: dialect})
	deps := &Deps{
		Logger: logging.NewWriterLogger(logging.INFO, io.Discard, io.Discard),
		SQL:    db,
	}
	return deps, db, mock
}

type fakeMigrator struct {
	checkTableFn func(ctx context.Context, deps *Deps) error
	listFn       func(ctx context.Context, deps *Deps) ([]Record, error)
	lastFn       func(ctx context.Context, deps *Deps) (int64, error)
	beginFn      func(ctx context.Context, deps *Deps) (transactionData, error)
	commitFn     func(ctx context.Context, deps *Deps, data transactionData) error
	rollbackFn   func(ctx context.Context, deps *Deps, data transactionData) error
}

func (f *fakeMigrator) checkAndCreateMigrationTable(ctx context.Context, deps *Deps) error {
	if f != nil && f.checkTableFn != nil {
		return f.checkTableFn(ctx, deps)
	}
	return nil
}

func (f *fakeMigrator) listApplied(ctx context.Context, deps *Deps) ([]Record, error) {
	if f != nil && f.listFn != nil {
		return f.listFn(ctx, deps)
	}
	return nil, nil
}

func (f *fakeMigrator) getLastMigration(ctx context.Context, deps *Deps) (int64, error) {
	if f != nil && f.lastFn != nil {
		return f.lastFn(ctx, deps)
	}
	return 0, nil
}

func (f *fakeMigrator) beginTransaction(ctx context.Context, deps *Deps) (transactionData, error) {
	if f != nil && f.beginFn != nil {
		return f.beginFn(ctx, deps)
	}
	return transactionData{}, nil
}

func (f *fakeMigrator) commitMigration(ctx context.Context, deps *Deps, data transactionData) error {
	if f != nil && f.commitFn != nil {
		return f.commitFn(ctx, deps, data)
	}
	return nil
}

func (f *fakeMigrator) rollback(ctx context.Context, deps *Deps, data transactionData) error {
	if f != nil && f.rollbackFn != nil {
		return f.rollbackFn(ctx, deps, data)
	}
	return nil
}

type fakePubSub struct {
	queryFn       func(ctx context.Context, query string, args ...any) ([]byte, error)
	createTopicFn func(ctx context.Context, name string) error
	deleteTopicFn func(ctx context.Context, name string) error
	publishFn     func(ctx context.Context, name string, message []byte) error
	eventHubName  string
}

func (f *fakePubSub) Query(ctx context.Context, query string, args ...any) ([]byte, error) {
	if f.queryFn != nil {
		return f.queryFn(ctx, query, args...)
	}
	return nil, nil
}

func (f *fakePubSub) CreateTopic(ctx context.Context, name string) error {
	if f.createTopicFn != nil {
		return f.createTopicFn(ctx, name)
	}
	return nil
}

func (f *fakePubSub) DeleteTopic(ctx context.Context, name string) error {
	if f.deleteTopicFn != nil {
		return f.deleteTopicFn(ctx, name)
	}
	return nil
}

func (f *fakePubSub) Publish(ctx context.Context, name string, message []byte) error {
	if f.publishFn != nil {
		return f.publishFn(ctx, name, message)
	}
	return nil
}

func (f *fakePubSub) GetEventHubName() string {
	return f.eventHubName
}

type fakeRedis struct {
	getFn        func(ctx context.Context, key string) *goRedis.StringCmd
	setFn        func(ctx context.Context, key string, value any, expiration time.Duration) *goRedis.StatusCmd
	delFn        func(ctx context.Context, keys ...string) *goRedis.IntCmd
	renameFn     func(ctx context.Context, key, newKey string) *goRedis.StatusCmd
	hgetAllFn    func(ctx context.Context, key string) *goRedis.MapStringStringCmd
	txPipelineFn func() goRedis.Pipeliner
	setNXFn      func(ctx context.Context, key string, value any, expiration time.Duration) *goRedis.BoolCmd
	evalFn       func(ctx context.Context, script string, keys []string, args ...any) *goRedis.Cmd
}

func (f *fakeRedis) Get(ctx context.Context, key string) *goRedis.StringCmd {
	if f.getFn != nil {
		return f.getFn(ctx, key)
	}
	return &goRedis.StringCmd{}
}

func (f *fakeRedis) Set(ctx context.Context, key string, value any, expiration time.Duration) *goRedis.StatusCmd {
	if f.setFn != nil {
		return f.setFn(ctx, key, value, expiration)
	}
	return &goRedis.StatusCmd{}
}

func (f *fakeRedis) Del(ctx context.Context, keys ...string) *goRedis.IntCmd {
	if f.delFn != nil {
		return f.delFn(ctx, keys...)
	}
	return &goRedis.IntCmd{}
}

func (f *fakeRedis) Rename(ctx context.Context, key, newKey string) *goRedis.StatusCmd {
	if f.renameFn != nil {
		return f.renameFn(ctx, key, newKey)
	}
	return &goRedis.StatusCmd{}
}

func (f *fakeRedis) HGetAll(ctx context.Context, key string) *goRedis.MapStringStringCmd {
	if f.hgetAllFn != nil {
		return f.hgetAllFn(ctx, key)
	}
	return goRedis.NewMapStringStringResult(nil, nil)
}

func (f *fakeRedis) TxPipeline() goRedis.Pipeliner {
	if f.txPipelineFn != nil {
		return f.txPipelineFn()
	}
	return nil
}

func (f *fakeRedis) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *goRedis.BoolCmd {
	if f.setNXFn != nil {
		return f.setNXFn(ctx, key, value, expiration)
	}
	return goRedis.NewBoolResult(true, nil)
}

func (f *fakeRedis) Eval(ctx context.Context, script string, keys []string, args ...any) *goRedis.Cmd {
	if f.evalFn != nil {
		return f.evalFn(ctx, script, keys, args...)
	}
	return goRedis.NewCmdResult(int64(1), nil)
}

type fakeAdapterLookup struct {
	items map[string]any
}

func (f *fakeAdapterLookup) LookupExternalType(target reflect.Type) ([]NamedCandidate, error) {
	if f == nil {
		return nil, nil
	}
	var out []NamedCandidate
	for name, inst := range f.items {
		if matchesTargetType(inst, target) {
			out = append(out, NamedCandidate{Name: name, Value: inst})
		}
	}
	return out, nil
}

func (f *fakeAdapterLookup) LookupExternalNamed(name string, target reflect.Type) (any, error) {
	if f == nil {
		return nil, ErrAdapterNotFound
	}
	inst, ok := f.items[name]
	if !ok || isNil(inst) {
		return nil, ErrAdapterNotFound
	}
	if !matchesTargetType(inst, target) {
		return nil, ErrInvalidAdapterType
	}
	return inst, nil
}
