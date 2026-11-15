package migration

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	goRedis "github.com/redis/go-redis/v9"

	"github.com/sllt/pi/pkg/pi/datasource"
	piRedis "github.com/sllt/pi/pkg/pi/datasource/redis"
	piSql "github.com/sllt/pi/pkg/pi/datasource/sql"
)

var (
	ErrAdapterNotFound         = datasource.ErrAdapterNotFound
	ErrAmbiguousAdapter        = datasource.ErrAmbiguousAdapter
	ErrInvalidAdapterType      = datasource.ErrInvalidAdapterType
	ErrTransactionalDatasource = datasource.ErrTransactionalDatasource
)

// Target provides the runtime dependencies needed to execute, plan, or inspect migrations.
// Both *pi.App and migration.Deps implement Target.
type Target interface {
	MigrationDeps() Deps
}

// NamedCandidate pairs a registered adapter's name with its runtime instance.
type NamedCandidate struct {
	Name  string
	Value any
}

// AdapterLookup bridges external adapters mounted on *pi.App into migration scripts
// without introducing a package import cycle between pkg/pi and pkg/pi/migration.
type AdapterLookup interface {
	LookupExternalType(target reflect.Type) ([]NamedCandidate, error)
	LookupExternalNamed(name string, target reflect.Type) (any, error)
}

// SQLSource is the SQL pool contract required by the migration engine.
type SQLSource interface {
	SQL
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Dialect() string
	BeginTxContext(ctx context.Context, opts *sql.TxOptions) (*piSql.Tx, error)
}

// RedisSource is the Redis client contract required by the migration engine.
type RedisSource interface {
	Redis
	HGetAll(ctx context.Context, key string) *goRedis.MapStringStringCmd
	TxPipeline() goRedis.Pipeliner
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *goRedis.BoolCmd
	Eval(ctx context.Context, script string, keys []string, args ...any) *goRedis.Cmd
}

// PubSubSource is the Pub/Sub client contract required by the migration engine.
type PubSubSource interface {
	PubSub
	Publish(ctx context.Context, name string, message []byte) error
}

// Deps bundles the active datasources, logger, and optional external adapter registry
// used by Run, Plan, and Status.
type Deps struct {
	Logger   Logger
	SQL      SQLSource
	Redis    RedisSource
	PubSub   PubSubSource
	Adapters AdapterLookup
}

// MigrationDeps implements Target so callers and tests can pass Deps directly.
func (d Deps) MigrationDeps() Deps {
	return d
}

func (d *Deps) log() Logger {
	if d == nil || isNil(d.Logger) {
		return nopLogger{}
	}

	return d.Logger
}

func resolveDeps(target Target) (*Deps, bool) {
	if isNil(target) {
		return nil, false
	}

	deps := target.MigrationDeps()
	if isNil(deps.Logger) {
		deps.Logger = nopLogger{}
	}
	if isNil(deps.SQL) {
		deps.SQL = nil
	}
	if isNil(deps.Redis) {
		deps.Redis = nil
	}
	if isNil(deps.PubSub) {
		deps.PubSub = nil
	}
	if isNil(deps.Adapters) {
		deps.Adapters = nil
	}

	hasDatasource := deps.SQL != nil || deps.Redis != nil || deps.PubSub != nil

	return &deps, hasDatasource
}

type nopLogger struct{}

func (nopLogger) Debug(...any)           {}
func (nopLogger) Debugf(string, ...any)  {}
func (nopLogger) Info(...any)            {}
func (nopLogger) Infof(string, ...any)   {}
func (nopLogger) Notice(...any)          {}
func (nopLogger) Noticef(string, ...any) {}
func (nopLogger) Warn(...any)            {}
func (nopLogger) Warnf(string, ...any)   {}
func (nopLogger) Error(...any)           {}
func (nopLogger) Errorf(string, ...any)  {}
func (nopLogger) Fatal(...any)           {}
func (nopLogger) Fatalf(string, ...any)  {}

var (
	sqlClientType   = reflect.TypeFor[piSql.Client]()
	sqlSourceType   = reflect.TypeFor[SQLSource]()
	sqlDBType       = reflect.TypeFor[*piSql.DB]()
	sqlHandleType   = reflect.TypeFor[*piSql.Handle]()
	redisClientType = reflect.TypeFor[piRedis.Client]()
	redisSourceType = reflect.TypeFor[RedisSource]()
	redisImplType   = reflect.TypeFor[*piRedis.Redis]()
	redisHandleType = reflect.TypeFor[*piRedis.Handle]()
	goRedisType     = reflect.TypeFor[*goRedis.Client]()
)

func isSQLPoolType(t reflect.Type) bool {
	switch t {
	case sqlClientType, sqlSourceType, sqlDBType, sqlHandleType:
		return true
	default:
		return false
	}
}

func isRedisPoolType(t reflect.Type) bool {
	switch t {
	case redisClientType, redisSourceType, redisImplType, redisHandleType, goRedisType:
		return true
	default:
		return false
	}
}

func matchesTargetType(val any, target reflect.Type) bool {
	if isNil(val) || target == nil {
		return false
	}

	vt := reflect.TypeOf(val)
	if target.Kind() == reflect.Interface {
		return vt.Implements(target)
	}

	return vt == target
}

// Use retrieves a datasource or mounted external adapter of type T from d in a transaction-safe manner.
func Use[T any](d Datasource) (T, bool) {
	v, err := Resolve[T](d)
	if err != nil {
		var zero T
		return zero, false
	}

	return v, true
}

// MustUse retrieves a datasource or mounted external adapter of type T from d, or panics on error.
func MustUse[T any](d Datasource) T {
	v, err := Resolve[T](d)
	if err != nil {
		panic(err)
	}

	return v
}

// Resolve retrieves a datasource or mounted external adapter of type T from d.
// Requesting a non-transactional primary SQL or Redis pool while a migration transaction/pipeline
// is active returns ErrTransactionalDatasource to prevent bypassing the active transaction or deadlocking single-connection pools.
func Resolve[T any](d Datasource) (T, error) {
	var zero T
	targetType := reflect.TypeFor[T]()
	if targetType.Kind() == reflect.Interface && targetType.NumMethod() == 0 {
		return zero, fmt.Errorf("%w: empty interface any is not allowed", ErrInvalidAdapterType)
	}

	if !isNil(d.SQL) && isSQLPoolType(targetType) {
		return zero, fmt.Errorf("%w: primary SQL migration runs inside a transaction; use d.SQL instead of %s", ErrTransactionalDatasource, targetType)
	}
	if !isNil(d.Redis) && isRedisPoolType(targetType) {
		return zero, fmt.Errorf("%w: primary Redis migration runs inside a pipeline; use d.Redis instead of %s", ErrTransactionalDatasource, targetType)
	}

	var candidates []NamedCandidate
	if matchesTargetType(d.SQL, targetType) {
		candidates = append(candidates, NamedCandidate{Name: "sql", Value: d.SQL})
	}
	if matchesTargetType(d.Redis, targetType) {
		candidates = append(candidates, NamedCandidate{Name: "redis", Value: d.Redis})
	}
	if matchesTargetType(d.PubSub, targetType) {
		candidates = append(candidates, NamedCandidate{Name: "pubsub", Value: d.PubSub})
	}

	if !isNil(d.adapters) {
		ext, err := d.adapters.LookupExternalType(targetType)
		if err != nil {
			return zero, err
		}
		candidates = append(candidates, ext...)
	}

	if len(candidates) == 0 {
		if targetType.Kind() == reflect.Struct {
			ptrType := reflect.PointerTo(targetType)
			if hasPointerCandidate(d, ptrType) {
				return zero, fmt.Errorf("%w: %s is a struct value; did you mean *%s?", ErrInvalidAdapterType, targetType, targetType)
			}
		}

		return zero, fmt.Errorf("%w: no adapter found for %s", ErrAdapterNotFound, targetType)
	}

	if len(candidates) > 1 {
		names := make([]string, len(candidates))
		for i, c := range candidates {
			names[i] = c.Name
		}
		sort.Strings(names)

		return zero, fmt.Errorf("%w: multiple adapters match %s: %v", ErrAmbiguousAdapter, targetType, names)
	}

	val, ok := candidates[0].Value.(T)
	if !ok {
		return zero, fmt.Errorf("%w: adapter %q (%T) does not satisfy %s", ErrInvalidAdapterType, candidates[0].Name, candidates[0].Value, targetType)
	}

	return val, nil
}

func hasPointerCandidate(d Datasource, ptrType reflect.Type) bool {
	if (!isNil(d.SQL) && isSQLPoolType(ptrType)) || (!isNil(d.Redis) && isRedisPoolType(ptrType)) {
		return true
	}
	if matchesTargetType(d.SQL, ptrType) || matchesTargetType(d.Redis, ptrType) || matchesTargetType(d.PubSub, ptrType) {
		return true
	}
	if !isNil(d.adapters) {
		ext, _ := d.adapters.LookupExternalType(ptrType)
		if len(ext) > 0 {
			return true
		}
	}

	return false
}

// UseNamed retrieves a named datasource or mounted external adapter of type T from d.
func UseNamed[T any](d Datasource, name string) (T, bool) {
	v, err := ResolveNamed[T](d, name)
	if err != nil {
		var zero T
		return zero, false
	}

	return v, true
}

// MustUseNamed retrieves a named datasource or mounted external adapter of type T from d, or panics on error.
func MustUseNamed[T any](d Datasource, name string) T {
	v, err := ResolveNamed[T](d, name)
	if err != nil {
		panic(err)
	}

	return v
}

// ResolveNamed retrieves a named datasource or mounted external adapter of type T from d.
func ResolveNamed[T any](d Datasource, name string) (T, error) {
	var zero T
	targetType := reflect.TypeFor[T]()
	if targetType.Kind() == reflect.Interface && targetType.NumMethod() == 0 {
		return zero, fmt.Errorf("%w: empty interface any is not allowed", ErrInvalidAdapterType)
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return zero, fmt.Errorf("%w: empty adapter name", ErrAdapterNotFound)
	}

	switch name {
	case "sql":
		if isNil(d.SQL) {
			return zero, fmt.Errorf("%w: %q", ErrAdapterNotFound, name)
		}
		if isSQLPoolType(targetType) {
			return zero, fmt.Errorf("%w: primary SQL migration runs inside a transaction; use d.SQL instead of %s", ErrTransactionalDatasource, targetType)
		}
		v, ok := d.SQL.(T)
		if !ok {
			return zero, fmt.Errorf("%w: adapter %q (%T) does not implement %s", ErrInvalidAdapterType, name, d.SQL, targetType)
		}

		return v, nil

	case "redis":
		if isNil(d.Redis) {
			return zero, fmt.Errorf("%w: %q", ErrAdapterNotFound, name)
		}
		if isRedisPoolType(targetType) {
			return zero, fmt.Errorf("%w: primary Redis migration runs inside a pipeline; use d.Redis instead of %s", ErrTransactionalDatasource, targetType)
		}
		v, ok := d.Redis.(T)
		if !ok {
			return zero, fmt.Errorf("%w: adapter %q (%T) does not implement %s", ErrInvalidAdapterType, name, d.Redis, targetType)
		}

		return v, nil

	case "pubsub":
		if isNil(d.PubSub) {
			return zero, fmt.Errorf("%w: %q", ErrAdapterNotFound, name)
		}
		v, ok := d.PubSub.(T)
		if !ok {
			return zero, fmt.Errorf("%w: adapter %q (%T) does not implement %s", ErrInvalidAdapterType, name, d.PubSub, targetType)
		}

		return v, nil

	default:
		if isNil(d.adapters) {
			return zero, fmt.Errorf("%w: %q", ErrAdapterNotFound, name)
		}
		raw, err := d.adapters.LookupExternalNamed(name, targetType)
		if err != nil {
			return zero, err
		}
		v, ok := raw.(T)
		if !ok {
			return zero, fmt.Errorf("%w: adapter %q (%T) does not implement %s", ErrInvalidAdapterType, name, raw, targetType)
		}

		return v, nil
	}
}
