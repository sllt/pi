package migration

import (
	"errors"
	"fmt"
	"time"
)

// Direction describes which direction a migration run is moving.
type Direction string

const (
	DirectionUp   Direction = "up"
	DirectionDown Direction = "down"
)

// SkipReason explains why a migration version did not execute.
type SkipReason string

const (
	SkipAlreadyApplied SkipReason = "already_applied"
	SkipAboveTarget    SkipReason = "above_target"
	SkipBelowTarget    SkipReason = "below_target"
	SkipDryRun         SkipReason = "dry_run"
)

// StateSource identifies the datasource used as the authoritative migration state store.
// SQL is preferred, Redis is used when SQL is unavailable, and legacy means the runtime
// can only infer state from the highest version reported by the configured datasource chain.
type StateSource string

const (
	StateSourceSQL    StateSource = "sql"
	StateSourceRedis  StateSource = "redis"
	StateSourceLegacy StateSource = "legacy"
)

// Result is the structured outcome of a migration run.
type Result struct {
	Direction    Direction
	StateSource  StateSource
	StatePrecise bool
	Applied      []VersionResult
	Skipped      []VersionResult
	Failed       *VersionResult
	StartedAt    time.Time
	FinishedAt   time.Time
}

// VersionResult describes what happened to a single migration version.
type VersionResult struct {
	Version  int64
	Name     string
	Reason   SkipReason
	Duration time.Duration
	Error    error
}

// Record describes an applied migration state record read from a datasource.
type Record struct {
	Version   int64
	Name      string
	Method    string
	StartedAt time.Time
	Duration  time.Duration
}

// AppliedVersions returns only the applied migration version numbers.
func (r Result) AppliedVersions() []int64 {
	versions := make([]int64, 0, len(r.Applied))
	for _, applied := range r.Applied {
		versions = append(versions, applied.Version)
	}

	return versions
}

// SkippedVersions returns only the skipped migration version numbers.
func (r Result) SkippedVersions() []int64 {
	versions := make([]int64, 0, len(r.Skipped))
	for _, skipped := range r.Skipped {
		versions = append(versions, skipped.Version)
	}

	return versions
}

// Duration returns the total wall-clock duration of the migration run.
func (r Result) Duration() time.Duration {
	if r.StartedAt.IsZero() || r.FinishedAt.IsZero() {
		return 0
	}

	return r.FinishedAt.Sub(r.StartedAt)
}

var (
	ErrNoDatasource             = errors.New("migration: no datasource initialized")
	ErrInvalidMigration         = errors.New("migration: invalid migration")
	ErrMigrationFailed          = errors.New("migration: migration failed")
	ErrMigrationPanic           = errors.New("migration: panic")
	ErrMigrationLocked          = errors.New("migration: locked")
	ErrMigrationLockUnavailable = errors.New("migration: lock unavailable")
	ErrMigrationGap             = errors.New("migration: gap detected")
	ErrDownNotDefined           = errors.New("migration: down not defined")
	ErrInvalidOption            = errors.New("migration: invalid option")
	ErrUnsupportedSQLDialect    = errors.New("migration: unsupported SQL dialect")
)

// VersionError attaches migration version context to a lower-level error.
type VersionError struct {
	Version int64
	Name    string
	Op      string
	Err     error
}

func (e *VersionError) Error() string {
	if e == nil {
		return ""
	}

	name := e.Name
	if name == "" {
		name = fmt.Sprintf("%d", e.Version)
	}

	if e.Op == "" {
		return fmt.Sprintf("migration %s failed: %v", name, e.Err)
	}

	return fmt.Sprintf("migration %s failed during %s: %v", name, e.Op, e.Err)
}

func (e *VersionError) Unwrap() error {
	if e == nil {
		return nil
	}

	return e.Err
}

type LockMode string

const (
	LockDefault  LockMode = "default"
	LockEnabled  LockMode = "enabled"
	LockDisabled LockMode = "disabled"
)

// Option configures a migration run.
type Option func(*Options)

// Options holds migration run options.
type Options struct {
	Direction Direction
	Target    int64
	DryRun    bool
	Lock      LockMode
	LockTTL   time.Duration
}

func defaultOptions() Options {
	return Options{Direction: DirectionUp, Lock: LockDefault, LockTTL: migrationLockTTL}
}

func applyOptions(opts []Option) Options {
	options := defaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}

	if options.Direction == "" {
		options.Direction = DirectionUp
	}

	return options
}

func validateOptions(options Options) error {
	if options.Direction != DirectionUp && options.Direction != DirectionDown {
		return fmt.Errorf("%w: unknown direction %q", ErrInvalidOption, options.Direction)
	}
	if options.Target < 0 {
		return fmt.Errorf("%w: target must be greater than or equal to zero", ErrInvalidOption)
	}

	switch options.Lock {
	case LockDefault, LockEnabled, LockDisabled:
	default:
		return fmt.Errorf("%w: unknown lock mode %q", ErrInvalidOption, options.Lock)
	}

	if options.Lock == LockEnabled && options.LockTTL <= 0 {
		return fmt.Errorf("%w: lock TTL must be greater than zero", ErrInvalidOption)
	}

	return nil
}

// WithTarget limits the migration run to the given target version.
func WithTarget(version int64) Option {
	return func(o *Options) {
		o.Target = version
	}
}

// WithDryRun builds a plan-like Result without executing user migration functions.
func WithDryRun() Option {
	return func(o *Options) {
		o.DryRun = true
	}
}

// WithLock asks the migration runtime to acquire a migration lock before running.
func WithLock() Option {
	return func(o *Options) {
		o.Lock = LockEnabled
	}
}

// WithLockTTL configures the migration lock lease duration. The runtime does not renew
// the lease, so it must be longer than the longest expected migration run. This option
// only has an effect when locking is enabled with WithLock.
func WithLockTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.LockTTL = ttl
	}
}

// WithoutLock disables migration locking.
func WithoutLock() Option {
	return func(o *Options) {
		o.Lock = LockDisabled
	}
}

// WithDirection changes the migration direction.
func WithDirection(direction Direction) Option {
	return func(o *Options) {
		o.Direction = direction
	}
}
