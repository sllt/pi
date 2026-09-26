package migration

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/sortkeys"

	"github.com/sllt/pi/pkg/pi/infra"
)

type PlanAction string

const (
	PlanApply PlanAction = "apply"
	PlanSkip  PlanAction = "skip"
	PlanDown  PlanAction = "down"
	PlanError PlanAction = "error"
)

// PlanItem describes the decision for one migration version.
type PlanItem struct {
	Version int64
	Name    string
	Action  PlanAction
	Reason  string
}

// PlanResult describes what a migration run would do without executing user migration functions.
type PlanResult struct {
	Direction    Direction
	StateSource  StateSource
	StatePrecise bool
	Items        []PlanItem
}

// StatusResult describes current migration state from the configured datasource and the given migration map.
type StatusResult struct {
	StateSource  StateSource
	StatePrecise bool
	Applied      []VersionResult
	Pending      []VersionResult
	Gaps         []int64
}

type migrationState struct {
	Records    []Record
	Applied    map[int64]Record
	MaxApplied int64
	Source     StateSource
	Precise    bool
}

type migrationStateReader struct {
	migrator migrator
	source   StateSource
	precise  bool
}

// Plan builds a migration plan without executing user migration functions. It may
// initialize the authoritative migration state store when it does not exist yet.
func Plan(ctx context.Context, migrationsMap map[int64]Migrate, c *infra.Container, opts ...Option) (PlanResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	options := applyOptions(opts)
	plan := PlanResult{Direction: options.Direction}
	if err := validateOptions(options); err != nil {
		return plan, err
	}
	if options.Direction != DirectionUp {
		return plan, fmt.Errorf("%w: direction %q is not implemented", ErrDownNotDefined, options.Direction)
	}

	invalidKeys, keys := getKeys(migrationsMap)
	if len(invalidKeys) > 0 {
		return plan, fmt.Errorf("%w: UP not defined for the following keys: %v", ErrInvalidMigration, invalidKeys)
	}
	sortkeys.Int64s(keys)

	state, err := currentMigrationState(ctx, c, migrationsMap)
	if err != nil {
		return plan, err
	}
	plan.StateSource = state.Source
	plan.StatePrecise = state.Precise
	gapSet := makeVersionSet(state.detectGaps(keys, options.Target))

	for _, version := range keys {
		migrationDef := migrationsMap[version]
		item := PlanItem{Version: version, Name: migrationDef.displayName(version)}

		switch {
		case options.Target > 0 && version > options.Target:
			item.Action = PlanSkip
			item.Reason = string(SkipAboveTarget)
		case gapSet[version]:
			item.Action = PlanError
			item.Reason = ErrMigrationGap.Error()
		case state.shouldSkipAsApplied(version):
			item.Action = PlanSkip
			item.Reason = string(SkipAlreadyApplied)
		default:
			item.Action = PlanApply
			item.Reason = "pending"
		}

		plan.Items = append(plan.Items, item)
	}

	return plan, nil
}

// Status returns an applied/pending summary without executing user migration functions.
// It may initialize the authoritative migration state store when it does not exist yet.
func Status(ctx context.Context, migrationsMap map[int64]Migrate, c *infra.Container, opts ...Option) (StatusResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	options := applyOptions(opts)
	if err := validateOptions(options); err != nil {
		return StatusResult{}, err
	}
	if options.Direction != DirectionUp {
		return StatusResult{}, fmt.Errorf("%w: direction %q is not implemented", ErrDownNotDefined, options.Direction)
	}

	invalidKeys, keys := getKeys(migrationsMap)
	if len(invalidKeys) > 0 {
		return StatusResult{}, fmt.Errorf("%w: UP not defined for the following keys: %v", ErrInvalidMigration, invalidKeys)
	}
	sortkeys.Int64s(keys)

	state, err := currentMigrationState(ctx, c, migrationsMap)
	if err != nil {
		return StatusResult{}, err
	}

	status := StatusResult{StateSource: state.Source, StatePrecise: state.Precise}
	for _, record := range state.Records {
		if options.Target > 0 && record.Version > options.Target {
			continue
		}

		status.Applied = append(status.Applied, state.recordResult(record, migrationsMap, SkipAlreadyApplied))
	}

	for _, version := range keys {
		if options.Target > 0 && version > options.Target {
			continue
		}
		if state.shouldSkipAsApplied(version) {
			continue
		}

		migrationDef := migrationsMap[version]
		status.Pending = append(status.Pending, VersionResult{Version: version, Name: migrationDef.displayName(version)})
	}

	status.Gaps = state.detectGaps(keys, options.Target)

	return status, nil
}

func currentMigrationState(ctx context.Context, c *infra.Container, migrationsMap map[int64]Migrate) (migrationState, error) {
	if err := ctx.Err(); err != nil {
		return migrationState{}, err
	}

	_, mg, ok := getMigrator(c)
	if !ok {
		return migrationState{}, ErrNoDatasource
	}

	reader := selectMigrationStateReader(c, mg)
	if err := reader.migrator.checkAndCreateMigrationTable(ctx, c); err != nil {
		return migrationState{}, fmt.Errorf("migration: ensure state store: %w", err)
	}

	return readMigrationState(ctx, c, reader, migrationsMap)
}

func selectMigrationStateReader(c *infra.Container, fallback migrator) migrationStateReader {
	base := &Datasource{}
	if c != nil && !isNil(c.SQL) {
		return migrationStateReader{
			migrator: (&sqlDS{SQL: c.SQL}).apply(base),
			source:   StateSourceSQL,
			precise:  true,
		}
	}

	if c != nil && !isNil(c.Redis) {
		return migrationStateReader{
			migrator: redisDS{Redis: c.Redis}.apply(base),
			source:   StateSourceRedis,
			precise:  true,
		}
	}

	return migrationStateReader{
		migrator: fallback,
		source:   StateSourceLegacy,
		precise:  false,
	}
}

func readMigrationState(
	ctx context.Context,
	c *infra.Container,
	reader migrationStateReader,
	migrationsMap map[int64]Migrate,
) (migrationState, error) {
	records, err := reader.migrator.listApplied(ctx, c)
	if err != nil {
		return migrationState{}, fmt.Errorf("migration: list applied state: %w", err)
	}

	lastMigration := maxRecordVersion(records)
	if !reader.precise {
		lastMigration, err = reader.migrator.getLastMigration(ctx, c)
		if err != nil {
			return migrationState{}, fmt.Errorf("migration: read legacy migration state: %w", err)
		}
	}

	return newMigrationState(records, lastMigration, migrationsMap, reader.source, reader.precise), nil
}

func newMigrationState(
	records []Record,
	lastMigration int64,
	migrationsMap map[int64]Migrate,
	source StateSource,
	precise bool,
) migrationState {
	state := migrationState{Source: source, Precise: precise}
	if !state.Precise && lastMigration > 0 {
		records = mergeAppliedRecords(records, legacyRecordsFromLast(lastMigration, migrationsMap))
	}

	state.Records = mergeAppliedRecords(records)
	state.Applied = make(map[int64]Record, len(state.Records))
	for _, record := range state.Records {
		state.Applied[record.Version] = record
		if record.Version > state.MaxApplied {
			state.MaxApplied = record.Version
		}
	}
	if lastMigration > state.MaxApplied {
		state.MaxApplied = lastMigration
	}

	return state
}

func legacyRecordsFromLast(lastMigration int64, migrationsMap map[int64]Migrate) []Record {
	if lastMigration <= 0 {
		return nil
	}

	keys := make([]int64, 0, len(migrationsMap))
	for version := range migrationsMap {
		if version <= lastMigration {
			keys = append(keys, version)
		}
	}
	if len(keys) == 0 {
		return []Record{{Version: lastMigration, Method: "UP"}}
	}

	sortkeys.Int64s(keys)
	records := make([]Record, 0, len(keys))
	for _, version := range keys {
		records = append(records, Record{Version: version, Method: "UP"})
	}

	return records
}

func maxRecordVersion(records []Record) int64 {
	var maxApplied int64
	for _, record := range records {
		if record.Version > maxApplied {
			maxApplied = record.Version
		}
	}

	return maxApplied
}

func mergeAppliedRecords(groups ...[]Record) []Record {
	byVersion := make(map[int64]Record)
	for _, records := range groups {
		for _, record := range records {
			if record.Version == 0 {
				continue
			}
			if _, exists := byVersion[record.Version]; !exists {
				byVersion[record.Version] = record
			}
		}
	}

	keys := make([]int64, 0, len(byVersion))
	for version := range byVersion {
		keys = append(keys, version)
	}
	sortkeys.Int64s(keys)

	merged := make([]Record, 0, len(keys))
	for _, version := range keys {
		merged = append(merged, byVersion[version])
	}

	return merged
}

func (s migrationState) isApplied(version int64) bool {
	_, ok := s.Applied[version]

	return ok
}

func (s migrationState) shouldSkipAsApplied(version int64) bool {
	if s.isApplied(version) {
		return true
	}

	return !s.Precise && version <= s.MaxApplied
}

func (s migrationState) detectGaps(keys []int64, target int64) []int64 {
	if !s.Precise || s.MaxApplied == 0 {
		return nil
	}

	gaps := make([]int64, 0)
	for _, version := range keys {
		if target > 0 && version > target {
			break
		}
		if version >= s.MaxApplied {
			break
		}
		if !s.isApplied(version) {
			gaps = append(gaps, version)
		}
	}

	return gaps
}

func (s migrationState) recordResult(record Record, migrationsMap map[int64]Migrate, reason SkipReason) VersionResult {
	name := record.Name
	if migrationDef, ok := migrationsMap[record.Version]; ok {
		name = migrationDef.displayName(record.Version)
	}
	if name == "" {
		name = fmt.Sprintf("%d", record.Version)
	}

	return VersionResult{Version: record.Version, Name: name, Reason: reason, Duration: record.Duration}
}

func makeVersionSet(versions []int64) map[int64]bool {
	set := make(map[int64]bool, len(versions))
	for _, version := range versions {
		set[version] = true
	}

	return set
}

func newDryRunResultFromPlan(plan PlanResult) Result {
	now := time.Now()
	result := Result{
		Direction:    plan.Direction,
		StateSource:  plan.StateSource,
		StatePrecise: plan.StatePrecise,
		StartedAt:    now,
		FinishedAt:   now,
	}
	for _, item := range plan.Items {
		vr := VersionResult{Version: item.Version, Name: item.Name}
		if item.Action == PlanApply {
			vr.Reason = SkipDryRun
			result.Skipped = append(result.Skipped, vr)
			continue
		}
		if item.Action == PlanSkip {
			vr.Reason = SkipReason(item.Reason)
			result.Skipped = append(result.Skipped, vr)
		}
	}

	return result
}
