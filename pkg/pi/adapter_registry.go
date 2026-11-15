package pi

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/sllt/pi/pkg/pi/datasource/file"
	"github.com/sllt/pi/pkg/pi/migration"
)

var (
	fileSystemIfaceType         = reflect.TypeFor[file.FileSystem]()
	fileSystemProviderIfaceType = reflect.TypeFor[file.FileSystemProvider]()
)

// ProviderSource is implemented by *App and *Context to enable type-safe adapter retrieval.
type ProviderSource interface {
	runtimeKernelRef() *runtimeKernel
}

type adapterEntry struct {
	name         string
	instance     any
	concreteType reflect.Type
	isCore       bool
	isDefaultFS  bool
}

type cachedIfaceLookup struct {
	entry adapterEntry
	err   error
}

type adapterRegistry struct {
	mu             sync.RWMutex
	frozen         atomic.Bool
	entries        []adapterEntry
	byName         map[string]int
	byConcreteType map[reflect.Type][]int
	ifaceCache     sync.Map // reflect.Type -> cachedIfaceLookup (populated only after freeze)
}

func newAdapterRegistry() *adapterRegistry {
	return &adapterRegistry{
		byName:         make(map[string]int),
		byConcreteType: make(map[reflect.Type][]int),
	}
}

func (r *adapterRegistry) freeze() {
	if r == nil || r.frozen.Load() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen.Load() {
		return
	}
	r.rebuildConcreteIndexLocked()
	r.frozen.Store(true)
}

func (r *adapterRegistry) rebuildConcreteIndexLocked() {
	idxMap := make(map[reflect.Type][]int, len(r.entries))
	for i, e := range r.entries {
		if e.instance == nil || e.concreteType == nil {
			continue
		}
		idxMap[e.concreteType] = append(idxMap[e.concreteType], i)
	}
	r.byConcreteType = idxMap
}

func (r *adapterRegistry) upsertCore(name string, instance any, isDefaultFS bool) {
	if r == nil || r.frozen.Load() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen.Load() {
		return
	}

	if isNilValue(instance) {
		if idx, ok := r.byName[name]; ok {
			r.entries = append(r.entries[:idx], r.entries[idx+1:]...)
			r.byName = make(map[string]int, len(r.entries))
			for i, e := range r.entries {
				r.byName[e.name] = i
			}
			r.rebuildConcreteIndexLocked()
		}
		return
	}

	entry := adapterEntry{
		name:         name,
		instance:     instance,
		concreteType: reflect.TypeOf(instance),
		isCore:       true,
		isDefaultFS:  isDefaultFS,
	}
	if idx, ok := r.byName[name]; ok {
		r.entries[idx] = entry
	} else {
		r.byName[name] = len(r.entries)
		r.entries = append(r.entries, entry)
	}
	r.rebuildConcreteIndexLocked()
}

func (r *adapterRegistry) upsertAdapter(name string, instance any) {
	if r == nil || isNilValue(instance) || r.frozen.Load() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen.Load() {
		return
	}

	entry := adapterEntry{
		name:         name,
		instance:     instance,
		concreteType: reflect.TypeOf(instance),
		isCore:       false,
		isDefaultFS:  false,
	}
	if idx, ok := r.byName[name]; ok {
		r.entries[idx] = entry
	} else {
		r.byName[name] = len(r.entries)
		r.entries = append(r.entries, entry)
	}
	r.rebuildConcreteIndexLocked()
}

func isDefaultFSFallbackTarget(targetType reflect.Type) bool {
	return targetType == fileSystemIfaceType || targetType == fileSystemProviderIfaceType
}

func entryMatchesTarget(e adapterEntry, targetType reflect.Type) bool {
	if e.instance == nil || e.concreteType == nil {
		return false
	}
	if targetType.Kind() == reflect.Interface {
		return e.concreteType.Implements(targetType)
	}
	return e.concreteType == targetType
}

func (r *adapterRegistry) resolveType(targetType reflect.Type) (any, error) {
	if targetType == nil || (targetType.Kind() == reflect.Interface && targetType.NumMethod() == 0) {
		return nil, fmt.Errorf("%w: empty interface any is not allowed", ErrInvalidAdapterType)
	}
	if r == nil {
		return nil, fmt.Errorf("%w: type %s", ErrAdapterNotFound, targetType)
	}

	frozen := r.frozen.Load()
	if !frozen {
		r.mu.RLock()
		defer r.mu.RUnlock()
	}

	if targetType.Kind() != reflect.Interface {
		indices := r.byConcreteType[targetType]
		if len(indices) == 1 {
			e := r.entries[indices[0]]
			if !e.isDefaultFS {
				return e.instance, nil
			}
		} else if len(indices) > 1 {
			var matches []adapterEntry
			for _, idx := range indices {
				e := r.entries[idx]
				if !e.isDefaultFS {
					matches = append(matches, e)
				}
			}
			if len(matches) == 1 {
				return matches[0].instance, nil
			}
			if len(matches) > 1 {
				names := make([]string, len(matches))
				for i, m := range matches {
					names[i] = m.name
				}
				return nil, fmt.Errorf("%w: multiple adapters match %s: [%s]",
					ErrAmbiguousAdapter, targetType, strings.Join(names, ", "))
			}
		}

		// Friendly struct value -> pointer hint when *T is registered (ignoring implicit default local FS).
		if targetType.Kind() == reflect.Struct {
			ptrType := reflect.PointerTo(targetType)
			for _, idx := range r.byConcreteType[ptrType] {
				if !r.entries[idx].isDefaultFS {
					return nil, fmt.Errorf("%w: %s is a value type; did you mean *%s?",
						ErrInvalidAdapterType, targetType, targetType)
				}
			}
		}
		return nil, fmt.Errorf("%w: no adapter registered for type %s", ErrAdapterNotFound, targetType)
	}

	if frozen {
		if cached, ok := r.ifaceCache.Load(targetType); ok {
			res := cached.(cachedIfaceLookup)
			if res.err != nil {
				return nil, res.err
			}
			return res.entry.instance, nil
		}
	}

	var (
		matches      []adapterEntry
		defaultFSMat *adapterEntry
	)
	for i := range r.entries {
		e := r.entries[i]
		if !entryMatchesTarget(e, targetType) {
			continue
		}
		if e.isDefaultFS {
			eCopy := e
			defaultFSMat = &eCopy
			continue
		}
		matches = append(matches, e)
	}

	var (
		chosen adapterEntry
		err    error
	)
	switch {
	case len(matches) == 1:
		chosen = matches[0]
	case len(matches) > 1:
		names := make([]string, len(matches))
		for i, m := range matches {
			names[i] = m.name
		}
		err = fmt.Errorf("%w: multiple adapters implement %s: [%s]",
			ErrAmbiguousAdapter, targetType, strings.Join(names, ", "))
	case defaultFSMat != nil && isDefaultFSFallbackTarget(targetType):
		chosen = *defaultFSMat
	default:
		err = fmt.Errorf("%w: no adapter implements %s", ErrAdapterNotFound, targetType)
	}

	if frozen {
		r.ifaceCache.Store(targetType, cachedIfaceLookup{entry: chosen, err: err})
	}
	if err != nil {
		return nil, err
	}
	return chosen.instance, nil
}

func (r *adapterRegistry) resolveNamed(name string, targetType reflect.Type) (any, error) {
	if targetType == nil || (targetType.Kind() == reflect.Interface && targetType.NumMethod() == 0) {
		return nil, fmt.Errorf("%w: empty interface any is not allowed", ErrInvalidAdapterType)
	}
	name = strings.TrimSpace(name)
	if name == "" || r == nil {
		return nil, fmt.Errorf("%w: %q", ErrAdapterNotFound, name)
	}

	if !r.frozen.Load() {
		r.mu.RLock()
		defer r.mu.RUnlock()
	}

	idx, ok := r.byName[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrAdapterNotFound, name)
	}
	e := r.entries[idx]
	if entryMatchesTarget(e, targetType) {
		return e.instance, nil
	}
	if targetType.Kind() == reflect.Struct && e.concreteType == reflect.PointerTo(targetType) {
		return nil, fmt.Errorf("%w: adapter %q has type %s; did you mean *%s?",
			ErrInvalidAdapterType, name, e.concreteType, targetType)
	}
	return nil, fmt.Errorf("%w: adapter %q (%s) does not satisfy %s",
		ErrInvalidAdapterType, name, e.concreteType, targetType)
}

func extractKernel(src ProviderSource) *runtimeKernel {
	if src == nil || isNilValue(src) {
		return nil
	}
	return src.runtimeKernelRef()
}

// Resolve retrieves the single registered adapter matching T from *App or *Context.
func Resolve[T any](src ProviderSource) (T, error) {
	var zero T
	targetType := reflect.TypeFor[T]()
	if targetType.Kind() == reflect.Interface && targetType.NumMethod() == 0 {
		return zero, fmt.Errorf("%w: empty interface any is not allowed", ErrInvalidAdapterType)
	}
	k := extractKernel(src)
	if k == nil || k.adapters == nil {
		return zero, fmt.Errorf("%w: type %s", ErrAdapterNotFound, targetType)
	}
	val, err := k.adapters.resolveType(targetType)
	if err != nil {
		return zero, err
	}
	typed, ok := val.(T)
	if !ok {
		return zero, fmt.Errorf("%w: adapter of type %T cannot convert to %s", ErrInvalidAdapterType, val, targetType)
	}
	return typed, nil
}

// Use retrieves the single registered adapter matching T and reports whether it was found without ambiguity.
func Use[T any](src ProviderSource) (T, bool) {
	v, err := Resolve[T](src)
	if err != nil {
		var zero T
		return zero, false
	}
	return v, true
}

// MustUse retrieves the single registered adapter matching T or panics with a descriptive error.
func MustUse[T any](src ProviderSource) T {
	v, err := Resolve[T](src)
	if err != nil {
		panic(err)
	}
	return v
}

// ResolveNamed retrieves the registered adapter with the given name and asserts type T.
func ResolveNamed[T any](src ProviderSource, name string) (T, error) {
	var zero T
	targetType := reflect.TypeFor[T]()
	if targetType.Kind() == reflect.Interface && targetType.NumMethod() == 0 {
		return zero, fmt.Errorf("%w: empty interface any is not allowed", ErrInvalidAdapterType)
	}
	k := extractKernel(src)
	if k == nil || k.adapters == nil {
		return zero, fmt.Errorf("%w: %q", ErrAdapterNotFound, strings.TrimSpace(name))
	}
	val, err := k.adapters.resolveNamed(name, targetType)
	if err != nil {
		return zero, err
	}
	typed, ok := val.(T)
	if !ok {
		return zero, fmt.Errorf("%w: adapter %q (%T) cannot convert to %s", ErrInvalidAdapterType, name, val, targetType)
	}
	return typed, nil
}

// UseNamed retrieves the registered adapter with the given name and reports whether it matched T.
func UseNamed[T any](src ProviderSource, name string) (T, bool) {
	v, err := ResolveNamed[T](src, name)
	if err != nil {
		var zero T
		return zero, false
	}
	return v, true
}

// MustUseNamed retrieves the registered adapter with the given name as T or panics.
func MustUseNamed[T any](src ProviderSource, name string) T {
	v, err := ResolveNamed[T](src, name)
	if err != nil {
		panic(err)
	}
	return v
}

// migrationAdapterLookup adapts adapterRegistry to migration.AdapterLookup while
// excluding the 3 migration-managed core slots {"sql", "redis", "pubsub"}.
type migrationAdapterLookup struct {
	reg *adapterRegistry
}

func isMigrationManagedSlot(name string) bool {
	return name == "sql" || name == "redis" || name == "pubsub"
}

func (m migrationAdapterLookup) LookupExternalType(target reflect.Type) ([]migration.NamedCandidate, error) {
	if target == nil || (target.Kind() == reflect.Interface && target.NumMethod() == 0) {
		return nil, fmt.Errorf("%w: empty interface any is not allowed", ErrInvalidAdapterType)
	}
	if m.reg == nil {
		return nil, nil
	}
	if !m.reg.frozen.Load() {
		m.reg.mu.RLock()
		defer m.reg.mu.RUnlock()
	}

	var (
		candidates   []migration.NamedCandidate
		defaultFSMat *adapterEntry
	)
	for i := range m.reg.entries {
		e := m.reg.entries[i]
		if isMigrationManagedSlot(e.name) {
			continue
		}
		if !entryMatchesTarget(e, target) {
			continue
		}
		if e.isDefaultFS {
			eCopy := e
			defaultFSMat = &eCopy
			continue
		}
		candidates = append(candidates, migration.NamedCandidate{
			Name:  e.name,
			Value: e.instance,
		})
	}
	if len(candidates) == 0 && defaultFSMat != nil && isDefaultFSFallbackTarget(target) {
		candidates = append(candidates, migration.NamedCandidate{
			Name:  defaultFSMat.name,
			Value: defaultFSMat.instance,
		})
	}
	return candidates, nil
}

func (m migrationAdapterLookup) LookupExternalNamed(name string, target reflect.Type) (any, error) {
	if target == nil || (target.Kind() == reflect.Interface && target.NumMethod() == 0) {
		return nil, fmt.Errorf("%w: empty interface any is not allowed", ErrInvalidAdapterType)
	}
	name = strings.TrimSpace(name)
	if name == "" || isMigrationManagedSlot(name) || m.reg == nil {
		return nil, fmt.Errorf("%w: %q", ErrAdapterNotFound, name)
	}
	return m.reg.resolveNamed(name, target)
}
