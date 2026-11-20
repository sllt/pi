package pi

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"time"

	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/contract"
	"github.com/sllt/pi/pkg/pi/http/middleware"
)

// Meta is embedded anonymously as the top-level field of a typed request DTO
// to declare route, HTTP method, status code, and OpenAPI metadata via struct tags.
type Meta struct{}

// ErrRoutesFrozen is returned when attempting to register typed routes after
// the HTTP router has already been compiled and frozen.
var ErrRoutesFrozen = errors.New("routes are already compiled and frozen")

// RouteTarget abstracts *App and *RouteGroup as targets for typed route registration.
type RouteTarget interface {
	routeGroupTarget() *RouteGroup
}

func (a *App) routeGroupTarget() *RouteGroup {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return nil
	}
	return &RouteGroup{node: a.httpServer.registry.root, app: a}
}

// TypedBindConfig configures application-wide defaults for typed HTTP request binding.
type TypedBindConfig struct {
	StrictUnknownFields bool
}

// WithTypedBindConfig configures typed request binding defaults on the application.
func WithTypedBindConfig(cfg TypedBindConfig) Option {
	return func(o *buildOptions) error {
		o.typedBind = cfg
		return nil
	}
}

// DocOption configures OpenAPI documentation metadata on a RouteGroup or endpoint.
type DocOption func(*docConfig) error

// DocTags sets the OpenAPI tags inherited by routes in a group or applied to an endpoint.
func DocTags(tags ...string) DocOption {
	return func(cfg *docConfig) error {
		cleaned, err := cleanDocStringList(tags, "OpenAPI tag")
		if err != nil {
			return err
		}
		return applyDocTags(cfg, cleaned)
	}
}

// DocSecurity sets required OpenAPI security scheme names inherited by routes in a group or applied to an endpoint.
func DocSecurity(names ...string) DocOption {
	return func(cfg *docConfig) error {
		if len(names) == 0 {
			return errors.New("DocSecurity requires at least one security scheme name; use DocNoSecurity() to clear security")
		}
		cleaned, err := cleanDocStringList(names, "OpenAPI security scheme name")
		if err != nil {
			return err
		}
		return applyDocSecurity(cfg, cleaned)
	}
}

// DocNoSecurity explicitly clears inherited OpenAPI security requirements for a group or endpoint.
func DocNoSecurity() DocOption {
	return func(cfg *docConfig) error {
		return applyDocSecurity(cfg, []string{})
	}
}

// WithDoc configures OpenAPI documentation attributes on this route group before routes are frozen.
func (g *RouteGroup) WithDoc(options ...DocOption) *RouteGroup {
	if g == nil || g.node == nil {
		return g
	}

	applyFn := func(scope string, current docConfig) (docConfig, error) {
		next := cloneDocConfig(current)
		for _, opt := range options {
			if opt == nil {
				return docConfig{}, fmt.Errorf("%s: DocOption must not be nil", scope)
			}
			var step docConfig
			if err := opt(&step); err != nil {
				return docConfig{}, fmt.Errorf("%s: %w", scope, err)
			}
			if err := mergeDocConfig(&next, step, scope); err != nil {
				return docConfig{}, err
			}
		}
		return next, nil
	}

	if reg := g.registry(); reg != nil {
		reg.mu.Lock()
		defer reg.mu.Unlock()
		if !g.app.canMutateRoutesLocked("configure route group documentation") {
			return g
		}
		scope := groupDocScopeName(g.node)
		next, err := applyFn(scope, g.node.doc)
		if err != nil {
			g.app.recordRouteErrorLocked(err)
			return g
		}
		if !docConfigEqual(g.node.doc, next) {
			g.node.doc = next
			reg.markDirtyLocked()
		}
		return g
	}

	next, err := applyFn(groupDocScopeName(g.node), g.node.doc)
	if err == nil {
		g.node.doc = next
	}
	return g
}

const maxEndpointBodyBytesLimit int64 = 1 << 30 // 1 GiB

type typedEndpointConfig struct {
	timeout        time.Duration
	maxBodyBytes   int64
	piMWs          []PiMiddleware
	httpMWs        []func(http.Handler) http.Handler
	closers        []func() error
	doc            docConfig
	hasEndpointDoc bool
	strictOverride *bool
	hasIdempotency bool
	idempotencyCfg middleware.IdempotencyConfig
	idempotencyPos int
	hasRateLimit   bool
	rateLimitCfg   RateLimitConfig
	rateLimitPos   int
}

// EndpointOption configures per-endpoint runtime and OpenAPI behavior on route registration.
type EndpointOption func(*typedEndpointConfig) error

func withEndpointCloser(fn func() error) EndpointOption {
	return func(cfg *typedEndpointConfig) error {
		if fn != nil {
			cfg.closers = append(cfg.closers, fn)
		}
		return nil
	}
}

// WithEndpointTimeout sets a per-endpoint request timeout overriding the global REQUEST_TIMEOUT.
func WithEndpointTimeout(d time.Duration) EndpointOption {
	return func(cfg *typedEndpointConfig) error {
		if d < 0 {
			return fmt.Errorf("endpoint timeout must not be negative: %v", d)
		}
		cfg.timeout = d
		return nil
	}
}

// WithEndpointMaxBodyBytes overrides the maximum request body size in bytes for a single endpoint
// (both typed and legacy handlers). The limit must satisfy 0 < limit <= 1 GiB (1 << 30).
func WithEndpointMaxBodyBytes(limit int64) EndpointOption {
	return func(cfg *typedEndpointConfig) error {
		if limit <= 0 || limit > maxEndpointBodyBytesLimit {
			return fmt.Errorf("endpoint max body bytes must be in (0, %d], got %d", maxEndpointBodyBytesLimit, limit)
		}
		cfg.maxBodyBytes = limit
		return nil
	}
}

// WithEndpointMiddleware attaches per-endpoint PiMiddleware executed after group PiMiddleware.
func WithEndpointMiddleware(mws ...PiMiddleware) EndpointOption {
	return func(cfg *typedEndpointConfig) error {
		for i, mw := range mws {
			if mw == nil {
				return fmt.Errorf("endpoint PiMiddleware at index %d must not be nil", i)
			}
		}
		cfg.piMWs = append(cfg.piMWs, mws...)
		return nil
	}
}

// WithEndpointHTTPMiddleware attaches per-endpoint HTTP middleware executed after group HTTP middleware.
func WithEndpointHTTPMiddleware(mws ...func(http.Handler) http.Handler) EndpointOption {
	return func(cfg *typedEndpointConfig) error {
		for i, mw := range mws {
			if mw == nil {
				return fmt.Errorf("endpoint HTTP middleware at index %d must not be nil", i)
			}
		}
		cfg.httpMWs = append(cfg.httpMWs, mws...)
		return nil
	}
}

// WithEndpointDoc attaches per-endpoint OpenAPI tags or security overrides.
func WithEndpointDoc(options ...DocOption) EndpointOption {
	return func(cfg *typedEndpointConfig) error {
		cfg.hasEndpointDoc = true
		for _, opt := range options {
			if opt == nil {
				return errors.New("endpoint DocOption must not be nil")
			}
			var step docConfig
			if err := opt(&step); err != nil {
				return err
			}
			if err := mergeDocConfig(&cfg.doc, step, "endpoint"); err != nil {
				return err
			}
		}
		return nil
	}
}

// WithStrictUnknownFields overrides the application-level StrictUnknownFields setting for one endpoint.
func WithStrictUnknownFields(strict bool) EndpointOption {
	return func(cfg *typedEndpointConfig) error {
		v := strict
		cfg.strictOverride = &v
		return nil
	}
}

// TypedHandler is a compile-time-checked typed endpoint descriptor produced by H.
// Passing H(fn) to App/RouteGroup HTTP method helpers provides compile-time signature
// checking and eliminates reflect.Value.Call on the request hot path.
type TypedHandler struct {
	handlerName string
	reqPtrType  reflect.Type
	resPtrType  reflect.Type
	fnIsNil     bool
	buildDirect func(*App, *contract.CompiledOperation) Handler
}

// H wraps a strongly-typed handler function into a TypedHandler descriptor with
// compile-time type checking and zero reflect.Value.Call on request dispatch.
func H[Req, Res any](fn func(*Context, *Req) (*Res, error)) TypedHandler {
	return TypedHandler{
		handlerName: runtimeFuncName(fn),
		reqPtrType:  reflect.TypeOf((*Req)(nil)),
		resPtrType:  reflect.TypeOf((*Res)(nil)),
		fnIsNil:     fn == nil,
		buildDirect: func(app *App, op *contract.CompiledOperation) Handler {
			return makeDirectTypedHandler(app, op, fn)
		},
	}
}

type typedRouteEntry struct {
	handlerName          string
	reqPtrType           reflect.Type
	resPtrType           reflect.Type
	hasExplicitRoute     bool
	explicitMethod       string
	explicitPattern      string
	endpointMaxBody      int64
	strictOverride       *bool
	endpointDoc          docConfig
	compiledInheritedDoc docConfig
	compiledPrefix       string
	op                   *contract.CompiledOperation
}

// Register registers a strongly-typed HTTP endpoint with compile-time handler signature checks
// and zero-reflect.Call request dispatch. Errors are both returned and recorded on the App.
func Register[Req, Res any](
	target RouteTarget,
	fn func(*Context, *Req) (*Res, error),
	opts ...EndpointOption,
) error {
	if target == nil {
		return errors.New("route target must not be nil")
	}
	group := target.routeGroupTarget()
	if group == nil || group.node == nil || group.app == nil {
		return errors.New("route target must be a valid *pi.App or *pi.RouteGroup")
	}
	app := group.app
	if app.httpServer == nil || app.httpServer.registry == nil {
		return errors.New("application has no HTTP router")
	}
	reg := app.httpServer.registry

	reg.mu.Lock()
	defer reg.mu.Unlock()

	if reg.compiled {
		return ErrRoutesFrozen
	}

	if fn == nil {
		err := errors.New("typed handler function must not be nil")
		app.recordRouteErrorLocked(err)
		return err
	}

	var epCfg typedEndpointConfig
	for i, opt := range opts {
		if opt == nil {
			err := fmt.Errorf("endpoint option at index %d must not be nil", i)
			app.recordRouteErrorLocked(err)
			return err
		}
		if err := opt(&epCfg); err != nil {
			app.recordRouteErrorLocked(err)
			return err
		}
	}
	app.resolveEndpointRateLimit(&epCfg, groupFullPrefix(group.node)+":"+runtimeFuncName(fn))
	app.resolveEndpointIdempotency(&epCfg)
	for _, c := range epCfg.closers {
		app.registerMiddlewareCloser(c)
	}

	cand := typedBindCandidate{
		handlerName: runtimeFuncName(fn),
		reqPtrType:  reflect.TypeOf((*Req)(nil)),
		resPtrType:  reflect.TypeOf((*Res)(nil)),
		cfg:         epCfg,
		buildHandler: func(op *contract.CompiledOperation) Handler {
			return makeDirectTypedHandler(app, op, fn)
		},
	}

	if err := app.commitTypedCandidatesLocked(group.node, []typedBindCandidate{cand}); err != nil {
		app.recordRouteErrorLocked(err)
		return err
	}
	return nil
}

// Bind validates and atomically registers one or more *struct controllers or typed handler functions at root scope.
//
// Deprecated: Prefer App.GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS (with optional pi.H and App.Scope)
// to declare explicit HTTP routes per endpoint instead of whole-controller reflection binding.
func (a *App) Bind(targets ...any) error {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return errors.New("application has no HTTP router")
	}
	return a.bindToGroup(a.httpServer.registry.root, targets...)
}

// Bind validates and atomically registers one or more *struct controllers or typed handler functions onto this group.
//
// Deprecated: Prefer RouteGroup.GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS (with optional pi.H and RouteGroup.Scope)
// to declare explicit HTTP routes per endpoint instead of whole-controller reflection binding.
func (g *RouteGroup) Bind(targets ...any) error {
	if g == nil || g.node == nil {
		return errors.New("route group must not be nil")
	}
	if g.app == nil || g.app.httpServer == nil || g.app.httpServer.registry == nil {
		return errors.New("route group has no backing application HTTP router")
	}
	return g.app.bindToGroup(g.node, targets...)
}

// RouteError returns all route, contract, and OpenAPI declaration errors accumulated on the App, or nil if none.
func (a *App) RouteError() error {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return nil
	}
	reg := a.httpServer.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()
	if err := a.syncAndValidateRoutesLocked(false); err != nil {
		return err
	}
	return nil
}

func (a *App) bindToGroup(node *GroupNode, targets ...any) error {
	reg := a.httpServer.registry
	reg.mu.Lock()
	defer reg.mu.Unlock()

	if reg.compiled {
		return ErrRoutesFrozen
	}

	if len(targets) == 0 {
		err := errors.New("Bind requires at least one controller pointer or typed handler function")
		a.recordRouteErrorLocked(err)
		return err
	}

	var candidates []typedBindCandidate
	for idx, target := range targets {
		discovered, err := a.discoverBindCandidates(idx, target)
		if err != nil {
			a.recordRouteErrorLocked(err)
			return err
		}
		candidates = append(candidates, discovered...)
	}

	if err := a.commitTypedCandidatesLocked(node, candidates); err != nil {
		a.recordRouteErrorLocked(err)
		return err
	}
	return nil
}

type typedBindCandidate struct {
	handlerName      string
	reqPtrType       reflect.Type
	resPtrType       reflect.Type
	hasExplicitRoute bool
	explicitMethod   string
	explicitPattern  string
	cfg              typedEndpointConfig
	buildHandler     func(*contract.CompiledOperation) Handler
}

var (
	contextPtrType = reflect.TypeOf((*Context)(nil))
	errorType      = reflect.TypeOf((*error)(nil)).Elem()
	anyType        = reflect.TypeOf((*any)(nil)).Elem()
	handlerType    = reflect.TypeOf((*Handler)(nil)).Elem()
)

func matchLegacyHandler(rawHandler any) (fn Handler, matched bool, isNil bool) {
	if rawHandler == nil {
		return nil, false, false
	}
	switch h := rawHandler.(type) {
	case Handler:
		return h, true, h == nil
	case func(*Context) (any, error):
		return Handler(h), true, h == nil
	}
	rv := reflect.ValueOf(rawHandler)
	rt := rv.Type()
	if rt.Kind() == reflect.Func && rt.ConvertibleTo(handlerType) {
		if rv.IsNil() {
			return nil, true, true
		}
		converted, ok := rv.Convert(handlerType).Interface().(Handler)
		if !ok || converted == nil {
			return nil, true, true
		}
		return converted, true, false
	}
	return nil, false, false
}

func (g *RouteGroup) addUnifiedRoute(method, pattern string, rawHandler any, opts ...EndpointOption) *RouteGroup {
	if g == nil || g.node == nil {
		return g
	}
	reg := g.registry()
	if reg == nil {
		// Offline GroupNode without a backing *App (e.g. unit tests constructing &GroupNode{} directly).
		var epCfg typedEndpointConfig
		for _, opt := range opts {
			if opt == nil {
				return g
			}
			if err := opt(&epCfg); err != nil {
				return g
			}
		}
		g.app.resolveEndpointRateLimit(&epCfg, endpointRateLimitScope(g.node, method, pattern, rawHandler))
		g.app.resolveEndpointIdempotency(&epCfg)
		if legacyFn, matched, isNil := matchLegacyHandler(rawHandler); matched && !isNil && !epCfg.hasEndpointDoc && epCfg.strictOverride == nil {
			idx := len(g.node.routes)
			g.node.routes = append(g.node.routes, RouteDef{
				Method:         method,
				Pattern:        pattern,
				Handler:        legacyFn,
				RequestTimeout: epCfg.timeout,
				MaxBodyBytes:   epCfg.maxBodyBytes,
			})
			if len(epCfg.httpMWs) > 0 || len(epCfg.piMWs) > 0 {
				g.node.setRouteMeta(idx, &routeMeta{
					httpMWs: append([]func(http.Handler) http.Handler(nil), epCfg.httpMWs...),
					piMWs:   append([]PiMiddleware(nil), epCfg.piMWs...),
				})
			}
		}
		return g
	}

	reg.mu.Lock()
	defer reg.mu.Unlock()

	if !g.app.canMutateRoutesLocked("register routes") {
		return g
	}

	var epCfg typedEndpointConfig
	for i, opt := range opts {
		if opt == nil {
			g.app.recordRouteErrorLocked(fmt.Errorf("endpoint option at index %d must not be nil", i))
			return g
		}
		if err := opt(&epCfg); err != nil {
			g.app.recordRouteErrorLocked(err)
			return g
		}
	}
	g.app.resolveEndpointRateLimit(&epCfg, endpointRateLimitScope(g.node, method, pattern, rawHandler))
	g.app.resolveEndpointIdempotency(&epCfg)
	for _, c := range epCfg.closers {
		g.app.registerMiddlewareCloser(c)
	}

	switch th := rawHandler.(type) {
	case TypedHandler:
		if th.buildDirect == nil || th.fnIsNil {
			g.app.recordRouteErrorLocked(errors.New("typed handler function must not be nil"))
			return g
		}
		cand := typedBindCandidate{
			handlerName:      th.handlerName,
			reqPtrType:       th.reqPtrType,
			resPtrType:       th.resPtrType,
			hasExplicitRoute: true,
			explicitMethod:   method,
			explicitPattern:  pattern,
			cfg:              epCfg,
			buildHandler: func(op *contract.CompiledOperation) Handler {
				return th.buildDirect(g.app, op)
			},
		}
		if err := g.app.commitTypedCandidatesLocked(g.node, []typedBindCandidate{cand}); err != nil {
			g.app.recordRouteErrorLocked(err)
		}
		return g
	case *TypedHandler:
		g.app.recordRouteErrorLocked(errors.New("expected pi.TypedHandler value from pi.H(fn), not *pi.TypedHandler pointer"))
		return g
	}

	if legacyFn, matched, isNil := matchLegacyHandler(rawHandler); matched {
		if isNil {
			g.app.recordRouteErrorLocked(errors.New("route handler must not be nil"))
			return g
		}
		if epCfg.hasEndpointDoc || epCfg.strictOverride != nil {
			g.app.recordRouteErrorLocked(fmt.Errorf(
				"legacy route %s %s (func(*pi.Context) (any, error)) does not support WithEndpointDoc or WithStrictUnknownFields; switch to typed handler func(*pi.Context, *Req) (*Res, error)",
				method, pattern,
			))
			return g
		}
		idx := len(g.node.routes)
		g.node.routes = append(g.node.routes, RouteDef{
			Method:         method,
			Pattern:        pattern,
			Handler:        legacyFn,
			RequestTimeout: epCfg.timeout,
			MaxBodyBytes:   epCfg.maxBodyBytes,
		})
		if len(epCfg.httpMWs) > 0 || len(epCfg.piMWs) > 0 {
			g.node.setRouteMeta(idx, &routeMeta{
				httpMWs: append([]func(http.Handler) http.Handler(nil), epCfg.httpMWs...),
				piMWs:   append([]PiMiddleware(nil), epCfg.piMWs...),
			})
		}
		g.app.ensureHTTPAvailable()
		reg.markDirtyLocked()
		return g
	}

	if rawHandler == nil {
		g.app.recordRouteErrorLocked(errors.New("route handler must not be nil"))
		return g
	}

	rv := reflect.ValueOf(rawHandler)
	rt := rv.Type()
	if rv.Kind() == reflect.Func {
		if rv.IsNil() {
			g.app.recordRouteErrorLocked(errors.New("typed handler function must not be nil"))
			return g
		}
		handlerName := runtimeFuncName(rawHandler)
		reqPtrType, resPtrType, err := validateUnifiedTypedHandlerSignature(handlerName, rt)
		if err != nil {
			g.app.recordRouteErrorLocked(err)
			return g
		}
		reqElem := reqPtrType.Elem()
		fnVal := rv
		cand := typedBindCandidate{
			handlerName:      handlerName,
			reqPtrType:       reqPtrType,
			resPtrType:       resPtrType,
			hasExplicitRoute: true,
			explicitMethod:   method,
			explicitPattern:  pattern,
			cfg:              epCfg,
			buildHandler: func(op *contract.CompiledOperation) Handler {
				return makeReflectTypedHandler(g.app, op, fnVal, reqElem)
			},
		}
		if err := g.app.commitTypedCandidatesLocked(g.node, []typedBindCandidate{cand}); err != nil {
			g.app.recordRouteErrorLocked(err)
		}
		return g
	}

	if rv.Kind() == reflect.Ptr && rt.Elem().Kind() == reflect.Struct {
		g.app.recordRouteErrorLocked(fmt.Errorf(
			"route %s %s received controller pointer %s; pass a specific handler method (e.g. h.Method) instead of the controller struct",
			method, pattern, rt.String(),
		))
		return g
	}

	g.app.recordRouteErrorLocked(fmt.Errorf(
		"route %s %s handler must be func(*pi.Context) (any, error), func(*pi.Context, *Req) (*Res, error), or pi.TypedHandler, got %s",
		method, pattern, rt.String(),
	))
	return g
}

func (a *App) discoverBindCandidates(index int, target any) ([]typedBindCandidate, error) {
	if target == nil {
		return nil, fmt.Errorf("bind target[%d] must not be nil", index)
	}

	rv := reflect.ValueOf(target)
	rt := rv.Type()

	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return nil, fmt.Errorf("bind target[%d] (%s) must not be nil", index, rt.String())
		}
		if rt.Elem().Kind() != reflect.Struct {
			return nil, fmt.Errorf("bind target[%d] (%s): expected non-nil pointer to struct controller or typed handler function", index, rt.String())
		}
		methodCount := rt.NumMethod()
		if methodCount == 0 {
			return nil, fmt.Errorf("controller %s has no exported methods to bind", rt.Elem().String())
		}
		candidates := make([]typedBindCandidate, 0, methodCount)
		for i := 0; i < methodCount; i++ {
			method := rt.Method(i)
			mVal := rv.Method(i)
			mType := mVal.Type()
			handlerName := rt.Elem().String() + "." + method.Name
			reqPtrType, resPtrType, err := validateTypedHandlerSignature(handlerName, mType)
			if err != nil {
				return nil, err
			}
			reqElem := reqPtrType.Elem()
			fnVal := mVal
			candidates = append(candidates, typedBindCandidate{
				handlerName: handlerName,
				reqPtrType:  reqPtrType,
				resPtrType:  resPtrType,
				buildHandler: func(op *contract.CompiledOperation) Handler {
					return makeReflectTypedHandler(a, op, fnVal, reqElem)
				},
			})
		}
		return candidates, nil

	case reflect.Func:
		if rv.IsNil() {
			return nil, fmt.Errorf("bind target[%d] handler function must not be nil", index)
		}
		handlerName := runtimeFuncName(target)
		reqPtrType, resPtrType, err := validateTypedHandlerSignature(handlerName, rt)
		if err != nil {
			return nil, err
		}
		reqElem := reqPtrType.Elem()
		fnVal := rv
		return []typedBindCandidate{{
			handlerName: handlerName,
			reqPtrType:  reqPtrType,
			resPtrType:  resPtrType,
			buildHandler: func(op *contract.CompiledOperation) Handler {
				return makeReflectTypedHandler(a, op, fnVal, reqElem)
			},
		}}, nil

	case reflect.Struct:
		return nil, fmt.Errorf("bind target[%d] (%s): struct value is not supported; pass a pointer (*%s)", index, rt.String(), rt.Name())

	default:
		return nil, fmt.Errorf("bind target[%d] (%s): expected non-nil pointer to struct controller or typed handler function", index, rt.String())
	}
}

func validateTypedHandlerSignature(handlerName string, fnType reflect.Type) (reqPtrType reflect.Type, resPtrType reflect.Type, err error) {
	if fnType == nil || fnType.Kind() != reflect.Func {
		return nil, nil, fmt.Errorf("handler %s is not a function", handlerName)
	}
	if !fnType.IsVariadic() &&
		fnType.NumIn() == 1 &&
		fnType.In(0) == contextPtrType &&
		fnType.NumOut() == 2 &&
		fnType.Out(0) == anyType &&
		fnType.Out(1) == errorType {
		return nil, nil, fmt.Errorf(
			"handler %s uses legacy signature func(*pi.Context) (any, error), which cannot be auto-bound; split streaming/legacy handlers into a separate controller or use pi.Register(group, h.TypedMethod) alongside group.GET/POST(..., h.LegacyMethod)",
			handlerName,
		)
	}
	return validateTypedHandlerArgs(handlerName, fnType, "func(*pi.Context, *Req) (*Res, error)")
}

func validateUnifiedTypedHandlerSignature(handlerName string, fnType reflect.Type) (reqPtrType reflect.Type, resPtrType reflect.Type, err error) {
	if fnType == nil || fnType.Kind() != reflect.Func {
		return nil, nil, fmt.Errorf("handler %s is not a function", handlerName)
	}
	return validateTypedHandlerArgs(handlerName, fnType, "func(*pi.Context) (any, error) or func(*pi.Context, *Req) (*Res, error)")
}

func validateTypedHandlerArgs(handlerName string, fnType reflect.Type, expectedSig string) (reqPtrType reflect.Type, resPtrType reflect.Type, err error) {
	if fnType.IsVariadic() {
		return nil, nil, fmt.Errorf("handler %s must not be variadic", handlerName)
	}
	if fnType.NumIn() != 2 || fnType.In(0) != contextPtrType || fnType.NumOut() != 2 || fnType.Out(1) != errorType {
		return nil, nil, fmt.Errorf("handler %s must match %s, got %s", handlerName, expectedSig, fnType.String())
	}
	reqPtrType = fnType.In(1)
	resPtrType = fnType.Out(0)
	if reqPtrType.Kind() != reflect.Ptr || reqPtrType.Elem().Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("handler %s request argument must be a pointer to struct, got %s", handlerName, reqPtrType.String())
	}
	if resPtrType.Kind() != reflect.Ptr || resPtrType.Elem().Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("handler %s response return value must be a pointer to struct, got %s", handlerName, resPtrType.String())
	}
	return reqPtrType, resPtrType, nil
}

func makeDirectTypedHandler[Req, Res any](
	app *App,
	op *contract.CompiledOperation,
	fn func(*Context, *Req) (*Res, error),
) Handler {
	var customValidate func(any) error
	if app != nil {
		customValidate = app.customValidate
	}
	return func(ctx *Context) (any, error) {
		httpReq, ok := ctx.Request.(*piHTTP.Request)
		if !ok || httpReq == nil {
			return nil, errors.New("typed HTTP handler requires an HTTP request context")
		}
		req := new(Req)
		if err := op.BindAndValidate(httpReq, req, customValidate); err != nil {
			return nil, err
		}
		httpReq.ReleaseRawBody()
		res, err := fn(ctx, req)
		return op.WrapResponse(res, err)
	}
}

func makeReflectTypedHandler(
	app *App,
	op *contract.CompiledOperation,
	fnVal reflect.Value,
	reqElemType reflect.Type,
) Handler {
	var customValidate func(any) error
	if app != nil {
		customValidate = app.customValidate
	}
	return func(ctx *Context) (any, error) {
		httpReq, ok := ctx.Request.(*piHTTP.Request)
		if !ok || httpReq == nil {
			return nil, errors.New("typed HTTP handler requires an HTTP request context")
		}
		reqVal := reflect.New(reqElemType)
		if err := op.BindAndValidate(httpReq, reqVal.Interface(), customValidate); err != nil {
			return nil, err
		}
		httpReq.ReleaseRawBody()
		outs := fnVal.Call([]reflect.Value{reflect.ValueOf(ctx), reqVal})
		var fnErr error
		if !outs[1].IsNil() {
			fnErr, _ = outs[1].Interface().(error)
		}
		return op.WrapResponse(outs[0].Interface(), fnErr)
	}
}

type routeConflictEntry struct {
	method        string
	canonicalPath string
	source        string
}

func (a *App) commitTypedCandidatesLocked(node *GroupNode, candidates []typedBindCandidate) error {
	if _, err := a.syncTypedDocsLocked(); err != nil {
		return err
	}

	if len(candidates) > 0 {
		if err := a.checkGroupMountConflictsForNodeLocked(node); err != nil {
			return err
		}
	}

	groupPrefix := groupFullPrefix(node)
	inheritedDoc := effectiveGroupDoc(node)

	existingShapes, existingOpIDs, existingTemplates, reservedShapes := a.buildRouteIndexLocked()
	stagingSR := contract.NewSchemaRegistry()

	type stagedRoute struct {
		rd   RouteDef
		meta *routeMeta
	}
	staged := make([]stagedRoute, 0, len(candidates))

	for _, cand := range candidates {
		compileOpts := buildCompileOptions(
			groupPrefix,
			inheritedDoc,
			a.typedBind.StrictUnknownFields,
			cand.cfg.strictOverride,
			cand.cfg.doc,
			cand.hasExplicitRoute,
			cand.explicitMethod,
			cand.explicitPattern,
			cand.cfg.maxBodyBytes,
			a.explicitMaxBodyBytes(),
		)
		op, err := contract.CompileOperation(cand.handlerName, cand.reqPtrType, cand.resPtrType, compileOpts, stagingSR)
		if err != nil {
			return err
		}

		if reservedDesc, conflict := reservedShapes[op.ConflictShape]; conflict {
			return fmt.Errorf("typed route %s (%s %s) conflicts with reserved %s", cand.handlerName, op.Method, op.CanonicalPath, reservedDesc)
		}
		if prev, conflict := existingShapes[op.ConflictShape]; conflict {
			return fmt.Errorf("route conflict on %s (%s): typed route %s (%s %s) conflicts with %s (%s %s)",
				op.ConflictShape, op.CanonicalPath, cand.handlerName, op.Method, op.CanonicalPath, prev.source, prev.method, prev.canonicalPath)
		}
		if prev, dupOp := existingOpIDs[op.OperationID]; dupOp {
			return fmt.Errorf("duplicate OpenAPI operationId %q: typed route %s conflicts with %s", op.OperationID, cand.handlerName, prev.source)
		}
		tmpl := contract.NormalizeOpenAPIPathTemplate(op.CanonicalPath)
		if prev, conflict := existingTemplates[tmpl]; conflict && prev.canonicalPath != op.CanonicalPath {
			return fmt.Errorf("inconsistent OpenAPI path parameter names on %s: typed route %s (%s %s) conflicts with %s (%s %s)",
				tmpl, cand.handlerName, op.Method, op.CanonicalPath, prev.source, prev.method, prev.canonicalPath)
		}

		existingShapes[op.ConflictShape] = routeConflictEntry{
			method:        op.Method,
			canonicalPath: op.CanonicalPath,
			source:        cand.handlerName,
		}
		existingOpIDs[op.OperationID] = routeConflictEntry{
			method:        op.Method,
			canonicalPath: op.CanonicalPath,
			source:        cand.handlerName,
		}
		if _, exists := existingTemplates[tmpl]; !exists {
			existingTemplates[tmpl] = routeConflictEntry{
				method:        op.Method,
				canonicalPath: op.CanonicalPath,
				source:        cand.handlerName,
			}
		}

		entry := &typedRouteEntry{
			handlerName:          cand.handlerName,
			reqPtrType:           cand.reqPtrType,
			resPtrType:           cand.resPtrType,
			hasExplicitRoute:     cand.hasExplicitRoute,
			explicitMethod:       cand.explicitMethod,
			explicitPattern:      cand.explicitPattern,
			endpointMaxBody:      cand.cfg.maxBodyBytes,
			strictOverride:       cand.cfg.strictOverride,
			endpointDoc:          cloneDocConfig(cand.cfg.doc),
			compiledInheritedDoc: cloneDocConfig(inheritedDoc),
			compiledPrefix:       groupPrefix,
			op:                   op,
		}

		meta := &routeMeta{
			httpMWs: append([]func(http.Handler) http.Handler(nil), cand.cfg.httpMWs...),
			piMWs:   append([]PiMiddleware(nil), cand.cfg.piMWs...),
			typed:   entry,
		}

		staged = append(staged, stagedRoute{
			rd: RouteDef{
				Method:         op.Method,
				Pattern:        op.ChiPattern,
				Handler:        cand.buildHandler(op),
				RequestTimeout: cand.cfg.timeout,
				MaxBodyBytes:   op.MaxBodyBytes,
			},
			meta: meta,
		})
	}

	for _, item := range staged {
		idx := len(node.routes)
		node.routes = append(node.routes, item.rd)
		node.setRouteMeta(idx, item.meta)
	}

	a.ensureHTTPAvailable()
	a.httpServer.registry.markDirtyLocked()
	return nil
}

func (a *App) syncTypedDocsLocked() (bool, error) {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return false, nil
	}
	if a.httpServer.registry.compiled {
		return false, nil
	}
	var errs []error
	changed := false
	walkGroupNodesLocked(a.httpServer.registry.root, "", docConfig{}, func(node *GroupNode, fullPrefix string, inherited docConfig) {
		for i := range node.routes {
			meta := node.routeMetaAt(i)
			if meta == nil || meta.typed == nil {
				continue
			}
			entry := meta.typed
			if docConfigEqual(entry.compiledInheritedDoc, inherited) && entry.compiledPrefix == fullPrefix {
				continue
			}
			compileOpts := buildCompileOptions(
				fullPrefix,
				inherited,
				a.typedBind.StrictUnknownFields,
				entry.strictOverride,
				entry.endpointDoc,
				entry.hasExplicitRoute,
				entry.explicitMethod,
				entry.explicitPattern,
				entry.endpointMaxBody,
				a.explicitMaxBodyBytes(),
			)
			op, err := contract.CompileOperation(entry.handlerName, entry.reqPtrType, entry.resPtrType, compileOpts, nil)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			// Update OpenAPI metadata on the existing op pointer in-place as well as entry.op.
			if entry.op != nil {
				*entry.op = *op
			} else {
				entry.op = op
			}
			node.routes[i].Pattern = op.ChiPattern
			node.routes[i].MaxBodyBytes = op.MaxBodyBytes
			entry.compiledInheritedDoc = cloneDocConfig(inherited)
			entry.compiledPrefix = fullPrefix
			changed = true
		}
	})
	if changed {
		a.httpServer.registry.markDirtyLocked()
	}
	return changed, errors.Join(errs...)
}

func (a *App) syncAndValidateRoutesLocked(checkSecuritySchemes bool) error {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return nil
	}
	reg := a.httpServer.registry

	errs := append([]error(nil), reg.declErrors...)
	if a.kernel != nil && a.kernel.trustedProxyErr != nil {
		errs = append(errs, a.kernel.trustedProxyErr)
	}
	if _, syncErr := a.syncTypedDocsLocked(); syncErr != nil {
		errs = append(errs, syncErr)
	}
	if a.hasOpenAPIConfig {
		if pathErr := validateOpenAPIRoutePaths(a.openAPI.SpecPath, a.openAPI.UIPath); pathErr != nil {
			errs = append(errs, pathErr)
		}
	}
	_, mountErrs := collectGroupMountIndexLocked(reg.root)
	errs = append(errs, mountErrs...)

	reservedShapes := a.reservedOpenAPIShapes()
	shapeSeen := make(map[string]routeConflictEntry)
	opIDSeen := make(map[string]routeConflictEntry)
	tmplSeen := make(map[string]routeConflictEntry)

	walkGroupNodesLocked(reg.root, "", docConfig{}, func(node *GroupNode, fullPrefix string, _ docConfig) {
		for i, rd := range node.routes {
			meta := node.routeMetaAt(i)
			if meta != nil && meta.isSystem && meta.staticHandler == nil {
				continue
			}

			var method, canonicalPath, shape, source string
			if meta != nil && meta.typed != nil && meta.typed.op != nil {
				op := meta.typed.op
				method = op.Method
				canonicalPath = op.CanonicalPath
				shape = op.ConflictShape
				source = "typed route " + meta.typed.handlerName
			} else if meta != nil && meta.staticHandler != nil {
				method = strings.ToUpper(strings.TrimSpace(rd.Method))
				canonicalPath, shape = contract.NormalizeRouteConflict(fullPrefix, method, rd.Pattern)
				source = fmt.Sprintf("static FS route %s %s", method, canonicalPath)
				if a.hasOpenAPIConfig && a.openAPI.ServeUI && a.openAPI.UIPath != "" && isDirectChildRoute(a.openAPI.UIPath, canonicalPath) {
					errs = append(errs, fmt.Errorf("%s conflicts with OpenAPI UI asset route GET %s", source, swaggerAssetRoutePattern(a.openAPI.UIPath)))
				}
			} else {
				method = strings.ToUpper(strings.TrimSpace(rd.Method))
				canonicalPath, shape = contract.NormalizeRouteConflict(fullPrefix, method, rd.Pattern)
				source = fmt.Sprintf("legacy route %s %s", method, canonicalPath)
				if err := validateLegacyChiRoute(method, canonicalPath); err != nil {
					errs = append(errs, err)
				}
			}

			if reservedDesc, conflict := reservedShapes[shape]; conflict {
				errs = append(errs, fmt.Errorf("%s conflicts with reserved %s", source, reservedDesc))
			}

			if prev, conflict := shapeSeen[shape]; conflict {
				errs = append(errs, fmt.Errorf("route conflict on %s (%s): %s conflicts with earlier %s (%s %s)",
					shape, canonicalPath, source, prev.source, prev.method, prev.canonicalPath))
			} else {
				shapeSeen[shape] = routeConflictEntry{
					method:        method,
					canonicalPath: canonicalPath,
					source:        source,
				}
			}

			if meta != nil && meta.typed != nil && meta.typed.op != nil {
				op := meta.typed.op
				if prev, dupOp := opIDSeen[op.OperationID]; dupOp {
					errs = append(errs, fmt.Errorf("duplicate OpenAPI operationId %q: %s conflicts with %s",
						op.OperationID, source, prev.source))
				} else {
					opIDSeen[op.OperationID] = routeConflictEntry{
						method:        method,
						canonicalPath: canonicalPath,
						source:        source,
					}
				}

				tmpl := contract.NormalizeOpenAPIPathTemplate(op.CanonicalPath)
				if prev, conflict := tmplSeen[tmpl]; conflict {
					if prev.canonicalPath != op.CanonicalPath {
						errs = append(errs, fmt.Errorf("inconsistent OpenAPI path parameter names on %s: %s (%s %s) conflicts with %s (%s %s)",
							tmpl, source, method, op.CanonicalPath, prev.source, prev.method, prev.canonicalPath))
					}
				} else {
					tmplSeen[tmpl] = routeConflictEntry{
						method:        method,
						canonicalPath: op.CanonicalPath,
						source:        source,
					}
				}

				if checkSecuritySchemes {
					for _, schemeName := range op.ReferencedSecSchemes {
						if !a.hasOpenAPIConfig {
							errs = append(errs, fmt.Errorf("typed route %s (%s %s) references OpenAPI security scheme %q, but WithOpenAPI is not configured",
								meta.typed.handlerName, op.Method, op.CanonicalPath, schemeName))
							continue
						}
						if _, ok := a.openAPI.SecuritySchemes[schemeName]; !ok {
							errs = append(errs, fmt.Errorf("typed route %s (%s %s) references undefined OpenAPI security scheme %q",
								meta.typed.handlerName, op.Method, op.CanonicalPath, schemeName))
						}
					}
				}
			}
		}
	})

	return errors.Join(errs...)
}

func (a *App) buildRouteIndexLocked() (shapes map[string]routeConflictEntry, opIDs map[string]routeConflictEntry, pathTemplates map[string]routeConflictEntry, reserved map[string]string) {
	shapes = make(map[string]routeConflictEntry)
	opIDs = make(map[string]routeConflictEntry)
	pathTemplates = make(map[string]routeConflictEntry)
	reserved = a.reservedOpenAPIShapes()

	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return shapes, opIDs, pathTemplates, reserved
	}

	walkGroupNodesLocked(a.httpServer.registry.root, "", docConfig{}, func(node *GroupNode, fullPrefix string, _ docConfig) {
		for i, rd := range node.routes {
			meta := node.routeMetaAt(i)
			if meta != nil && meta.isSystem && meta.staticHandler == nil {
				continue
			}

			var method, canonicalPath, shape, source string
			if meta != nil && meta.typed != nil && meta.typed.op != nil {
				op := meta.typed.op
				method = op.Method
				canonicalPath = op.CanonicalPath
				shape = op.ConflictShape
				source = meta.typed.handlerName
				if _, exists := opIDs[op.OperationID]; !exists {
					opIDs[op.OperationID] = routeConflictEntry{
						method:        method,
						canonicalPath: canonicalPath,
						source:        source,
					}
				}
				tmpl := contract.NormalizeOpenAPIPathTemplate(op.CanonicalPath)
				if _, exists := pathTemplates[tmpl]; !exists {
					pathTemplates[tmpl] = routeConflictEntry{
						method:        method,
						canonicalPath: canonicalPath,
						source:        source,
					}
				}
			} else if meta != nil && meta.staticHandler != nil {
				method = strings.ToUpper(strings.TrimSpace(rd.Method))
				canonicalPath, shape = contract.NormalizeRouteConflict(fullPrefix, method, rd.Pattern)
				source = fmt.Sprintf("static FS route %s %s", method, canonicalPath)
			} else {
				method = strings.ToUpper(strings.TrimSpace(rd.Method))
				canonicalPath, shape = contract.NormalizeRouteConflict(fullPrefix, method, rd.Pattern)
				source = fmt.Sprintf("legacy route %s %s", method, canonicalPath)
			}

			if _, exists := shapes[shape]; !exists {
				shapes[shape] = routeConflictEntry{
					method:        method,
					canonicalPath: canonicalPath,
					source:        source,
				}
			}
		}
	})

	return shapes, opIDs, pathTemplates, reserved
}

type groupMountIndex struct {
	byConflictShape map[string]string
}

var supportedLegacyChiMethods = map[string]struct{}{
	http.MethodGet:     {},
	http.MethodHead:    {},
	http.MethodPost:    {},
	http.MethodPut:     {},
	http.MethodPatch:   {},
	http.MethodDelete:  {},
	http.MethodConnect: {},
	http.MethodOptions: {},
	http.MethodTrace:   {},
}

// validateLegacyChiRoute mirrors chi's route parsing invariants before HTTP compilation
// so malformed legacy patterns report via RouteError/HTTPHandler instead of panicking.
func validateLegacyChiRoute(method, canonicalPath string) error {
	if _, ok := supportedLegacyChiMethods[method]; !ok {
		return fmt.Errorf("legacy route %q has unsupported HTTP method %q", canonicalPath, method)
	}
	if !strings.HasPrefix(canonicalPath, "/") {
		return fmt.Errorf("legacy route %s %q must start with '/'", method, canonicalPath)
	}

	seenParams := make(map[string]struct{})
	depth := 0
	blockStart := -1
	for i := 0; i < len(canonicalPath); i++ {
		switch canonicalPath[i] {
		case '{':
			if depth == 0 {
				blockStart = i
			}
			depth++
		case '}':
			if depth == 0 {
				return fmt.Errorf("legacy route %s %q has unmatched '}'", method, canonicalPath)
			}
			depth--
			if depth == 0 {
				if i+1 < len(canonicalPath) && (canonicalPath[i+1] == '{' || canonicalPath[i+1] == '*') {
					return fmt.Errorf("legacy route %s %q has adjacent parameters or wildcard without delimiter", method, canonicalPath)
				}
				inner := canonicalPath[blockStart+1 : i]
				paramName, rexpat, hasColon := strings.Cut(inner, ":")
				if paramName == "" || strings.ContainsAny(paramName, "{}") {
					return fmt.Errorf("legacy route %s %q has invalid parameter name in %q", method, canonicalPath, "{"+inner+"}")
				}
				if _, dup := seenParams[paramName]; dup {
					return fmt.Errorf("legacy route %s %q has duplicate path parameter %q", method, canonicalPath, paramName)
				}
				seenParams[paramName] = struct{}{}
				if hasColon {
					anchored := rexpat
					if len(anchored) > 0 {
						if anchored[0] != '^' {
							anchored = "^" + anchored
						}
						if anchored[len(anchored)-1] != '$' {
							anchored += "$"
						}
					}
					if _, err := regexp.Compile(anchored); err != nil {
						return fmt.Errorf("legacy route %s %q has invalid regexp %q: %w", method, canonicalPath, rexpat, err)
					}
				}
			}
		case '*':
			if depth == 0 {
				if i != len(canonicalPath)-1 {
					return fmt.Errorf("legacy route %s %q has wildcard '*' before end of path", method, canonicalPath)
				}
				if _, dup := seenParams["*"]; dup {
					return fmt.Errorf("legacy route %s %q has duplicate path parameter %q", method, canonicalPath, "*")
				}
				seenParams["*"] = struct{}{}
			}
		}
	}
	if depth != 0 {
		return fmt.Errorf("legacy route %s %q has unclosed '{'", method, canonicalPath)
	}
	return nil
}

// splitPathSegmentsBraceAware splits a normalized path prefix on '/' only at brace depth 0
// so regexp patterns containing '/' inside '{...}' stay intact in their segment.
func splitPathSegmentsBraceAware(fullPrefix string) []string {
	trimmed := strings.TrimPrefix(fullPrefix, "/")
	if trimmed == "" {
		return nil
	}

	var segments []string
	depth := 0
	start := 0
	for i := 0; i < len(trimmed); i++ {
		switch trimmed[i] {
		case '{':
			depth++
		case '}':
			if depth > 0 {
				depth--
			}
		case '/':
			if depth == 0 {
				if seg := trimmed[start:i]; seg != "" {
					segments = append(segments, seg)
				}
				start = i + 1
			}
		}
	}
	if seg := trimmed[start:]; seg != "" {
		segments = append(segments, seg)
	}
	return segments
}

func cumulativeGroupPrefixes(fullPrefix string) []string {
	segs := splitPathSegmentsBraceAware(fullPrefix)
	if len(segs) == 0 {
		return nil
	}

	prefixes := make([]string, 0, len(segs))
	cur := ""
	for _, seg := range segs {
		cur += "/" + seg
		prefixes = append(prefixes, cur)
	}
	return prefixes
}

func collectGroupMountIndexLocked(root *GroupNode) (groupMountIndex, []error) {
	idx := groupMountIndex{
		byConflictShape: make(map[string]string),
	}
	if root == nil {
		return idx, nil
	}
	var errs []error

	walkGroupNodesLocked(root, "", docConfig{}, func(node *GroupNode, fullPrefix string, _ docConfig) {
		if node == root || !groupNodeHasRoutes(node) || fullPrefix == "" {
			return
		}
		for _, cumPrefix := range cumulativeGroupPrefixes(fullPrefix) {
			shape := contract.NormalizeConflictPath(cumPrefix)
			if prev, conflict := idx.byConflictShape[shape]; conflict {
				if prev != cumPrefix {
					errs = append(errs, fmt.Errorf("route group mount conflict on %s: group %q conflicts with existing group %q",
						shape, fullPrefix, prev))
					break
				}
				continue
			}
			idx.byConflictShape[shape] = cumPrefix
		}
	})

	return idx, errs
}

func (a *App) checkGroupMountConflictsForNodeLocked(node *GroupNode) error {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil || node == nil {
		return nil
	}
	reg := a.httpServer.registry
	active, _ := collectGroupMountIndexLocked(reg.root)

	targetFullPrefix := groupFullPrefix(node)
	if targetFullPrefix == "" {
		return nil
	}
	for _, cumPrefix := range cumulativeGroupPrefixes(targetFullPrefix) {
		shape := contract.NormalizeConflictPath(cumPrefix)
		if existing, conflict := active.byConflictShape[shape]; conflict && existing != cumPrefix {
			return fmt.Errorf("route group mount conflict on %s: group %q conflicts with existing group %q",
				shape, targetFullPrefix, existing)
		}
	}
	return nil
}

func (a *App) reservedOpenAPIShapes() map[string]string {
	reserved := make(map[string]string, 8)
	for _, p := range builtinSystemPaths() {
		_, shape := contract.NormalizeRouteConflict("", http.MethodGet, p)
		reserved[shape] = "system route GET " + p
	}
	if a == nil || !a.hasOpenAPIConfig {
		return reserved
	}
	if a.openAPI.ServeSpec && a.openAPI.SpecPath != "" {
		_, shape := contract.NormalizeRouteConflict("", http.MethodGet, a.openAPI.SpecPath)
		reserved[shape] = "OpenAPI spec route GET " + a.openAPI.SpecPath
	}
	if a.openAPI.ServeUI && a.openAPI.UIPath != "" {
		_, uiShape := contract.NormalizeRouteConflict("", http.MethodGet, a.openAPI.UIPath)
		reserved[uiShape] = "OpenAPI UI route GET " + a.openAPI.UIPath
		assetPattern := swaggerAssetRoutePattern(a.openAPI.UIPath)
		_, assetShape := contract.NormalizeRouteConflict("", http.MethodGet, assetPattern)
		reserved[assetShape] = "OpenAPI UI asset route GET " + assetPattern
	}
	return reserved
}

func (a *App) hasTypedRoutesLocked() bool {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return false
	}
	found := false
	walkGroupNodesLocked(a.httpServer.registry.root, "", docConfig{}, func(node *GroupNode, _ string, _ docConfig) {
		if found {
			return
		}
		for i := range node.routes {
			meta := node.routeMetaAt(i)
			if meta != nil && meta.typed != nil {
				found = true
				return
			}
		}
	})
	return found
}

func (a *App) recordRouteErrorLocked(err error) {
	if err == nil || a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return
	}
	reg := a.httpServer.registry
	reg.declErrors = append(reg.declErrors, err)
	reg.markDirtyLocked()
}

func walkGroupNodesLocked(
	node *GroupNode,
	parentPrefix string,
	parentDoc docConfig,
	fn func(node *GroupNode, fullPrefix string, effectiveInherited docConfig),
) {
	if node == nil {
		return
	}
	normPrefix := contract.NormalizeGroupPrefix(node.prefix)
	fullPrefix := joinNormalizedPrefixes(parentPrefix, normPrefix)
	effective := inheritDocConfig(parentDoc, node.doc)

	fn(node, fullPrefix, effective)

	for _, child := range node.children {
		walkGroupNodesLocked(child, fullPrefix, effective, fn)
	}
}

func joinNormalizedPrefixes(parentPrefix, childPrefix string) string {
	switch {
	case parentPrefix == "":
		return childPrefix
	case childPrefix == "":
		return parentPrefix
	default:
		return parentPrefix + childPrefix
	}
}

func groupFullPrefix(node *GroupNode) string {
	var parts []string
	for cur := node; cur != nil; cur = cur.parent {
		if p := contract.NormalizeGroupPrefix(cur.prefix); p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	for l, r := 0, len(parts)-1; l < r; l, r = l+1, r-1 {
		parts[l], parts[r] = parts[r], parts[l]
	}
	return strings.Join(parts, "")
}

func effectiveGroupDoc(node *GroupNode) docConfig {
	var chain []*GroupNode
	for cur := node; cur != nil; cur = cur.parent {
		chain = append(chain, cur)
	}
	var effective docConfig
	for i := len(chain) - 1; i >= 0; i-- {
		effective = inheritDocConfig(effective, chain[i].doc)
	}
	return effective
}

func inheritDocConfig(parent, current docConfig) docConfig {
	out := cloneDocConfig(parent)
	if current.hasTags {
		out.hasTags = true
		out.tags = append([]string(nil), current.tags...)
	}
	if current.hasSecurity {
		out.hasSecurity = true
		if current.security == nil {
			out.security = []string{}
		} else {
			out.security = append([]string(nil), current.security...)
		}
	}
	return out
}

func (a *App) explicitMaxBodyBytes() int64 {
	if a != nil && a.kernel != nil && a.kernel.hasExplicitMaxBody {
		return a.kernel.maxBodyBytes
	}
	return 0
}

func buildCompileOptions(
	groupPrefix string,
	inheritedDoc docConfig,
	defaultStrict bool,
	strictOverride *bool,
	endpointDoc docConfig,
	hasExplicitRoute bool,
	explicitMethod string,
	explicitPattern string,
	endpointMaxBody int64,
	explicitAppMaxBody int64,
) contract.CompileOptions {
	opts := contract.CompileOptions{
		GroupPrefix:             groupPrefix,
		HasExplicitRoute:        hasExplicitRoute,
		ExplicitMethod:          explicitMethod,
		ExplicitPattern:         explicitPattern,
		EndpointMaxBodyBytes:    endpointMaxBody,
		ExplicitAppMaxBodyBytes: explicitAppMaxBody,
		DefaultStrictJSON:       defaultStrict,
		StrictJSONOverride:      strictOverride,
	}
	if inheritedDoc.hasTags {
		opts.InheritedTags = append([]string(nil), inheritedDoc.tags...)
	}
	if inheritedDoc.hasSecurity {
		opts.HasInheritedSec = true
		if inheritedDoc.security == nil {
			opts.InheritedSecurity = []string{}
		} else {
			opts.InheritedSecurity = append([]string(nil), inheritedDoc.security...)
		}
	}
	if endpointDoc.hasTags {
		opts.HasEndpointTags = true
		opts.EndpointTags = append([]string(nil), endpointDoc.tags...)
	}
	if endpointDoc.hasSecurity {
		opts.HasEndpointSec = true
		if endpointDoc.security == nil {
			opts.EndpointSecurity = []string{}
		} else {
			opts.EndpointSecurity = append([]string(nil), endpointDoc.security...)
		}
	}
	return opts
}

func groupDocScopeName(node *GroupNode) string {
	prefix := groupFullPrefix(node)
	if node != nil && node.isScope {
		if prefix == "" {
			return "root scope"
		}
		return fmt.Sprintf("scope %q", prefix)
	}
	if prefix == "" {
		return "root group"
	}
	return fmt.Sprintf("group %q", prefix)
}

func cleanDocStringList(items []string, label string) ([]string, error) {
	if len(items) == 0 {
		return []string{}, nil
	}
	out := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, raw := range items {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			return nil, fmt.Errorf("%s must not be empty", label)
		}
		if _, dup := seen[trimmed]; dup {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out, nil
}

func applyDocTags(cfg *docConfig, tags []string) error {
	if cfg == nil {
		return nil
	}
	if cfg.hasTags && !stringSlicesEqual(cfg.tags, tags) {
		return fmt.Errorf("conflicting DocTags declarations: %v vs %v", cfg.tags, tags)
	}
	cfg.hasTags = true
	if tags == nil {
		cfg.tags = []string{}
	} else {
		cfg.tags = append([]string(nil), tags...)
	}
	return nil
}

func applyDocSecurity(cfg *docConfig, names []string) error {
	if cfg == nil {
		return nil
	}
	if cfg.hasSecurity && !docSecurityEqual(cfg.security, names) {
		return fmt.Errorf("conflicting DocSecurity declarations: %v vs %v", cfg.security, names)
	}
	cfg.hasSecurity = true
	if names == nil {
		cfg.security = []string{}
	} else {
		cfg.security = append([]string(nil), names...)
	}
	return nil
}

func mergeDocConfig(dst *docConfig, src docConfig, scope string) error {
	if dst == nil {
		return nil
	}
	if src.hasTags {
		if err := applyDocTags(dst, src.tags); err != nil {
			return fmt.Errorf("%s: %w", scope, err)
		}
	}
	if src.hasSecurity {
		if err := applyDocSecurity(dst, src.security); err != nil {
			return fmt.Errorf("%s: %w", scope, err)
		}
	}
	return nil
}

func cloneDocConfig(src docConfig) docConfig {
	out := docConfig{
		hasTags:     src.hasTags,
		hasSecurity: src.hasSecurity,
	}
	if src.hasTags {
		if src.tags == nil {
			out.tags = []string{}
		} else {
			out.tags = append([]string(nil), src.tags...)
		}
	}
	if src.hasSecurity {
		if src.security == nil {
			out.security = []string{}
		} else {
			out.security = append([]string(nil), src.security...)
		}
	}
	return out
}

func docConfigEqual(a, b docConfig) bool {
	if a.hasTags != b.hasTags || a.hasSecurity != b.hasSecurity {
		return false
	}
	if a.hasTags && !stringSlicesEqual(a.tags, b.tags) {
		return false
	}
	if a.hasSecurity && !docSecurityEqual(a.security, b.security) {
		return false
	}
	return true
}

func docSecurityEqual(a, b []string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return stringSlicesEqual(a, b)
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func runtimeFuncName(fn any) string {
	if fn == nil {
		return "handler"
	}
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func || v.IsNil() {
		return "handler"
	}
	rf := runtime.FuncForPC(v.Pointer())
	if rf == nil {
		return "handler"
	}
	name := rf.Name()
	if idx := strings.LastIndex(name, "/"); idx >= 0 && idx+1 < len(name) {
		name = name[idx+1:]
	}
	name = strings.TrimSuffix(name, "-fm")
	if name == "" {
		return "handler"
	}
	return name
}
