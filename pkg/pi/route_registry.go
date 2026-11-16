package pi

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/trace"

	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/contract"
	"github.com/sllt/pi/pkg/pi/openapi"
)

// PiMiddleware operates at the application layer with full *Context access.
// It wraps a Handler, optionally calling next to continue the chain.
// To short-circuit, return (nil, err) without calling next(c).
type PiMiddleware func(next Handler) Handler

// RouteDef represents a single route declaration.
type RouteDef struct {
	Method         string
	Pattern        string
	Handler        Handler
	RequestTimeout time.Duration
	MaxBodyBytes   int64
}

type docConfig struct {
	hasTags     bool
	tags        []string
	hasSecurity bool
	security    []string
}

type routeMeta struct {
	isSystem bool
	httpMWs  []func(http.Handler) http.Handler
	piMWs    []PiMiddleware
	typed    *typedRouteEntry
}

// GroupNode is a node in the route group tree.
// It holds middleware and routes for a given prefix, plus child groups.
type GroupNode struct {
	prefix     string
	parent     *GroupNode
	isScope    bool
	httpMWs    []func(http.Handler) http.Handler
	piMWs      []PiMiddleware
	routes     []RouteDef
	routeMetas map[int]*routeMeta
	doc        docConfig
	children   []*GroupNode
}

// RouteGroup is the public API for declaring routes and middleware within a group.
type RouteGroup struct {
	node *GroupNode
	app  *App
}

// RouteRegistry holds the root GroupNode and compiles the tree into chi routes.
type RouteRegistry struct {
	mu           sync.Mutex
	root         *GroupNode
	compiled     bool
	revision     uint64
	declErrors   []error
	cachedDoc    *openapi.Document
	cachedRev    uint64
	hasCachedDoc bool
}

func newRouteRegistry() *RouteRegistry {
	return &RouteRegistry{
		root: &GroupNode{},
	}
}

func (g *RouteGroup) routeGroupTarget() *RouteGroup {
	return g
}

func (g *RouteGroup) registry() *RouteRegistry {
	if g == nil || g.app == nil || g.app.httpServer == nil {
		return nil
	}
	return g.app.httpServer.registry
}

func (reg *RouteRegistry) markDirtyLocked() {
	reg.revision++
	reg.hasCachedDoc = false
	reg.cachedDoc = nil
}

func (n *GroupNode) routeMetaAt(idx int) *routeMeta {
	if n == nil || n.routeMetas == nil {
		return nil
	}
	return n.routeMetas[idx]
}

func (n *GroupNode) setRouteMeta(idx int, meta *routeMeta) {
	if n == nil || meta == nil {
		return
	}
	if n.routeMetas == nil {
		n.routeMetas = make(map[int]*routeMeta)
	}
	n.routeMetas[idx] = meta
}

// ---------- RouteGroup: route registration helpers ----------

// GET registers a typed or legacy handler for HTTP GET on this group.
func (g *RouteGroup) GET(pattern string, handler any, opts ...EndpointOption) *RouteGroup {
	return g.addUnifiedRoute(http.MethodGet, pattern, handler, opts...)
}

// POST registers a typed or legacy handler for HTTP POST on this group.
func (g *RouteGroup) POST(pattern string, handler any, opts ...EndpointOption) *RouteGroup {
	return g.addUnifiedRoute(http.MethodPost, pattern, handler, opts...)
}

// PUT registers a typed or legacy handler for HTTP PUT on this group.
func (g *RouteGroup) PUT(pattern string, handler any, opts ...EndpointOption) *RouteGroup {
	return g.addUnifiedRoute(http.MethodPut, pattern, handler, opts...)
}

// DELETE registers a typed or legacy handler for HTTP DELETE on this group.
func (g *RouteGroup) DELETE(pattern string, handler any, opts ...EndpointOption) *RouteGroup {
	return g.addUnifiedRoute(http.MethodDelete, pattern, handler, opts...)
}

// PATCH registers a typed or legacy handler for HTTP PATCH on this group.
func (g *RouteGroup) PATCH(pattern string, handler any, opts ...EndpointOption) *RouteGroup {
	return g.addUnifiedRoute(http.MethodPatch, pattern, handler, opts...)
}

// HEAD registers a typed or legacy handler for HTTP HEAD on this group.
func (g *RouteGroup) HEAD(pattern string, handler any, opts ...EndpointOption) *RouteGroup {
	return g.addUnifiedRoute(http.MethodHead, pattern, handler, opts...)
}

// OPTIONS registers a typed or legacy handler for HTTP OPTIONS on this group.
func (g *RouteGroup) OPTIONS(pattern string, handler any, opts ...EndpointOption) *RouteGroup {
	return g.addUnifiedRoute(http.MethodOptions, pattern, handler, opts...)
}

func (g *RouteGroup) addRoute(method, pattern string, h Handler, timeout time.Duration) {
	if g == nil || g.node == nil {
		return
	}
	if reg := g.registry(); reg != nil {
		reg.mu.Lock()
		defer reg.mu.Unlock()
		if !g.app.canMutateRoutesLocked("register routes") {
			return
		}
		g.node.routes = append(g.node.routes, RouteDef{
			Method:         method,
			Pattern:        pattern,
			Handler:        h,
			RequestTimeout: timeout,
		})
		reg.markDirtyLocked()
		return
	}

	g.node.routes = append(g.node.routes, RouteDef{
		Method:         method,
		Pattern:        pattern,
		Handler:        h,
		RequestTimeout: timeout,
	})
}

// Use appends standard net/http middleware to this group.
// These run at the HTTP layer before the pi Handler is invoked.
func (g *RouteGroup) Use(mws ...func(http.Handler) http.Handler) *RouteGroup {
	if g == nil || g.node == nil {
		return g
	}
	if reg := g.registry(); reg != nil {
		reg.mu.Lock()
		defer reg.mu.Unlock()
		if !g.app.canMutateRoutesLocked("register HTTP middlewares") {
			return g
		}
		g.node.httpMWs = append(g.node.httpMWs, mws...)
		reg.markDirtyLocked()
		return g
	}

	g.node.httpMWs = append(g.node.httpMWs, mws...)
	return g
}

// UseMiddleware appends PiMiddleware to this group.
// These run at the application layer with full *Context access.
func (g *RouteGroup) UseMiddleware(mws ...PiMiddleware) *RouteGroup {
	if g == nil || g.node == nil {
		return g
	}
	if reg := g.registry(); reg != nil {
		reg.mu.Lock()
		defer reg.mu.Unlock()
		if !g.app.canMutateRoutesLocked("register Pi middlewares") {
			return g
		}
		g.node.piMWs = append(g.node.piMWs, mws...)
		reg.markDirtyLocked()
		return g
	}

	g.node.piMWs = append(g.node.piMWs, mws...)
	return g
}

// Group creates or gets a child route group with the given prefix and returns it.
// An optional callback can be provided for backward-compatible inline registration.
func (g *RouteGroup) Group(prefix string, fns ...func(sub *RouteGroup)) *RouteGroup {
	if g == nil || g.node == nil {
		return g
	}

	validCallbacks := make([]func(sub *RouteGroup), 0, len(fns))
	for _, fn := range fns {
		if fn == nil {
			if g.app != nil {
				if l := g.app.safeLogger(); l != nil {
					l.Error("route group callback cannot be nil")
				}
			}
			continue
		}

		validCallbacks = append(validCallbacks, fn)
	}

	// Preserve old behavior for explicit nil callback: log and don't create groups.
	if len(fns) > 0 && len(validCallbacks) == 0 {
		return g
	}

	normalizedPrefix := normalizeGroupPrefix(prefix)

	var sub *RouteGroup
	if reg := g.registry(); reg != nil {
		reg.mu.Lock()
		if !g.app.canMutateRoutesLocked("create route groups") {
			reg.mu.Unlock()
			return g
		}
		if normalizedPrefix == "" {
			sub = g
		} else {
			child := g.node.getOrCreateChild(normalizedPrefix)
			reg.markDirtyLocked()
			sub = &RouteGroup{node: child, app: g.app}
		}
		reg.mu.Unlock()
	} else {
		if normalizedPrefix == "" {
			sub = g
		} else {
			child := g.node.getOrCreateChild(normalizedPrefix)
			sub = &RouteGroup{node: child, app: g.app}
		}
	}

	for _, fn := range validCallbacks {
		fn(sub)
	}

	return sub
}

// Scope creates an isolated child RouteGroup with the same URL prefix as g.
// Unlike Group(""), Scope always allocates an isolated child node so middleware
// and WithDoc attributes attached to the returned scope never leak back to g
// or sibling scopes.
func (g *RouteGroup) Scope(fns ...func(sub *RouteGroup)) *RouteGroup {
	if g == nil || g.node == nil {
		return &RouteGroup{node: &GroupNode{isScope: true}}
	}

	var sub *RouteGroup
	if reg := g.registry(); reg != nil {
		reg.mu.Lock()
		if !g.app.canMutateRoutesLocked("create route scope") {
			reg.mu.Unlock()
			return &RouteGroup{node: &GroupNode{isScope: true}, app: g.app}
		}
		g.app.ensureHTTPAvailable()
		child := &GroupNode{prefix: "", parent: g.node, isScope: true}
		g.node.children = append(g.node.children, child)
		reg.markDirtyLocked()
		sub = &RouteGroup{node: child, app: g.app}
		reg.mu.Unlock()
	} else {
		child := &GroupNode{prefix: "", parent: g.node, isScope: true}
		g.node.children = append(g.node.children, child)
		sub = &RouteGroup{node: child, app: g.app}
	}

	for _, fn := range fns {
		if fn == nil {
			if g.app != nil {
				if l := g.app.safeLogger(); l != nil {
					l.Error("route scope callback cannot be nil")
				}
			}
			continue
		}
		fn(sub)
	}

	return sub
}

// ---------- RouteRegistry: compilation to chi ----------

// compile walks the GroupNode tree and registers all routes and middleware on the chi router.
func (reg *RouteRegistry) compile(router chi.Router, rt *runtimeKernel, defaultTimeout time.Duration) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.compileLocked(router, rt, defaultTimeout)
}

func (reg *RouteRegistry) compileLocked(router chi.Router, rt *runtimeKernel, defaultTimeout time.Duration) {
	if reg.compiled {
		return
	}
	reg.compiled = true
	reg.compileNode(reg.root, router, rt, defaultTimeout, "", nil)
}

func collectFoldedNode(node *GroupNode) (
	httpMWs []func(http.Handler) http.Handler,
	piMWs []PiMiddleware,
	routes []RouteDef,
	metas []*routeMeta,
	children []*GroupNode,
) {
	if node == nil {
		return nil, nil, nil, nil, nil
	}

	httpMWs = append(httpMWs, node.httpMWs...)
	piMWs = append(piMWs, node.piMWs...)
	routes = append(routes, node.routes...)
	metas = make([]*routeMeta, 0, len(node.routes))
	for i := range node.routes {
		metas = append(metas, node.routeMetaAt(i))
	}

	children = make([]*GroupNode, 0, len(node.children))
	for _, child := range node.children {
		if child == nil {
			continue
		}

		childPrefix := normalizeGroupPrefix(child.prefix)
		if !child.isScope && childPrefix == "" {
			cHTTP, cPi, cRoutes, cMetas, cChildren := collectFoldedNode(child)
			httpMWs = append(httpMWs, cHTTP...)
			piMWs = append(piMWs, cPi...)
			routes = append(routes, cRoutes...)
			metas = append(metas, cMetas...)
			children = append(children, cChildren...)
			continue
		}

		c := child
		if c.prefix != childPrefix {
			c = cloneShallowGroupNode(c)
			c.prefix = childPrefix
		}
		children = append(children, c)
	}

	children = mergeChildrenByPrefix(children)
	return httpMWs, piMWs, routes, metas, children
}

func joinGroupPrefixes(parentPrefix, childPrefix string) string {
	if parentPrefix == "" {
		return childPrefix
	}
	if childPrefix == "" {
		return parentPrefix
	}
	return parentPrefix + childPrefix
}

func joinGroupRoutePattern(fullPrefix, pattern string) string {
	trimmed := strings.TrimSpace(pattern)
	if trimmed == "" || trimmed == "/" {
		if fullPrefix == "" {
			return "/"
		}
		return fullPrefix
	}
	if !strings.HasPrefix(trimmed, "/") {
		trimmed = "/" + trimmed
	}
	return fullPrefix + trimmed
}

func (reg *RouteRegistry) compileNode(
	node *GroupNode,
	router chi.Router,
	rt *runtimeKernel,
	defaultTimeout time.Duration,
	fullPrefix string,
	inheritedPiMWs []PiMiddleware,
) {
	if node == nil {
		return
	}

	// Recursively fold any empty-prefix children into this node before compiling.
	nodeHTTPMWs, nodePiMWs, nodeRoutes, nodeMetas, children := collectFoldedNode(node)

	nodePrefix := normalizeGroupPrefix(node.prefix)
	curPrefix := joinGroupPrefixes(fullPrefix, nodePrefix)

	hasRoutes := len(nodeRoutes) > 0
	if !hasRoutes {
		for _, child := range children {
			if groupNodeHasRoutes(child) {
				hasRoutes = true
				break
			}
		}
	}
	if node != reg.root && !hasRoutes {
		return
	}

	// Accumulate pi middleware: inherited from parent + this node's own.
	allPiMWs := make([]PiMiddleware, 0, len(inheritedPiMWs)+len(nodePiMWs))
	allPiMWs = append(allPiMWs, inheritedPiMWs...)
	allPiMWs = append(allPiMWs, nodePiMWs...)

	validHTTPMWs := make([]func(http.Handler) http.Handler, 0, len(nodeHTTPMWs))
	for _, mw := range nodeHTTPMWs {
		if mw != nil {
			validHTTPMWs = append(validHTTPMWs, mw)
		}
	}

	if node == reg.root {
		// Root node: apply HTTP middlewares via Use before any With/Method calls.
		for _, mw := range validHTTPMWs {
			router.Use(mw)
		}

		for _, child := range children {
			if !groupNodeHasRoutes(child) {
				continue
			}
			reg.compileNode(child, router, rt, defaultTimeout, "", allPiMWs)
		}

		reg.registerRoutes(router, "", nodeRoutes, nodeMetas, rt, defaultTimeout, allPiMWs)
		return
	}

	// Scoped group: use chi's inline With() mux instead of Route() (chi.Mount)
	// so nested and flat groups share the root radix tree without catch-all shadowing.
	scoped := router
	if len(validHTTPMWs) > 0 {
		scoped = router.With(validHTTPMWs...)
	}

	for _, child := range children {
		if !groupNodeHasRoutes(child) {
			continue
		}
		reg.compileNode(child, scoped, rt, defaultTimeout, curPrefix, allPiMWs)
	}

	reg.registerRoutes(scoped, curPrefix, nodeRoutes, nodeMetas, rt, defaultTimeout, allPiMWs)
}

func groupNodeHasRoutes(node *GroupNode) bool {
	if node == nil {
		return false
	}
	if len(node.routes) > 0 {
		return true
	}
	for _, child := range node.children {
		if groupNodeHasRoutes(child) {
			return true
		}
	}
	return false
}

func (reg *RouteRegistry) registerRoutes(
	router chi.Router,
	fullPrefix string,
	routes []RouteDef,
	metas []*routeMeta,
	rt *runtimeKernel,
	defaultTimeout time.Duration,
	piMWs []PiMiddleware,
) {
	for i, rd := range routes {
		var meta *routeMeta
		if i < len(metas) {
			meta = metas[i]
		}

		timeout := rd.RequestTimeout
		if timeout == 0 {
			timeout = defaultTimeout
		}

		var routePiMWs []PiMiddleware
		if len(piMWs) > 0 || (meta != nil && len(meta.piMWs) > 0) {
			extra := 0
			if meta != nil {
				extra = len(meta.piMWs)
			}
			routePiMWs = make([]PiMiddleware, 0, len(piMWs)+extra)
			routePiMWs = append(routePiMWs, piMWs...)
			if meta != nil {
				routePiMWs = append(routePiMWs, meta.piMWs...)
			}
		}

		composedFn := composePiMiddleware(routePiMWs, rd.Handler)

		h := handler{
			function:       composedFn,
			rt:             rt,
			requestTimeout: timeout,
			maxBodyBytes:   rd.MaxBodyBytes,
		}

		var providerFn func() trace.TracerProvider
		isolateMetrics := false
		if rt != nil {
			providerFn = rt.getTracerProvider
			isolateMetrics = rt.shouldIsolateMetrics()
		}
		var routeHTTP http.Handler = piHTTP.NewRouteHTTPHandler(h, "pi-router", providerFn, isolateMetrics)
		if meta != nil && len(meta.httpMWs) > 0 {
			for j := len(meta.httpMWs) - 1; j >= 0; j-- {
				if meta.httpMWs[j] != nil {
					routeHTTP = meta.httpMWs[j](routeHTTP)
				}
			}
		}
		fullPattern := joinGroupRoutePattern(fullPrefix, rd.Pattern)
		router.Method(rd.Method, fullPattern, routeHTTP)
	}
}

// composePiMiddleware chains a slice of PiMiddleware around a final Handler,
// applying them in declaration order (first middleware is outermost).
func composePiMiddleware(mws []PiMiddleware, final Handler) Handler {
	if len(mws) == 0 {
		return final
	}

	h := final
	for i := len(mws) - 1; i >= 0; i-- {
		if mws[i] == nil {
			continue
		}
		h = mws[i](h)
	}

	return h
}

func (g *RouteGroup) canMutate(action string) bool {
	if g.app == nil {
		return true
	}

	return g.app.canMutateRoutes(action)
}

func (g *GroupNode) getOrCreateChild(prefix string) *GroupNode {
	for _, child := range g.children {
		if child != nil && !child.isScope && child.prefix == prefix {
			child.parent = g
			return child
		}
	}

	child := &GroupNode{prefix: prefix, parent: g}
	g.children = append(g.children, child)

	return child
}

func cloneShallowGroupNode(src *GroupNode) *GroupNode {
	if src == nil {
		return nil
	}
	clone := &GroupNode{
		prefix:   src.prefix,
		parent:   src.parent,
		isScope:  src.isScope,
		httpMWs:  append([]func(http.Handler) http.Handler(nil), src.httpMWs...),
		piMWs:    append([]PiMiddleware(nil), src.piMWs...),
		routes:   append([]RouteDef(nil), src.routes...),
		doc:      src.doc,
		children: append([]*GroupNode(nil), src.children...),
	}
	if len(src.routeMetas) > 0 {
		clone.routeMetas = make(map[int]*routeMeta, len(src.routeMetas))
		for idx, meta := range src.routeMetas {
			clone.routeMetas[idx] = meta
		}
	}
	return clone
}

func (g *GroupNode) mergeFrom(other *GroupNode) {
	if other == nil {
		return
	}

	baseIdx := len(g.routes)
	g.httpMWs = append(g.httpMWs, other.httpMWs...)
	g.piMWs = append(g.piMWs, other.piMWs...)
	g.routes = append(g.routes, other.routes...)
	for idx, meta := range other.routeMetas {
		g.setRouteMeta(baseIdx+idx, meta)
	}
	g.children = append(g.children, other.children...)
}

func mergeChildrenByPrefix(children []*GroupNode) []*GroupNode {
	merged := make([]*GroupNode, 0, len(children))
	mergedCloned := make([]bool, 0, len(children))
	indexByPrefix := make(map[string]int, len(children))

	for _, child := range children {
		if child == nil {
			continue
		}
		if child.isScope {
			merged = append(merged, child)
			mergedCloned = append(mergedCloned, false)
			continue
		}

		if idx, ok := indexByPrefix[child.prefix]; ok {
			if !mergedCloned[idx] {
				merged[idx] = cloneShallowGroupNode(merged[idx])
				mergedCloned[idx] = true
			}
			merged[idx].mergeFrom(child)
			continue
		}

		indexByPrefix[child.prefix] = len(merged)
		merged = append(merged, child)
		mergedCloned = append(mergedCloned, false)
	}

	return merged
}

func normalizeGroupPrefix(prefix string) string {
	return contract.NormalizeGroupPrefix(prefix)
}
