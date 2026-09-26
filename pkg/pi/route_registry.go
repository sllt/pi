package pi

import (
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/sllt/pi/pkg/pi/infra"
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
}

// GroupNode is a node in the route group tree.
// It holds middleware and routes for a given prefix, plus child groups.
type GroupNode struct {
	prefix   string
	httpMWs  []func(http.Handler) http.Handler
	piMWs    []PiMiddleware
	routes   []RouteDef
	children []*GroupNode
}

// RouteGroup is the public API for declaring routes and middleware within a group.
type RouteGroup struct {
	node *GroupNode
	app  *App
}

// RouteRegistry holds the root GroupNode and compiles the tree into chi routes.
type RouteRegistry struct {
	root     *GroupNode
	compiled bool
}

func newRouteRegistry() *RouteRegistry {
	return &RouteRegistry{
		root: &GroupNode{},
	}
}

// ---------- RouteGroup: route registration helpers ----------

// GET registers a handler for HTTP GET on this group.
func (g *RouteGroup) GET(pattern string, h Handler) *RouteGroup {
	g.addRoute("GET", pattern, h, 0)
	return g
}

// POST registers a handler for HTTP POST on this group.
func (g *RouteGroup) POST(pattern string, h Handler) *RouteGroup {
	g.addRoute("POST", pattern, h, 0)
	return g
}

// PUT registers a handler for HTTP PUT on this group.
func (g *RouteGroup) PUT(pattern string, h Handler) *RouteGroup {
	g.addRoute("PUT", pattern, h, 0)
	return g
}

// DELETE registers a handler for HTTP DELETE on this group.
func (g *RouteGroup) DELETE(pattern string, h Handler) *RouteGroup {
	g.addRoute("DELETE", pattern, h, 0)
	return g
}

// PATCH registers a handler for HTTP PATCH on this group.
func (g *RouteGroup) PATCH(pattern string, h Handler) *RouteGroup {
	g.addRoute("PATCH", pattern, h, 0)
	return g
}

func (g *RouteGroup) addRoute(method, pattern string, h Handler, timeout time.Duration) {
	if !g.canMutate("register routes") {
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
	if !g.canMutate("register HTTP middlewares") {
		return g
	}

	g.node.httpMWs = append(g.node.httpMWs, mws...)
	return g
}

// UseMiddleware appends PiMiddleware to this group.
// These run at the application layer with full *Context access.
func (g *RouteGroup) UseMiddleware(mws ...PiMiddleware) *RouteGroup {
	if !g.canMutate("register Pi middlewares") {
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

	if !g.canMutate("create route groups") {
		return g
	}

	validCallbacks := make([]func(sub *RouteGroup), 0, len(fns))
	for _, fn := range fns {
		if fn == nil {
			if g.app != nil && g.app.container != nil {
				g.app.container.Logger.Error("route group callback cannot be nil")
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
	if normalizedPrefix == "" {
		for _, fn := range validCallbacks {
			fn(g)
		}

		return g
	}

	child := g.node.getOrCreateChild(normalizedPrefix)
	sub := &RouteGroup{node: child, app: g.app}
	for _, fn := range validCallbacks {
		fn(sub)
	}

	return sub
}

// ---------- RouteRegistry: compilation to chi ----------

// compile walks the GroupNode tree and registers all routes and middleware on the chi router.
func (reg *RouteRegistry) compile(router chi.Router, container *infra.Container, defaultTimeout time.Duration) {
	if reg.compiled {
		return
	}
	reg.compiled = true
	reg.compileNode(reg.root, router, container, defaultTimeout, nil)
}

func (reg *RouteRegistry) compileNode(
	node *GroupNode,
	router chi.Router,
	container *infra.Container,
	defaultTimeout time.Duration,
	inheritedPiMWs []PiMiddleware,
) {
	if node == nil {
		return
	}

	// RouteGroup APIs avoid creating empty-prefix children, but we still handle any
	// legacy/malformed trees defensively by folding empty-prefix children into parent.
	nodeHTTPMWs := append([]func(http.Handler) http.Handler{}, node.httpMWs...)
	nodePiMWs := append([]PiMiddleware{}, node.piMWs...)
	nodeRoutes := append([]RouteDef{}, node.routes...)
	children := make([]*GroupNode, 0, len(node.children))
	for _, child := range node.children {
		if child == nil {
			continue
		}

		childPrefix := normalizeGroupPrefix(child.prefix)
		if childPrefix == "" {
			nodeHTTPMWs = append(nodeHTTPMWs, child.httpMWs...)
			nodePiMWs = append(nodePiMWs, child.piMWs...)
			nodeRoutes = append(nodeRoutes, child.routes...)
			children = append(children, child.children...)
			continue
		}

		child.prefix = childPrefix
		children = append(children, child)
	}

	children = mergeChildrenByPrefix(children)

	nodePrefix := normalizeGroupPrefix(node.prefix)

	// Accumulate pi middleware: inherited from parent + this node's own.
	allPiMWs := make([]PiMiddleware, 0, len(inheritedPiMWs)+len(nodePiMWs))
	allPiMWs = append(allPiMWs, inheritedPiMWs...)
	allPiMWs = append(allPiMWs, nodePiMWs...)

	if nodePrefix != "" {
		// Scoped group: use chi's Route to create a sub-router.
		router.Route(nodePrefix, func(r chi.Router) {
			// Apply HTTP middleware to the sub-router.
			for _, mw := range nodeHTTPMWs {
				r.Use(mw)
			}

			// Register routes in this group.
			reg.registerRoutes(r, nodeRoutes, container, defaultTimeout, allPiMWs)

			// Recurse into children.
			for _, child := range children {
				reg.compileNode(child, r, container, defaultTimeout, allPiMWs)
			}
		})
	} else {
		// Root node or node without prefix: register directly on the router.
		for _, mw := range nodeHTTPMWs {
			router.Use(mw)
		}

		reg.registerRoutes(router, nodeRoutes, container, defaultTimeout, allPiMWs)

		for _, child := range children {
			reg.compileNode(child, router, container, defaultTimeout, allPiMWs)
		}
	}
}

func (reg *RouteRegistry) registerRoutes(
	router chi.Router,
	routes []RouteDef,
	container *infra.Container,
	defaultTimeout time.Duration,
	piMWs []PiMiddleware,
) {
	for _, rd := range routes {
		timeout := rd.RequestTimeout
		if timeout == 0 {
			timeout = defaultTimeout
		}

		composedFn := composePiMiddleware(piMWs, rd.Handler)

		h := handler{
			function:       composedFn,
			container:      container,
			requestTimeout: timeout,
		}

		otelH := otelhttp.NewHandler(h, "pi-router")
		router.Method(rd.Method, rd.Pattern, otelH)
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
		if child != nil && child.prefix == prefix {
			return child
		}
	}

	child := &GroupNode{prefix: prefix}
	g.children = append(g.children, child)

	return child
}

func (g *GroupNode) mergeFrom(other *GroupNode) {
	if other == nil {
		return
	}

	g.httpMWs = append(g.httpMWs, other.httpMWs...)
	g.piMWs = append(g.piMWs, other.piMWs...)
	g.routes = append(g.routes, other.routes...)
	g.children = append(g.children, other.children...)
}

func mergeChildrenByPrefix(children []*GroupNode) []*GroupNode {
	merged := make([]*GroupNode, 0, len(children))
	indexByPrefix := make(map[string]int, len(children))

	for _, child := range children {
		if child == nil {
			continue
		}

		if idx, ok := indexByPrefix[child.prefix]; ok {
			merged[idx].mergeFrom(child)
			continue
		}

		indexByPrefix[child.prefix] = len(merged)
		merged = append(merged, child)
	}

	return merged
}

func normalizeGroupPrefix(prefix string) string {
	trimmed := strings.TrimSpace(prefix)
	if trimmed == "" {
		return ""
	}

	normalized := path.Clean("/" + strings.TrimLeft(trimmed, "/"))
	if normalized == "." || normalized == "/" {
		return ""
	}

	return normalized
}
