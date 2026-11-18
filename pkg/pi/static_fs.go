package pi

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	iofs "io/fs"
	"math"
	"mime"
	"net/http"
	"net/url"
	"path"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/sllt/pi/pkg/pi/apperror"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/contract"
	"github.com/sllt/pi/pkg/pi/http/response"
	"github.com/sllt/pi/pkg/pi/logging"
)

const maxStaticETagFileSize int64 = 32 << 20 // 32 MiB

// StaticOption configures App.StaticFS and RouteGroup.StaticFS behavior.
type StaticOption func(*staticFSConfig) error

type staticFSConfig struct {
	subDir            string
	spaFallback       bool
	indexFile         string
	excludePrefixes   []string
	cacheControl      string
	immutablePrefixes []string
	precompressed     bool
}

// WithStaticSubDir scopes the mounted filesystem to a validated relative subdirectory
// (such as "dist" for an embed.FS), surfacing missing or invalid subdirectories via App.RouteError.
func WithStaticSubDir(dir string) StaticOption {
	return func(cfg *staticFSConfig) error {
		trimmed := strings.TrimSpace(dir)
		if trimmed == "" || trimmed == "." {
			return fmt.Errorf("staticfs: subdir must not be empty")
		}
		if strings.HasPrefix(trimmed, "/") || strings.Contains(trimmed, "\\") || strings.ContainsRune(trimmed, 0) {
			return fmt.Errorf("staticfs: invalid subdir %q", dir)
		}
		for _, seg := range strings.Split(trimmed, "/") {
			if seg == ".." {
				return fmt.Errorf("staticfs: invalid subdir %q: path traversal not allowed", dir)
			}
		}
		cleaned := path.Clean(trimmed)
		if cleaned == "." {
			return fmt.Errorf("staticfs: subdir must not be empty")
		}
		if cleaned == ".." || strings.HasPrefix(cleaned, "../") || strings.HasPrefix(cleaned, "/") || !iofs.ValidPath(cleaned) || hasDotSegment(cleaned) {
			return fmt.Errorf("staticfs: invalid subdir %q", dir)
		}
		cfg.subDir = cleaned
		return nil
	}
}

// WithSPAFallback enables Single Page Application fallback to indexFile (default "index.html")
// for non-excluded, extensionless routes that do not match an existing file in fsys.
func WithSPAFallback(indexFile ...string) StaticOption {
	return func(cfg *staticFSConfig) error {
		if len(indexFile) > 1 {
			return fmt.Errorf("static FS: WithSPAFallback accepts at most one index file, got %d", len(indexFile))
		}
		raw := "index.html"
		if len(indexFile) == 1 {
			raw = indexFile[0]
		}
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			trimmed = "index.html"
		}
		if strings.HasPrefix(trimmed, "/") || strings.Contains(trimmed, "\\") || strings.ContainsRune(trimmed, 0) {
			return fmt.Errorf("static FS: invalid SPA fallback index file %q", raw)
		}
		for _, seg := range strings.Split(trimmed, "/") {
			if seg == ".." {
				return fmt.Errorf("static FS: invalid SPA fallback index file %q: path traversal not allowed", raw)
			}
		}
		cleaned := path.Clean(trimmed)
		if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || strings.HasPrefix(cleaned, "/") || !iofs.ValidPath(cleaned) || hasDotSegment(cleaned) {
			return fmt.Errorf("static FS: invalid SPA fallback index file %q", raw)
		}
		cfg.spaFallback = true
		cfg.indexFile = cleaned
		return nil
	}
}

// WithStaticExclude excludes URL path prefixes (e.g. "/api") from static serving and SPA fallback,
// delegating unmatched requests under those prefixes to Pi's standard JSON 404 handler.
func WithStaticExclude(prefixes ...string) StaticOption {
	return func(cfg *staticFSConfig) error {
		for _, p := range prefixes {
			trimmed := strings.TrimSpace(p)
			if trimmed == "" {
				continue
			}
			norm, err := normalizeStaticPrefix(trimmed)
			if err != nil {
				return fmt.Errorf("static FS exclude prefix: %w", err)
			}
			if norm == "" {
				continue
			}
			cfg.excludePrefixes = append(cfg.excludePrefixes, norm)
		}
		return nil
	}
}

// WithStaticCacheControl sets the default Cache-Control header value for non-immutable,
// non-SPA-index static files when headerValue is non-empty.
func WithStaticCacheControl(headerValue string) StaticOption {
	return func(cfg *staticFSConfig) error {
		cfg.cacheControl = strings.TrimSpace(headerValue)
		return nil
	}
}

// WithImmutableAssets marks relative prefixes inside the static mount (e.g. "/assets" or "assets")
// to be served with "Cache-Control: public, max-age=31536000, immutable".
func WithImmutableAssets(prefixes ...string) StaticOption {
	return func(cfg *staticFSConfig) error {
		for _, p := range prefixes {
			trimmed := strings.Trim(strings.TrimSpace(p), "/")
			if trimmed == "" {
				continue
			}
			if strings.Contains(trimmed, "\\") || strings.ContainsRune(trimmed, 0) {
				return fmt.Errorf("static FS: invalid immutable assets prefix %q", p)
			}
			for _, seg := range strings.Split(trimmed, "/") {
				if seg == ".." {
					return fmt.Errorf("static FS: invalid immutable assets prefix %q: path traversal not allowed", p)
				}
			}
			cleaned := path.Clean(trimmed)
			if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || !iofs.ValidPath(cleaned) {
				return fmt.Errorf("static FS: invalid immutable assets prefix %q", p)
			}
			cfg.immutablePrefixes = append(cfg.immutablePrefixes, cleaned)
		}
		return nil
	}
}

// WithStaticPrecompressed enables or disables automatic .br and .gz companion file negotiation
// (enabled by default).
func WithStaticPrecompressed(enabled bool) StaticOption {
	return func(cfg *staticFSConfig) error {
		cfg.precompressed = enabled
		return nil
	}
}

// StaticFS mounts an iofs.FS (such as embed.FS) at prefix on the application's HTTP router.
// Registration and validation errors are recorded on the App and surfaced via RouteError,
// HTTPHandler, OpenAPISpec, and Start.
func (a *App) StaticFS(prefix string, fsys iofs.FS, opts ...StaticOption) {
	if a == nil || a.httpServer == nil || a.httpServer.registry == nil {
		return
	}
	a.rootRouteGroup().StaticFS(prefix, fsys, opts...)
}

// StaticFS mounts an iofs.FS (such as embed.FS) at prefix relative to this RouteGroup,
// inheriting the group's prefix and HTTP/Pi middlewares.
func (g *RouteGroup) StaticFS(prefix string, fsys iofs.FS, opts ...StaticOption) {
	if g == nil || g.node == nil || g.app == nil {
		return
	}
	reg := g.registry()
	if reg == nil {
		return
	}

	reg.mu.Lock()
	defer reg.mu.Unlock()

	if !g.app.canMutateRoutesLocked("register static FS") {
		return
	}

	localPrefix, err := normalizeStaticPrefix(prefix)
	if err != nil {
		g.app.recordRouteErrorLocked(err)
		return
	}

	groupPrefix := groupFullPrefix(g.node)
	mountPrefix := joinNormalizedPrefixes(groupPrefix, localPrefix)
	displayPrefix := mountPrefix
	if displayPrefix == "" {
		displayPrefix = "/"
	}

	if strings.ContainsAny(mountPrefix, "{}*") {
		g.app.recordRouteErrorLocked(fmt.Errorf("invalid static FS mount prefix %q: path parameters and wildcards are not allowed", displayPrefix))
		return
	}

	if mountPrefix == "/.well-known" || strings.HasPrefix(mountPrefix, "/.well-known/") {
		g.app.recordRouteErrorLocked(fmt.Errorf("static FS %q cannot be mounted on reserved /.well-known prefix", displayPrefix))
		return
	}

	if fsys == nil {
		g.app.recordRouteErrorLocked(fmt.Errorf("static FS %q: filesystem must not be nil", displayPrefix))
		return
	}

	cfg := staticFSConfig{
		precompressed: true,
	}
	for i, opt := range opts {
		if opt == nil {
			g.app.recordRouteErrorLocked(fmt.Errorf("static FS %q: option at index %d must not be nil", displayPrefix, i))
			return
		}
		if err := opt(&cfg); err != nil {
			g.app.recordRouteErrorLocked(fmt.Errorf("static FS %q: %w", displayPrefix, err))
			return
		}
	}

	if reg.staticMounts != nil {
		if _, exists := reg.staticMounts[mountPrefix]; exists {
			g.app.recordRouteErrorLocked(fmt.Errorf("static FS %q is already registered", displayPrefix))
			return
		}
	}

	if cfg.subDir != "" {
		if hasSymlinkAlongPath(fsys, cfg.subDir) {
			g.app.recordRouteErrorLocked(fmt.Errorf("static FS %q: subdir %q must not traverse symlinks", displayPrefix, cfg.subDir))
			return
		}
		subFS, subErr := iofs.Sub(fsys, cfg.subDir)
		if subErr != nil {
			g.app.recordRouteErrorLocked(fmt.Errorf("static FS %q: subdir %q: %w", displayPrefix, cfg.subDir, subErr))
			return
		}
		subInfo, statErr := iofs.Stat(subFS, ".")
		if statErr != nil {
			g.app.recordRouteErrorLocked(fmt.Errorf("static FS %q: subdir %q: %w", displayPrefix, cfg.subDir, statErr))
			return
		}
		if !subInfo.IsDir() {
			g.app.recordRouteErrorLocked(fmt.Errorf("static FS %q: subdir %q must be a directory", displayPrefix, cfg.subDir))
			return
		}
		fsys = subFS
	}

	if cfg.spaFallback {
		info, statErr := iofs.Stat(fsys, cfg.indexFile)
		if statErr != nil {
			g.app.recordRouteErrorLocked(fmt.Errorf("static FS %q: SPA fallback index file %q: %w", displayPrefix, cfg.indexFile, statErr))
			return
		}
		if info.IsDir() {
			g.app.recordRouteErrorLocked(fmt.Errorf("static FS %q: SPA fallback index file %q must not be a directory", displayPrefix, cfg.indexFile))
			return
		}
	}

	if reg.staticMounts == nil {
		reg.staticMounts = make(map[string]struct{})
	}
	reg.staticMounts[mountPrefix] = struct{}{}

	staticH := newStaticFSHandler(mountPrefix, fsys, cfg)

	var patterns []string
	switch {
	case mountPrefix == "":
		patterns = []string{"/*"}
	case localPrefix == "":
		patterns = []string{"/", "/*"}
	default:
		patterns = []string{localPrefix, localPrefix + "/*"}
	}

	for _, pat := range patterns {
		fullPat := joinGroupRoutePattern(groupPrefix, pat)
		staticH.ownPatterns[fullPat] = struct{}{}
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			idx := len(g.node.routes)
			g.node.routes = append(g.node.routes, RouteDef{
				Method:  method,
				Pattern: pat,
			})
			g.node.setRouteMeta(idx, &routeMeta{
				isSystem:      true,
				staticHandler: staticH,
			})
		}
	}

	g.app.ensureHTTPAvailable()
	reg.markDirtyLocked()
}

func normalizeStaticPrefix(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "/" {
		return "", nil
	}
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if ch <= 0x20 || ch == 0x7f || ch == '?' || ch == '#' || ch == '\\' || ch == '{' || ch == '}' || ch == '*' {
			return "", fmt.Errorf("invalid static FS prefix %q", raw)
		}
	}
	for _, seg := range strings.Split(strings.Trim(trimmed, "/"), "/") {
		if seg == "." || seg == ".." {
			return "", fmt.Errorf("invalid static FS prefix %q: relative segment not allowed", raw)
		}
	}
	return contract.NormalizeGroupPrefix(trimmed), nil
}

type staticFSHandler struct {
	mountPrefix     string
	fsys            iofs.FS
	cfg             staticFSConfig
	excludePrefixes []string
	ownPatterns     map[string]struct{}
	etags           sync.Map // clean fs path -> quoted ETag string
}

func newStaticFSHandler(mountPrefix string, fsys iofs.FS, cfg staticFSConfig) *staticFSHandler {
	excludes := make([]string, 0, len(cfg.excludePrefixes)+1)
	seen := make(map[string]struct{}, len(cfg.excludePrefixes)+1)
	addExclude := func(p string) {
		if p == "" {
			return
		}
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		excludes = append(excludes, p)
	}
	addExclude("/.well-known")
	for _, p := range cfg.excludePrefixes {
		addExclude(p)
	}
	return &staticFSHandler{
		mountPrefix:     mountPrefix,
		fsys:            fsys,
		cfg:             cfg,
		excludePrefixes: excludes,
		ownPatterns:     make(map[string]struct{}, 2),
	}
}

func staticRouteHTTPHandler(staticH *staticFSHandler, rt *runtimeKernel, piMWs []PiMiddleware) http.Handler {
	if len(piMWs) == 0 {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			staticH.serveHTTP(w, r, rt)
		})
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		policy := piHTTP.LegacyStatus
		if rt != nil && rt.explicitHTTPStatus {
			policy = piHTTP.ExplicitStatus
		}
		responder := piHTTP.NewResponderForRequest(w, r, policy)
		c := newContext(responder, piHTTP.NewRequest(r), rt)

		statusW := &statusTrackingWriter{ResponseWriter: w}

		inner := func(ctx *Context) (any, error) {
			req := r
			if ctx != nil && ctx.Context != nil && ctx.Context != r.Context() {
				req = r.WithContext(ctx.Context)
			}
			staticH.serveHTTP(statusW, req, rt)
			return response.Handled{}, nil
		}
		composed := composePiMiddleware(piMWs, inner)

		var (
			data any
			err  error
		)
		func() {
			defer func() {
				if re := recover(); re != nil {
					err = piHTTP.ErrorPanicRecovery{}
					if rt != nil {
						rt.getLogger().Error(panicLog{
							Error:      fmt.Sprint(re),
							StackTrace: string(debug.Stack()),
						})
					}
				}
			}()
			data, err = composed(c)
		}()

		if statusW.wroteHeader {
			return
		}
		if err != nil {
			responder.Respond(nil, err)
			return
		}
		if _, ok := data.(response.Handled); ok {
			return
		}
		responder.Respond(data, nil)
	})
}

type statusTrackingWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

func (w *statusTrackingWriter) WriteHeader(code int) {
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusTrackingWriter) Write(p []byte) (int, error) {
	w.wroteHeader = true
	return w.ResponseWriter.Write(p)
}

func (w *statusTrackingWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (h *staticFSHandler) serveHTTP(w http.ResponseWriter, r *http.Request, rt *runtimeKernel) {
	if hasUnsafeStaticRequestPath(r) {
		h.serveNotFound(w, r, rt)
		return
	}

	reqPath := "/"
	if r.URL != nil && r.URL.Path != "" {
		reqPath = r.URL.Path
	}
	if !strings.HasPrefix(reqPath, "/") {
		reqPath = "/" + reqPath
	}

	if isExcludedStaticPath(reqPath, h.excludePrefixes) {
		h.serveUnmatched(w, r, rt, reqPath, false)
		return
	}

	rel := reqPath
	if h.mountPrefix != "" {
		if reqPath != h.mountPrefix && !strings.HasPrefix(reqPath, h.mountPrefix+"/") {
			h.serveUnmatched(w, r, rt, reqPath, false)
			return
		}
		rel = strings.TrimPrefix(reqPath, h.mountPrefix)
	}
	rel = strings.TrimPrefix(rel, "/")

	cleanRel := "."
	if rel != "" {
		cleanRel = path.Clean(rel)
	}
	if !iofs.ValidPath(cleanRel) {
		h.serveNotFound(w, r, rt)
		return
	}

	if hasDotSegment(cleanRel) {
		h.serveNotFound(w, r, rt)
		return
	}

	if path.Base(cleanRel) == piHTTP.DefaultSwaggerFileName {
		h.serveForbidden(w, r, rt)
		return
	}

	if hasSymlinkAlongPath(h.fsys, cleanRel) {
		h.serveNotFound(w, r, rt)
		return
	}

	info, err := iofs.Stat(h.fsys, cleanRel)
	if err == nil && info.IsDir() {
		indexCandidate := "index.html"
		isSPA := false
		if cleanRel == "." {
			if h.cfg.spaFallback {
				indexCandidate = h.cfg.indexFile
				isSPA = true
			}
		} else {
			indexCandidate = path.Join(cleanRel, "index.html")
			isSPA = h.cfg.spaFallback && indexCandidate == h.cfg.indexFile
		}

		if hasSymlinkAlongPath(h.fsys, indexCandidate) {
			h.serveNotFound(w, r, rt)
			return
		}
		if idxInfo, idxErr := iofs.Stat(h.fsys, indexCandidate); idxErr == nil && !idxInfo.IsDir() {
			h.serveFile(w, r, rt, indexCandidate, isSPA)
			return
		}
		h.serveUnmatched(w, r, rt, reqPath, h.cfg.spaFallback && !hasFileExtension(cleanRel))
		return
	}

	if err == nil && !info.IsDir() {
		isSPA := h.cfg.spaFallback && cleanRel == h.cfg.indexFile
		h.serveFile(w, r, rt, cleanRel, isSPA)
		return
	}

	h.serveUnmatched(w, r, rt, reqPath, h.cfg.spaFallback && !hasFileExtension(cleanRel))
}

func (h *staticFSHandler) serveUnmatched(w http.ResponseWriter, r *http.Request, rt *runtimeKernel, reqPath string, allowSPA bool) {
	if allowed := h.probeAllowedNonStaticMethods(r, reqPath); len(allowed) > 0 {
		h.serveMethodNotAllowed(w, r, rt, allowed)
		return
	}
	if allowSPA {
		h.serveFile(w, r, rt, h.cfg.indexFile, true)
		return
	}
	h.serveNotFound(w, r, rt)
}

func (h *staticFSHandler) serveFile(w http.ResponseWriter, r *http.Request, rt *runtimeKernel, origPath string, isSPAIndex bool) {
	if hasSymlinkAlongPath(h.fsys, origPath) {
		h.serveNotFound(w, r, rt)
		return
	}
	origInfo, err := iofs.Stat(h.fsys, origPath)
	if err != nil || origInfo.IsDir() {
		h.serveNotFound(w, r, rt)
		return
	}

	servePath := origPath
	encoding := ""

	if h.cfg.precompressed {
		hasBr := hasValidStaticFile(h.fsys, origPath+".br")
		hasGz := hasValidStaticFile(h.fsys, origPath+".gz")
		if hasBr || hasGz {
			addVaryHeader(w.Header(), "Accept-Encoding")
		}
		if r.Header.Get("Range") == "" {
			aeValues := r.Header.Values("Accept-Encoding")
			if hasBr && acceptsEncodingValues(aeValues, "br") {
				servePath = origPath + ".br"
				encoding = "br"
			} else if hasGz && acceptsEncodingValues(aeValues, "gzip") {
				servePath = origPath + ".gz"
				encoding = "gzip"
			}
		}
	}

	f, err := h.fsys.Open(servePath)
	if err != nil {
		h.serveNotFound(w, r, rt)
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil || info.IsDir() {
		h.serveNotFound(w, r, rt)
		return
	}

	contentType := h.resolveContentType(origPath, encoding != "")
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}

	if isSPAIndex {
		w.Header().Set("Cache-Control", "no-cache")
	} else if isImmutableAsset(origPath, h.cfg.immutablePrefixes) {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else if h.cfg.cacheControl != "" {
		w.Header().Set("Cache-Control", h.cfg.cacheControl)
	}

	if encoding != "" {
		w.Header().Set("Content-Encoding", encoding)
	}

	modTime := info.ModTime()
	var etag string
	var preloaded []byte

	if modTime.IsZero() && info.Size() >= 0 && info.Size() <= maxStaticETagFileSize {
		var ok bool
		etag, preloaded, ok = h.getOrComputeETag(servePath, f)
		if ok && etag != "" {
			w.Header().Set("ETag", etag)
		}
	}

	if rs, ok := f.(io.ReadSeeker); ok {
		http.ServeContent(w, r, path.Base(origPath), modTime, rs)
		return
	}
	if preloaded != nil {
		http.ServeContent(w, r, path.Base(origPath), modTime, bytes.NewReader(preloaded))
		return
	}

	h.serveNonSeekable(w, r, f, info, modTime, etag, contentType)
}

func (h *staticFSHandler) resolveContentType(origPath string, isCompressed bool) string {
	if ctype := mime.TypeByExtension(strings.ToLower(path.Ext(origPath))); ctype != "" {
		return ctype
	}
	if !isCompressed {
		return ""
	}
	uf, err := h.fsys.Open(origPath)
	if err != nil {
		return "application/octet-stream"
	}
	defer uf.Close()
	var buf [512]byte
	n, _ := io.ReadFull(uf, buf[:])
	if n > 0 {
		return http.DetectContentType(buf[:n])
	}
	return "application/octet-stream"
}

func (h *staticFSHandler) getOrComputeETag(servePath string, f iofs.File) (etag string, preloaded []byte, ok bool) {
	if cached, found := h.etags.Load(servePath); found {
		if s, isStr := cached.(string); isStr && s != "" {
			return s, nil, true
		}
	}

	if rs, isSeekable := f.(io.ReadSeeker); isSeekable {
		hasher := sha256.New()
		n, err := io.Copy(hasher, io.LimitReader(f, maxStaticETagFileSize+1))
		if err != nil || n > maxStaticETagFileSize {
			_, _ = rs.Seek(0, io.SeekStart)
			return "", nil, false
		}
		if _, seekErr := rs.Seek(0, io.SeekStart); seekErr != nil {
			return "", nil, false
		}
		sum := hasher.Sum(nil)
		etag = fmt.Sprintf(`"sha256-%x"`, sum[:8])
		actual, _ := h.etags.LoadOrStore(servePath, etag)
		if s, isStr := actual.(string); isStr && s != "" {
			etag = s
		}
		return etag, nil, true
	}

	data, err := io.ReadAll(io.LimitReader(f, maxStaticETagFileSize+1))
	if err != nil || int64(len(data)) > maxStaticETagFileSize {
		return "", nil, false
	}
	sum := sha256.Sum256(data)
	etag = fmt.Sprintf(`"sha256-%x"`, sum[:8])
	actual, _ := h.etags.LoadOrStore(servePath, etag)
	if s, isStr := actual.(string); isStr && s != "" {
		etag = s
	}
	return etag, data, true
}

func (h *staticFSHandler) serveNonSeekable(
	w http.ResponseWriter,
	r *http.Request,
	f iofs.File,
	info iofs.FileInfo,
	modTime time.Time,
	etag string,
	contentType string,
) {
	var reader io.Reader = f
	if contentType == "" {
		var buf [512]byte
		n, _ := io.ReadFull(f, buf[:])
		if n > 0 {
			contentType = http.DetectContentType(buf[:n])
			w.Header().Set("Content-Type", contentType)
			reader = io.MultiReader(bytes.NewReader(buf[:n]), f)
		} else {
			w.Header().Set("Content-Type", "application/octet-stream")
		}
	}

	if !modTime.IsZero() {
		w.Header().Set("Last-Modified", modTime.UTC().Format(http.TimeFormat))
	}

	if etag != "" && ifNoneMatchMatches(r.Header.Get("If-None-Match"), etag) {
		writeStaticNotModified(w)
		return
	}
	if etag == "" && !modTime.IsZero() {
		if ims := r.Header.Get("If-Modified-Since"); ims != "" {
			if t, parseErr := http.ParseTime(ims); parseErr == nil {
				if !modTime.Truncate(time.Second).After(t) {
					writeStaticNotModified(w)
					return
				}
			}
		}
	}

	if info.Size() >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	}
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = io.Copy(w, reader)
	}
}

func writeStaticNotModified(w http.ResponseWriter) {
	w.Header().Del("Content-Type")
	w.Header().Del("Content-Length")
	w.Header().Del("Content-Encoding")
	w.WriteHeader(http.StatusNotModified)
}

type errorMethodNotAllowed struct{}

func (errorMethodNotAllowed) Error() string           { return "method not allowed" }
func (errorMethodNotAllowed) StatusCode() int         { return http.StatusMethodNotAllowed }
func (errorMethodNotAllowed) LogLevel() logging.Level { return logging.INFO }

var standardStaticProbeMethods = []string{
	http.MethodDelete,
	http.MethodGet,
	http.MethodHead,
	http.MethodOptions,
	http.MethodPatch,
	http.MethodPost,
	http.MethodPut,
}

func (h *staticFSHandler) isStaticRouteMatch(method, pat string) bool {
	if method != http.MethodGet && method != http.MethodHead {
		return false
	}
	_, ok := h.ownPatterns[pat]
	return ok
}

func (h *staticFSHandler) probeAllowedNonStaticMethods(r *http.Request, reqPath string) []string {
	if r == nil {
		return nil
	}
	rctx := chi.RouteContext(r.Context())
	if rctx == nil || rctx.Routes == nil {
		return nil
	}
	probeCtx := chi.NewRouteContext()
	var allowed []string
	for _, method := range standardStaticProbeMethods {
		probeCtx.Reset()
		if !rctx.Routes.Match(probeCtx, method, reqPath) {
			continue
		}
		if h.isStaticRouteMatch(method, probeCtx.RoutePattern()) {
			continue
		}
		allowed = append(allowed, method)
	}
	sort.Strings(allowed)
	return allowed
}

func (h *staticFSHandler) respondError(w http.ResponseWriter, r *http.Request, rt *runtimeKernel, err error) {
	policy := piHTTP.LegacyStatus
	if rt != nil && rt.explicitHTTPStatus {
		policy = piHTTP.ExplicitStatus
	}
	piHTTP.NewResponderForRequest(w, r, policy).Respond(nil, err)
}

func (h *staticFSHandler) serveMethodNotAllowed(w http.ResponseWriter, r *http.Request, rt *runtimeKernel, allowed []string) {
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	h.respondError(w, r, rt, errorMethodNotAllowed{})
}

func (h *staticFSHandler) serveNotFound(w http.ResponseWriter, r *http.Request, rt *runtimeKernel) {
	h.respondError(w, r, rt, piHTTP.ErrorInvalidRoute{})
}

func (h *staticFSHandler) serveForbidden(w http.ResponseWriter, r *http.Request, rt *runtimeKernel) {
	h.respondError(w, r, rt, apperror.New(apperror.Forbidden, http.StatusForbidden, "direct access to openapi.json is forbidden"))
}

func hasUnsafeStaticRequestPath(r *http.Request) bool {
	if r == nil {
		return true
	}
	if r.URL != nil {
		if containsUnsafePathCharsOrTraversal(r.URL.Path) {
			return true
		}
		if r.URL.RawPath != "" && containsUnsafePathCharsOrTraversal(r.URL.RawPath) {
			return true
		}
	}
	if raw := extractRequestURIPath(r.RequestURI); raw != "" {
		if containsUnsafePathCharsOrTraversal(raw) {
			return true
		}
	}
	return false
}

func extractRequestURIPath(rawURI string) string {
	if rawURI == "" {
		return ""
	}
	if idx := strings.IndexAny(rawURI, "?#"); idx >= 0 {
		rawURI = rawURI[:idx]
	}
	if strings.Contains(rawURI, "://") {
		if u, err := url.Parse(rawURI); err == nil {
			return u.Path
		}
	}
	return rawURI
}

func containsUnsafePathCharsOrTraversal(p string) bool {
	if p == "" {
		return false
	}
	if strings.ContainsRune(p, 0) || strings.Contains(p, "\\") {
		return true
	}
	lower := strings.ToLower(p)
	if strings.Contains(lower, "%00") || strings.Contains(lower, "%5c") {
		return true
	}
	if hasDotDotSegment(p) {
		return true
	}
	if unescaped, err := url.PathUnescape(p); err == nil && unescaped != p {
		if strings.ContainsRune(unescaped, 0) || strings.Contains(unescaped, "\\") || hasDotDotSegment(unescaped) {
			return true
		}
	}
	return false
}

func hasDotDotSegment(p string) bool {
	for _, seg := range strings.Split(p, "/") {
		if seg == ".." {
			return true
		}
	}
	return false
}

func isExcludedStaticPath(reqPath string, excludes []string) bool {
	for _, prefix := range excludes {
		if reqPath == prefix || strings.HasPrefix(reqPath, prefix+"/") {
			return true
		}
	}
	return false
}

func isImmutableAsset(cleanRel string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if cleanRel == prefix || strings.HasPrefix(cleanRel, prefix+"/") {
			return true
		}
	}
	return false
}

func hasDotSegment(cleanRel string) bool {
	if cleanRel == "" || cleanRel == "." {
		return false
	}
	for _, seg := range strings.Split(cleanRel, "/") {
		if strings.HasPrefix(seg, ".") {
			return true
		}
	}
	return false
}

func hasSymlinkAlongPath(fsys iofs.FS, relPath string) bool {
	if fsys == nil || relPath == "" || relPath == "." {
		return false
	}
	rlfs, ok := fsys.(iofs.ReadLinkFS)
	if !ok {
		return false
	}
	cleaned := path.Clean(strings.TrimPrefix(relPath, "/"))
	if cleaned == "" || cleaned == "." {
		return false
	}
	cur := ""
	for _, part := range strings.Split(cleaned, "/") {
		if part == "" || part == "." {
			continue
		}
		if cur == "" {
			cur = part
		} else {
			cur = cur + "/" + part
		}
		info, err := rlfs.Lstat(cur)
		if err != nil {
			return false
		}
		if info.Mode()&iofs.ModeSymlink != 0 {
			return true
		}
	}
	return false
}

func hasValidStaticFile(fsys iofs.FS, relPath string) bool {
	if hasSymlinkAlongPath(fsys, relPath) {
		return false
	}
	info, err := iofs.Stat(fsys, relPath)
	return err == nil && !info.IsDir()
}

func hasFileExtension(cleanRel string) bool {
	if cleanRel == "" || cleanRel == "." {
		return false
	}
	return path.Ext(path.Base(cleanRel)) != ""
}

func acceptsEncodingValues(headerValues []string, target string) bool {
	hasExplicitTarget := false
	targetQ := 0.0

	for _, headerVal := range headerValues {
		if strings.TrimSpace(headerVal) == "" {
			continue
		}
		for _, part := range strings.Split(headerVal, ",") {
			item := strings.TrimSpace(part)
			if item == "" {
				continue
			}
			segments := strings.Split(item, ";")
			coding := strings.ToLower(strings.TrimSpace(segments[0]))
			if coding != target {
				continue
			}
			q := 1.0
			valid := true
			for _, param := range segments[1:] {
				k, v, ok := strings.Cut(param, "=")
				if !ok {
					continue
				}
				if strings.ToLower(strings.TrimSpace(k)) == "q" {
					parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
					if err != nil || math.IsNaN(parsed) || parsed < 0 || parsed > 1 {
						valid = false
						break
					}
					q = parsed
				}
			}
			if !valid {
				continue
			}
			if !hasExplicitTarget {
				hasExplicitTarget = true
				targetQ = q
			} else if q == 0 {
				targetQ = 0
			}
		}
	}

	return hasExplicitTarget && targetQ > 0
}

func addVaryHeader(headers http.Header, value string) {
	for _, existing := range headers.Values("Vary") {
		for _, part := range strings.Split(existing, ",") {
			if strings.EqualFold(strings.TrimSpace(part), value) {
				return
			}
		}
	}
	headers.Add("Vary", value)
}

func ifNoneMatchMatches(headerVal, etag string) bool {
	trimmed := strings.TrimSpace(headerVal)
	if trimmed == "" || etag == "" {
		return false
	}
	if trimmed == "*" {
		return true
	}
	target := strings.TrimPrefix(etag, "W/")
	for _, part := range strings.Split(trimmed, ",") {
		cand := strings.TrimSpace(part)
		cand = strings.TrimPrefix(cand, "W/")
		if cand == target {
			return true
		}
	}
	return false
}
