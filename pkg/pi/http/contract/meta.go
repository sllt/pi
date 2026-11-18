package contract

import (
	"fmt"
	"mime"
	"net/http"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/sllt/pi/pkg/pi/openapi"
)

const (
	piPackagePath           = "github.com/sllt/pi/pkg/pi"
	httpResponsePackagePath = "github.com/sllt/pi/pkg/pi/http/response"
)

var forbiddenResponseTypes = map[string]struct{}{
	"Result":   {},
	"Response": {},
	"Raw":      {},
	"Stream":   {},
	"SSE":      {},
	"File":     {},
	"Template": {},
	"XML":      {},
	"Redirect": {},
	"Handled":  {},
}

var supportedMethods = map[string]struct{}{
	"GET":     {},
	"POST":    {},
	"PUT":     {},
	"PATCH":   {},
	"DELETE":  {},
	"HEAD":    {},
	"OPTIONS": {},
}

func isBodylessMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodDelete, http.MethodOptions:
		return true
	default:
		return false
	}
}

var supportedStatuses = map[int]struct{}{
	200: {},
	201: {},
	202: {},
	204: {},
}

// IsPiMetaType reports whether t is github.com/sllt/pi/pkg/pi.Meta.
func IsPiMetaType(t reflect.Type) bool {
	return t != nil && t.PkgPath() == piPackagePath && t.Name() == "Meta" && t.Kind() == reflect.Struct
}

// NormalizeGroupPrefix normalizes a route group prefix so empty/root prefixes
// become "" and non-root prefixes start with "/" without a trailing slash.
func NormalizeGroupPrefix(prefix string) string {
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

// NormalizeRouteConflict computes the canonical path and normalized conflict
// shape ("METHOD /path/{}/...") shared across legacy and typed routes.
func NormalizeRouteConflict(groupPrefix, method, pattern string) (canonicalPath string, conflictShape string) {
	normPrefix := NormalizeGroupPrefix(groupPrefix)
	normMethod := strings.ToUpper(strings.TrimSpace(method))
	trimmedPattern := strings.TrimSpace(pattern)

	switch {
	case trimmedPattern == "" || trimmedPattern == "/":
		if normPrefix == "" {
			canonicalPath = "/"
		} else {
			canonicalPath = normPrefix
		}
	default:
		if !strings.HasPrefix(trimmedPattern, "/") {
			trimmedPattern = "/" + trimmedPattern
		}
		if normPrefix == "" {
			canonicalPath = trimmedPattern
		} else {
			canonicalPath = normPrefix + trimmedPattern
		}
	}

	return canonicalPath, normMethod + " " + NormalizeConflictPath(canonicalPath)
}

// NormalizeConflictPath normalizes path parameters for route conflict detection,
// collapsing plain parameter names ("{id}" -> "{}") while preserving regex
// constraints ("{id:[0-9]+}" -> "{:[0-9]+}") using brace-depth matching.
func NormalizeConflictPath(path string) string {
	return normalizeBracedPath(path, true)
}

// NormalizeOpenAPIPathTemplate normalizes a path into an OpenAPI path template
// shape by replacing every braced parameter segment ("{id}" or "{id:[0-9]+}")
// with "{}" using brace-depth matching.
func NormalizeOpenAPIPathTemplate(path string) string {
	return normalizeBracedPath(path, false)
}

func normalizeBracedPath(rawPath string, preserveRegex bool) string {
	if rawPath == "" {
		return "/"
	}
	var b strings.Builder
	b.Grow(len(rawPath))
	for i := 0; i < len(rawPath); {
		if rawPath[i] == '{' {
			depth := 1
			closeIdx := -1
			for j := i + 1; j < len(rawPath); j++ {
				switch rawPath[j] {
				case '{':
					depth++
				case '}':
					depth--
				}
				if depth == 0 {
					closeIdx = j
					break
				}
			}
			if closeIdx < 0 {
				b.WriteString(rawPath[i:])
				break
			}
			inner := rawPath[i+1 : closeIdx]
			if preserveRegex {
				if colonIdx := strings.IndexByte(inner, ':'); colonIdx >= 0 {
					b.WriteString("{:")
					b.WriteString(inner[colonIdx+1:])
					b.WriteByte('}')
					i = closeIdx + 1
					continue
				}
			}
			b.WriteString("{}")
			i = closeIdx + 1
			continue
		}
		b.WriteByte(rawPath[i])
		i++
	}
	return b.String()
}

type parsedMeta struct {
	method               string
	metaPath             string
	chiPattern           string
	canonicalPath        string
	conflictShape        string
	pathParams           []string
	status               int
	summary              string
	description          string
	tags                 []string
	operationID          string
	deprecated           bool
	errorStatuses        []int
	security             *[]openapi.SecurityRequirement
	referencedSecSchemes []string
	strictJSON           bool
	nullableRes          bool
	maxBodyBytes         int64
	produces             []string
	hasExplicitProduces  bool
}

func validateReqResPtrTypes(reqPtrType, resPtrType reflect.Type) (reflect.Type, reflect.Type, ResponseKind, error) {
	if reqPtrType == nil || reqPtrType.Kind() != reflect.Ptr || reqPtrType.Elem().Kind() != reflect.Struct {
		return nil, nil, ResponseKindJSON, fmt.Errorf("request type must be a non-nil pointer to struct, got %v", reqPtrType)
	}
	if resPtrType == nil || resPtrType.Kind() != reflect.Ptr || resPtrType.Elem().Kind() != reflect.Struct {
		return nil, nil, ResponseKindJSON, fmt.Errorf("response type must be a non-nil pointer to struct, got %v", resPtrType)
	}

	reqType := reqPtrType.Elem()
	resType := resPtrType.Elem()

	if IsPiMetaType(reqType) {
		return nil, nil, ResponseKindJSON, fmt.Errorf("request struct cannot be pi.Meta itself")
	}
	if IsPiMetaType(resType) {
		return nil, nil, ResponseKindJSON, fmt.Errorf("response struct cannot be pi.Meta")
	}

	resKind := ResponseKindJSON
	if resType.PkgPath() == httpResponsePackagePath {
		switch resType.Name() {
		case "File":
			resKind = ResponseKindFile
		case "XML":
			resKind = ResponseKindXML
		case "Stream":
			resKind = ResponseKindStream
		case "SSE":
			resKind = ResponseKindSSE
		default:
			if _, forbidden := forbiddenResponseTypes[resType.Name()]; forbidden {
				return nil, nil, ResponseKindJSON, fmt.Errorf("response type %s.%s is a control response type and cannot be used as a typed handler response", resType.PkgPath(), resType.Name())
			}
			return nil, nil, ResponseKindJSON, fmt.Errorf("response type %s.%s cannot be used as a typed handler response", resType.PkgPath(), resType.Name())
		}
	}

	if resKind == ResponseKindJSON {
		visited := make(map[reflect.Type]bool)
		if err := ensureNoPiMetaInType(resType, resType.String(), visited); err != nil {
			return nil, nil, ResponseKindJSON, fmt.Errorf("response struct %s: %w", typeDisplayName(resType), err)
		}
	}

	return reqType, resType, resKind, nil
}

func ensureNoPiMetaInType(t reflect.Type, pathDesc string, visited map[reflect.Type]bool) error {
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		t = t.Elem()
	}
	if t.Kind() == reflect.Map {
		if err := ensureNoPiMetaInType(t.Key(), pathDesc+"[key]", visited); err != nil {
			return err
		}
		return ensureNoPiMetaInType(t.Elem(), pathDesc+"[val]", visited)
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	if IsPiMetaType(t) {
		return fmt.Errorf("pi.Meta is not allowed at %s", pathDesc)
	}
	if visited[t] {
		return nil
	}
	visited[t] = true
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if err := ensureNoPiMetaInType(f.Type, pathDesc+"."+f.Name, visited); err != nil {
			return err
		}
	}
	return nil
}

func extractAndValidateMeta(reqType reflect.Type, opts CompileOptions, resKind ResponseKind) (*parsedMeta, error) {
	var metaField reflect.StructField
	metaCount := 0
	visited := make(map[reflect.Type]bool)

	for i := 0; i < reqType.NumField(); i++ {
		f := reqType.Field(i)
		if IsPiMetaType(f.Type) {
			if !f.Anonymous {
				return nil, fmt.Errorf("request struct %s field %s must embed pi.Meta anonymously at the top level", typeDisplayName(reqType), f.Name)
			}
			metaCount++
			metaField = f
			continue
		}
		if f.Type.Kind() == reflect.Ptr && IsPiMetaType(f.Type.Elem()) {
			return nil, fmt.Errorf("request struct %s cannot embed pointer *pi.Meta", typeDisplayName(reqType))
		}
		if err := ensureNoPiMetaInType(f.Type, f.Name, visited); err != nil {
			return nil, fmt.Errorf("request struct %s must place pi.Meta directly at the top level: %w", typeDisplayName(reqType), err)
		}
	}

	if !opts.HasExplicitRoute {
		if metaCount == 0 {
			return nil, fmt.Errorf("request struct %s must embed pi.Meta", typeDisplayName(reqType))
		}
		if metaCount > 1 {
			return nil, fmt.Errorf("request struct %s must embed exactly one pi.Meta, found %d", typeDisplayName(reqType), metaCount)
		}
	} else if metaCount > 1 {
		return nil, fmt.Errorf("request struct %s must embed at most one pi.Meta, found %d", typeDisplayName(reqType), metaCount)
	}

	groupPrefix := NormalizeGroupPrefix(opts.GroupPrefix)
	if groupPrefix != "" {
		if err := validatePathSyntax(groupPrefix, true); err != nil {
			return nil, fmt.Errorf("invalid group prefix %q: %w", opts.GroupPrefix, err)
		}
	}

	var (
		method        string
		rawPath       string
		chiPattern    string
		canonicalPath string
		conflictShape string
	)

	if opts.HasExplicitRoute {
		explicitMethod := strings.ToUpper(strings.TrimSpace(opts.ExplicitMethod))
		if _, ok := supportedMethods[explicitMethod]; !ok {
			return nil, fmt.Errorf("unsupported HTTP method %q (allowed: GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS)", opts.ExplicitMethod)
		}

		normPattern := strings.TrimSpace(opts.ExplicitPattern)
		if normPattern == "" {
			if groupPrefix == "" {
				normPattern = "/"
			}
		} else if !strings.HasPrefix(normPattern, "/") {
			normPattern = "/" + normPattern
		}

		if err := validateMetaPath(normPattern, groupPrefix); err != nil {
			return nil, fmt.Errorf("invalid route path %q: %w", opts.ExplicitPattern, err)
		}

		explicitChiPattern := computeChiPattern(normPattern, groupPrefix)
		explicitCanonicalPath, explicitConflictShape := NormalizeRouteConflict(groupPrefix, explicitMethod, explicitChiPattern)
		if explicitCanonicalPath != "/" && strings.HasSuffix(explicitCanonicalPath, "/") {
			suggested := strings.TrimRight(normPattern, "/")
			if suggested == "" {
				suggested = "/"
			}
			return nil, fmt.Errorf("route path %q must not have a trailing '/'; use %q instead", explicitCanonicalPath, suggested)
		}

		method = explicitMethod
		rawPath = normPattern
		chiPattern = explicitChiPattern
		canonicalPath = explicitCanonicalPath
		conflictShape = explicitConflictShape

		if metaCount == 1 {
			if rawMetaMethod, hasMethod := metaField.Tag.Lookup("method"); hasMethod {
				if strings.TrimSpace(rawMetaMethod) == "" {
					return nil, fmt.Errorf("request struct %s pi.Meta method tag cannot be empty", typeDisplayName(reqType))
				}
				metaMethod := strings.ToUpper(strings.TrimSpace(rawMetaMethod))
				if _, ok := supportedMethods[metaMethod]; !ok {
					return nil, fmt.Errorf("request struct %s has unsupported HTTP method %q (allowed: GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS)", typeDisplayName(reqType), rawMetaMethod)
				}
				if metaMethod != explicitMethod {
					return nil, fmt.Errorf("request struct %s method mismatch: registered as %s, but pi.Meta declares method=%q", typeDisplayName(reqType), explicitMethod, rawMetaMethod)
				}
			}

			if rawMetaPath, hasPath := metaField.Tag.Lookup("path"); hasPath {
				if groupPrefix == "" && rawMetaPath == "" {
					if explicitCanonicalPath != "/" {
						return nil, fmt.Errorf("path mismatch: registered at %q, but %s pi.Meta declares path=%q", explicitCanonicalPath, typeDisplayName(reqType), rawMetaPath)
					}
				} else {
					if err := validateMetaPath(rawMetaPath, groupPrefix); err != nil {
						return nil, fmt.Errorf("request struct %s has invalid path %q: %w", typeDisplayName(reqType), rawMetaPath, err)
					}
					metaChiPattern := computeChiPattern(rawMetaPath, groupPrefix)
					metaCanonical, _ := NormalizeRouteConflict(groupPrefix, method, metaChiPattern)
					if metaCanonical != explicitCanonicalPath {
						return nil, fmt.Errorf("path mismatch: registered at %q, but %s pi.Meta declares path=%q", explicitCanonicalPath, typeDisplayName(reqType), rawMetaPath)
					}
				}
			}
		}
	} else {
		rawMethod, hasMethod := metaField.Tag.Lookup("method")
		if !hasMethod || strings.TrimSpace(rawMethod) == "" {
			return nil, fmt.Errorf("request struct %s pi.Meta requires method tag", typeDisplayName(reqType))
		}
		method = strings.ToUpper(strings.TrimSpace(rawMethod))
		if _, ok := supportedMethods[method]; !ok {
			return nil, fmt.Errorf("request struct %s has unsupported HTTP method %q (allowed: GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS)", typeDisplayName(reqType), rawMethod)
		}

		var hasPath bool
		rawPath, hasPath = metaField.Tag.Lookup("path")
		if !hasPath {
			return nil, fmt.Errorf("request struct %s pi.Meta requires path tag", typeDisplayName(reqType))
		}
		if err := validateMetaPath(rawPath, groupPrefix); err != nil {
			return nil, fmt.Errorf("request struct %s has invalid path %q: %w", typeDisplayName(reqType), rawPath, err)
		}

		chiPattern = computeChiPattern(rawPath, groupPrefix)
		canonicalPath, conflictShape = NormalizeRouteConflict(groupPrefix, method, chiPattern)
	}

	pathParams, err := extractPathPlaceholders(canonicalPath)
	if err != nil {
		return nil, fmt.Errorf("request struct %s path %q: %w", typeDisplayName(reqType), canonicalPath, err)
	}

	var metaTag reflect.StructTag
	if metaCount == 1 {
		metaTag = metaField.Tag
	}

	status := 200
	if rawStatus, ok := metaTag.Lookup("status"); ok {
		trimmed := strings.TrimSpace(rawStatus)
		if trimmed == "" {
			return nil, fmt.Errorf("request struct %s pi.Meta status tag cannot be empty", typeDisplayName(reqType))
		}
		parsed, err := strconv.Atoi(trimmed)
		if err != nil {
			return nil, fmt.Errorf("request struct %s pi.Meta has invalid status %q", typeDisplayName(reqType), rawStatus)
		}
		if _, valid := supportedStatuses[parsed]; !valid {
			return nil, fmt.Errorf("request struct %s pi.Meta status %d is unsupported (allowed: 200, 201, 202, 204)", typeDisplayName(reqType), parsed)
		}
		status = parsed
	}

	summary := strings.TrimSpace(metaTag.Get("summary"))

	descVal, hasDesc := metaTag.Lookup("description")
	dcVal, hasDC := metaTag.Lookup("dc")
	if hasDesc && hasDC && strings.TrimSpace(descVal) != strings.TrimSpace(dcVal) {
		return nil, fmt.Errorf("request struct %s pi.Meta has conflicting description and dc tags", typeDisplayName(reqType))
	}
	description := strings.TrimSpace(descVal)
	if !hasDesc && hasDC {
		description = strings.TrimSpace(dcVal)
	}

	tags, err := resolveMetaTags(metaTag, opts, reqType)
	if err != nil {
		return nil, err
	}

	operationID, err := resolveOperationID(metaTag, method, canonicalPath, reqType)
	if err != nil {
		return nil, err
	}

	deprecated := false
	if rawDep, ok := metaTag.Lookup("deprecated"); ok {
		b, err := parseStrictMetaBool(rawDep)
		if err != nil {
			return nil, fmt.Errorf("request struct %s pi.Meta deprecated: %w", typeDisplayName(reqType), err)
		}
		deprecated = b
	}

	errorStatuses, err := parseErrorsTag(metaTag, reqType)
	if err != nil {
		return nil, err
	}

	security, secSchemes, err := resolveMetaSecurity(metaTag, opts, reqType)
	if err != nil {
		return nil, err
	}

	strictJSON := opts.DefaultStrictJSON
	if rawStrict, ok := metaTag.Lookup("strict"); ok {
		b, err := parseStrictMetaBool(rawStrict)
		if err != nil {
			return nil, fmt.Errorf("request struct %s pi.Meta strict: %w", typeDisplayName(reqType), err)
		}
		strictJSON = b
	} else if opts.StrictJSONOverride != nil {
		strictJSON = *opts.StrictJSONOverride
	}

	nullableRes := false
	if rawNullable, ok := metaTag.Lookup("nullable"); ok {
		b, err := parseStrictMetaBool(rawNullable)
		if err != nil {
			return nil, fmt.Errorf("request struct %s pi.Meta nullable: %w", typeDisplayName(reqType), err)
		}
		nullableRes = b
	}

	var maxBodyBytes int64
	if rawMaxBody, ok := metaTag.Lookup("maxBody"); ok {
		parsedMaxBody, err := parseMaxBodyTag(rawMaxBody)
		if err != nil {
			return nil, fmt.Errorf("request struct %s pi.Meta maxBody: %w", typeDisplayName(reqType), err)
		}
		maxBodyBytes = parsedMaxBody
	}

	rawProduces, hasProduces := metaTag.Lookup("produces")
	var produces []string
	hasExplicitProduces := false
	switch resKind {
	case ResponseKindJSON:
		if hasProduces {
			return nil, fmt.Errorf("request struct %s pi.Meta produces tag is only supported for *response.File, *response.XML, *response.Stream, or *response.SSE responses", typeDisplayName(reqType))
		}
	case ResponseKindFile, ResponseKindXML, ResponseKindStream, ResponseKindSSE:
		if status == http.StatusNoContent || status == http.StatusResetContent || status == http.StatusNotModified {
			return nil, fmt.Errorf("request struct %s status %d is not supported for *response.File, *response.XML, *response.Stream, or *response.SSE responses", typeDisplayName(reqType), status)
		}
		if nullableRes {
			return nil, fmt.Errorf("request struct %s nullable:\"true\" is not supported for *response.File, *response.XML, *response.Stream, or *response.SSE responses", typeDisplayName(reqType))
		}
		if hasProduces {
			produces, err = parseProducesTag(rawProduces, reqType)
			if err != nil {
				return nil, err
			}
			if resKind == ResponseKindSSE {
				for _, rng := range produces {
					if !mediaRangeMatches("text/event-stream", map[string]string{"charset": "utf-8"}, rng) {
						return nil, fmt.Errorf("request struct %s pi.Meta produces %q is incompatible with *response.SSE (expected text/event-stream)", typeDisplayName(reqType), rng)
					}
				}
			}
			hasExplicitProduces = true
		} else {
			switch resKind {
			case ResponseKindXML:
				produces = []string{"application/xml"}
			case ResponseKindSSE:
				produces = []string{"text/event-stream"}
			default:
				produces = []string{"application/octet-stream"}
			}
		}
	}

	return &parsedMeta{
		method:               method,
		metaPath:             rawPath,
		chiPattern:           chiPattern,
		canonicalPath:        canonicalPath,
		conflictShape:        conflictShape,
		pathParams:           pathParams,
		status:               status,
		summary:              summary,
		description:          description,
		tags:                 tags,
		operationID:          operationID,
		deprecated:           deprecated,
		errorStatuses:        errorStatuses,
		security:             security,
		referencedSecSchemes: secSchemes,
		strictJSON:           strictJSON,
		nullableRes:          nullableRes,
		maxBodyBytes:         maxBodyBytes,
		produces:             produces,
		hasExplicitProduces:  hasExplicitProduces,
	}, nil
}

func validateMetaPath(rawPath, groupPrefix string) error {
	if rawPath == "" {
		if groupPrefix == "" {
			return fmt.Errorf("root group requires path starting with '/'")
		}
		return nil
	}
	if !strings.HasPrefix(rawPath, "/") {
		return fmt.Errorf("non-empty path must start with '/'")
	}
	return validatePathSyntax(rawPath, false)
}

func validatePathSyntax(p string, isGroupPrefix bool) error {
	for _, r := range p {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("path cannot contain whitespace or control characters")
		}
	}
	if strings.ContainsAny(p, "?#") {
		return fmt.Errorf("path cannot contain query '?' or fragment '#' characters")
	}
	if strings.Contains(p, "*") {
		return fmt.Errorf("wildcard '*' is not allowed in typed path")
	}
	if !isGroupPrefix && strings.Contains(p, "//") {
		return fmt.Errorf("path cannot contain empty '//' segments")
	}

	segments := strings.Split(p, "/")
	for _, seg := range segments {
		if seg == "." || seg == ".." {
			return fmt.Errorf("relative segment %q is not allowed in path", seg)
		}
		if err := validateSegmentBraces(seg); err != nil {
			return err
		}
	}
	return nil
}

func validateSegmentBraces(seg string) error {
	depth := 0
	start := -1
	for i := 0; i < len(seg); i++ {
		switch seg[i] {
		case '{':
			if depth > 0 {
				return fmt.Errorf("nested braces in segment %q", seg)
			}
			depth++
			start = i
		case '}':
			if depth == 0 {
				return fmt.Errorf("unmatched '}' in segment %q", seg)
			}
			depth--
			inner := seg[start+1 : i]
			if inner == "" {
				return fmt.Errorf("empty path parameter '{}' in segment %q", seg)
			}
			if strings.Contains(inner, ":") {
				return fmt.Errorf("regex path parameter %q is not supported in typed routes", seg[start:i+1])
			}
		}
	}
	if depth != 0 {
		return fmt.Errorf("unclosed '{' in segment %q", seg)
	}
	return nil
}

func computeChiPattern(rawPath, groupPrefix string) string {
	if groupPrefix != "" && (rawPath == "" || rawPath == "/") {
		return "/"
	}
	return rawPath
}

func extractPathPlaceholders(canonicalPath string) ([]string, error) {
	var params []string
	seen := make(map[string]struct{})
	for i := 0; i < len(canonicalPath); i++ {
		if canonicalPath[i] != '{' {
			continue
		}
		end := strings.IndexByte(canonicalPath[i+1:], '}')
		if end < 0 {
			return nil, fmt.Errorf("unclosed '{' in path %q", canonicalPath)
		}
		name := canonicalPath[i+1 : i+1+end]
		if name == "" {
			return nil, fmt.Errorf("empty path placeholder '{}' in %q", canonicalPath)
		}
		if strings.Contains(name, ":") {
			return nil, fmt.Errorf("regex path placeholder %q is not supported", "{"+name+"}")
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("duplicate path placeholder %q in %q", name, canonicalPath)
		}
		seen[name] = struct{}{}
		params = append(params, name)
		i += end + 1
	}
	return params, nil
}

func parseStrictMetaBool(raw string) (bool, error) {
	switch raw {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("must be \"true\" or \"false\", got %q", raw)
	}
}

func resolveMetaTags(tag reflect.StructTag, opts CompileOptions, reqType reflect.Type) ([]string, error) {
	if raw, ok := tag.Lookup("tags"); ok {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			return nil, nil
		}
		parts := strings.Split(raw, ",")
		out := make([]string, 0, len(parts))
		seen := make(map[string]struct{}, len(parts))
		for _, p := range parts {
			item := strings.TrimSpace(p)
			if item == "" {
				return nil, fmt.Errorf("request struct %s pi.Meta tags contains empty item in %q", typeDisplayName(reqType), raw)
			}
			if _, dup := seen[item]; dup {
				continue
			}
			seen[item] = struct{}{}
			out = append(out, item)
		}
		return out, nil
	}
	if opts.HasEndpointTags {
		return dedupeStrings(opts.EndpointTags), nil
	}
	return dedupeStrings(opts.InheritedTags), nil
}

func dedupeStrings(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, raw := range items {
		item := strings.TrimSpace(raw)
		if item == "" {
			continue
		}
		if _, dup := seen[item]; dup {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func resolveOperationID(tag reflect.StructTag, method, canonicalPath string, reqType reflect.Type) (string, error) {
	if raw, ok := tag.Lookup("operationId"); ok {
		id := strings.TrimSpace(raw)
		if id == "" {
			return "", fmt.Errorf("request struct %s pi.Meta operationId cannot be empty", typeDisplayName(reqType))
		}
		for _, r := range id {
			if unicode.IsSpace(r) || unicode.IsControl(r) {
				return "", fmt.Errorf("request struct %s pi.Meta operationId %q cannot contain whitespace or control characters", typeDisplayName(reqType), id)
			}
		}
		return id, nil
	}
	return buildDefaultOperationID(method, canonicalPath), nil
}

func buildDefaultOperationID(method, canonicalPath string) string {
	var b strings.Builder
	b.WriteString(strings.ToLower(method))

	segments := strings.Split(canonicalPath, "/")
	addedSegment := false
	for _, seg := range segments {
		if seg == "" {
			continue
		}
		addedSegment = true
		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") && len(seg) > 2 {
			paramName := seg[1 : len(seg)-1]
			b.WriteString("By")
			writeCamelWords(&b, paramName)
			continue
		}
		writeCamelWords(&b, seg)
	}
	if !addedSegment {
		b.WriteString("Root")
	}
	return b.String()
}

func writeCamelWords(b *strings.Builder, s string) {
	start := -1
	for i := 0; i <= len(s); i++ {
		isWord := i < len(s) && isASCIIAlphaNum(s[i])
		if isWord {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			word := s[start:i]
			first := word[0]
			if first >= 'a' && first <= 'z' {
				b.WriteByte(first - ('a' - 'A'))
				b.WriteString(word[1:])
			} else {
				b.WriteString(word)
			}
			start = -1
		}
	}
}

func isASCIIAlphaNum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func parseErrorsTag(tag reflect.StructTag, reqType reflect.Type) ([]int, error) {
	raw, ok := tag.Lookup("errors")
	if !ok {
		return nil, nil
	}
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("request struct %s pi.Meta errors tag cannot be empty", typeDisplayName(reqType))
	}
	parts := strings.Split(raw, ",")
	seen := make(map[int]struct{}, len(parts))
	codes := make([]int, 0, len(parts))
	for _, p := range parts {
		item := strings.TrimSpace(p)
		if item == "" {
			return nil, fmt.Errorf("request struct %s pi.Meta errors contains empty status code in %q", typeDisplayName(reqType), raw)
		}
		code, err := strconv.Atoi(item)
		if err != nil || code < 400 || code > 599 {
			return nil, fmt.Errorf("request struct %s pi.Meta errors status %q must be between 400 and 599", typeDisplayName(reqType), item)
		}
		if _, dup := seen[code]; dup {
			continue
		}
		seen[code] = struct{}{}
		codes = append(codes, code)
	}
	sort.Ints(codes)
	return codes, nil
}

func resolveMetaSecurity(tag reflect.StructTag, opts CompileOptions, reqType reflect.Type) (*[]openapi.SecurityRequirement, []string, error) {
	if raw, ok := tag.Lookup("security"); ok {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			return nil, nil, fmt.Errorf("request struct %s pi.Meta security tag cannot be empty (use \"-\" to clear security)", typeDisplayName(reqType))
		}
		if trimmed == "-" {
			empty := []openapi.SecurityRequirement{}
			return &empty, nil, nil
		}
		parts := strings.Split(trimmed, ",")
		names := make([]string, 0, len(parts))
		seen := make(map[string]struct{}, len(parts))
		for _, p := range parts {
			name := strings.TrimSpace(p)
			if name == "" || name == "-" {
				return nil, nil, fmt.Errorf("request struct %s pi.Meta security contains invalid scheme entry in %q", typeDisplayName(reqType), raw)
			}
			if _, dup := seen[name]; dup {
				continue
			}
			seen[name] = struct{}{}
			names = append(names, name)
		}
		return buildSecurityRequirement(names)
	}

	if opts.HasEndpointSec {
		return buildSecurityRequirement(opts.EndpointSecurity)
	}
	if opts.HasInheritedSec {
		return buildSecurityRequirement(opts.InheritedSecurity)
	}
	return nil, nil, nil
}

func buildSecurityRequirement(schemes []string) (*[]openapi.SecurityRequirement, []string, error) {
	if len(schemes) == 0 {
		empty := []openapi.SecurityRequirement{}
		return &empty, nil, nil
	}
	reqMap := make(openapi.SecurityRequirement, len(schemes))
	names := make([]string, 0, len(schemes))
	seen := make(map[string]struct{}, len(schemes))
	for _, raw := range schemes {
		name := strings.TrimSpace(raw)
		if name == "" {
			return nil, nil, fmt.Errorf("security scheme name cannot be empty")
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		reqMap[name] = []string{}
		names = append(names, name)
	}
	out := []openapi.SecurityRequirement{reqMap}
	return &out, names, nil
}

func typeDisplayName(t reflect.Type) string {
	if t == nil {
		return "<nil>"
	}
	if t.Name() != "" {
		return t.Name()
	}
	return t.String()
}

func parseMaxBodyTag(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, fmt.Errorf("maxBody tag cannot be empty")
	}

	lower := strings.ToLower(s)
	unit := int64(1)
	suffixLen := 0
	switch {
	case strings.HasSuffix(lower, "kib"):
		unit = 1 << 10
		suffixLen = 3
	case strings.HasSuffix(lower, "mib"):
		unit = 1 << 20
		suffixLen = 3
	case strings.HasSuffix(lower, "gib"):
		unit = 1 << 30
		suffixLen = 3
	case strings.HasSuffix(lower, "kb"):
		unit = 1 << 10
		suffixLen = 2
	case strings.HasSuffix(lower, "mb"):
		unit = 1 << 20
		suffixLen = 2
	case strings.HasSuffix(lower, "gb"):
		unit = 1 << 30
		suffixLen = 2
	case strings.HasSuffix(lower, "b"):
		unit = 1
		suffixLen = 1
	}

	numStr := strings.TrimSpace(s[:len(s)-suffixLen])
	if numStr == "" {
		return 0, fmt.Errorf("invalid size %q", raw)
	}
	for i := 0; i < len(numStr); i++ {
		if numStr[i] < '0' || numStr[i] > '9' {
			return 0, fmt.Errorf("invalid size %q", raw)
		}
	}
	n, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("size %q must be a positive integer", raw)
	}
	if n > maxAllowedBodyBytes/unit {
		return 0, fmt.Errorf("size %q exceeds maximum allowed %d bytes (1 GiB)", raw, maxAllowedBodyBytes)
	}
	return n * unit, nil
}

func parseProducesTag(raw string, reqType reflect.Type) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("request struct %s pi.Meta produces tag cannot be empty", typeDisplayName(reqType))
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, p := range parts {
		item := strings.TrimSpace(p)
		if item == "" {
			return nil, fmt.Errorf("request struct %s pi.Meta produces contains empty media type in %q", typeDisplayName(reqType), raw)
		}
		norm, _, _, err := parseAndValidateMediaRange(item, true)
		if err != nil {
			return nil, fmt.Errorf("request struct %s pi.Meta produces has invalid media type %q: %w", typeDisplayName(reqType), item, err)
		}
		key := strings.ToLower(norm)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, norm)
	}
	return out, nil
}

func parseAndValidateMediaRange(raw string, allowWildcard bool) (string, string, map[string]string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", "", nil, fmt.Errorf("media type cannot be empty")
	}
	mt, params, err := mime.ParseMediaType(trimmed)
	if err != nil {
		return "", "", nil, err
	}
	if strings.Count(mt, "/") != 1 {
		return "", "", nil, fmt.Errorf("media type %q must be in type/subtype form", raw)
	}
	slash := strings.IndexByte(mt, '/')
	if slash <= 0 || slash == len(mt)-1 {
		return "", "", nil, fmt.Errorf("media type %q must have non-empty type and subtype", raw)
	}
	mainType := mt[:slash]
	subType := mt[slash+1:]
	if strings.Contains(mainType, "*") || strings.Contains(subType, "*") {
		if !allowWildcard {
			return "", "", nil, fmt.Errorf("wildcard media type %q is not allowed", raw)
		}
		if mainType == "*" {
			if subType != "*" {
				return "", "", nil, fmt.Errorf("invalid wildcard media range %q", raw)
			}
		} else if subType != "*" {
			return "", "", nil, fmt.Errorf("invalid wildcard media range %q", raw)
		}
		if len(params) > 0 {
			return "", "", nil, fmt.Errorf("wildcard media range %q cannot carry parameters", raw)
		}
		return mt, mt, nil, nil
	}
	if len(params) == 0 {
		return mt, mt, nil, nil
	}
	formatted := mime.FormatMediaType(mt, params)
	if formatted == "" {
		return "", "", nil, fmt.Errorf("invalid media type %q", raw)
	}
	return formatted, mt, params, nil
}

func mediaRangeMatches(actualMT string, actualParams map[string]string, rangeRaw string) bool {
	_, rangeMT, rangeParams, err := parseAndValidateMediaRange(rangeRaw, true)
	if err != nil {
		return false
	}
	if rangeMT == "*/*" {
		return true
	}
	rSlash := strings.IndexByte(rangeMT, '/')
	aSlash := strings.IndexByte(actualMT, '/')
	if rSlash <= 0 || aSlash <= 0 {
		return false
	}
	rangeMain, rangeSub := rangeMT[:rSlash], rangeMT[rSlash+1:]
	actualMain := actualMT[:aSlash]
	if rangeSub == "*" {
		return actualMain == rangeMain
	}
	if actualMT != rangeMT {
		return false
	}
	for k, v := range rangeParams {
		av, ok := actualParams[k]
		if !ok || !strings.EqualFold(av, v) {
			return false
		}
	}
	return true
}
