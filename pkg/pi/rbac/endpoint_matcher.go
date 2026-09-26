package rbac

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"

	"github.com/go-chi/chi/v5"
)

const (
	// DefaultConfigPath is the default config path value (empty string).
	// When passed to ResolveRBACConfigPath, it will try default paths: configs/rbac.json, configs/rbac.yaml, configs/rbac.yml.
	DefaultConfigPath = ""

	// Default RBAC config paths (tried in order).
	defaultRBACJSONPath = "configs/rbac.json"
	defaultRBACYAMLPath = "configs/rbac.yaml"
	defaultRBACYMLPath  = "configs/rbac.yml"
)

var (
	// errUnbalancedBraces is returned when a mux pattern has unbalanced braces.
	errUnbalancedBraces = errors.New("unbalanced braces in pattern")
)

// matchEndpoint checks if the request matches an endpoint configuration.
// This is the primary authorization check using the unified Endpoints configuration.
// Returns the matched endpoint and whether it's public.
func matchEndpoint(method, route string, endpoints []EndpointMapping, config *Config) (*EndpointMapping, bool) {
	for i := range endpoints {
		endpoint := &endpoints[i]

		// Check if endpoint is public
		if endpoint.Public {
			if matchesEndpointPattern(endpoint, route, config) {
				return endpoint, true
			}

			continue
		}

		// Check method match
		if !matchesHTTPMethod(method, endpoint.Methods) {
			continue
		}

		// Check route match
		if matchesEndpointPattern(endpoint, route, config) {
			return endpoint, false
		}
	}

	return nil, false
}

// matchesHTTPMethod checks if the HTTP method matches the endpoint's allowed methods.
func matchesHTTPMethod(method string, allowedMethods []string) bool {
	// Empty methods or "*" means all methods
	if len(allowedMethods) == 0 {
		return true
	}

	for _, m := range allowedMethods {
		if m == "*" || strings.EqualFold(m, method) {
			return true
		}
	}

	return false
}

// isMuxPattern detects if a pattern contains mux-style variables.
// Returns true if pattern contains { and }.
func isMuxPattern(pattern string) bool {
	return strings.Contains(pattern, "{") && strings.Contains(pattern, "}")
}

// matchMuxPattern uses a chi router to test if a path matches a URL parameter pattern.
// Creates a temporary chi router and checks if the route matches.
// Handles pattern types: {id}, {id:[0-9]+}, etc.
// Converts gorilla/mux multi-level patterns {path:.*} to chi wildcard syntax /*
func matchMuxPattern(pattern, method, path string) bool {
	matched := false

	// Convert gorilla/mux multi-level pattern {path:.*} to chi wildcard /*
	// This handles patterns like /api/{path:.*} -> /api/*
	chiPattern := convertMuxPatternToChi(pattern)

	r := chi.NewRouter()
	r.HandleFunc(chiPattern, func(w http.ResponseWriter, r *http.Request) {
		matched = true
	})

	req := httptest.NewRequest(http.MethodGet, path, http.NoBody)
	if method != "" {
		req.Method = method
	}

	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	return matched
}

// convertMuxPatternToChi converts gorilla/mux patterns to chi patterns.
// Main conversion: {varname:.*} -> /* (multi-level wildcard)
// Other patterns like {id} and {id:[0-9]+} are compatible between mux and chi.
func convertMuxPatternToChi(pattern string) string {
	// Replace {varname:.*} with /* for multi-level wildcards
	// This regex matches {anything:.*} and replaces with /*
	if strings.Contains(pattern, ":.*}") {
		// Find the last occurrence of {varname:.*}
		start := strings.LastIndex(pattern, "{")
		if start != -1 {
			end := strings.Index(pattern[start:], "}")
			if end != -1 && strings.Contains(pattern[start:start+end], ":.*") {
				// Replace from the / before { to the end with /*
				beforeBrace := strings.LastIndex(pattern[:start], "/")
				if beforeBrace != -1 {
					return pattern[:beforeBrace] + "/*"
				}
			}
		}
	}
	return pattern
}

// validateMuxPattern validates mux pattern syntax.
// Ensures balanced braces and validates regex constraints format.
func validateMuxPattern(pattern string) error {
	// Check for balanced braces
	openCount := strings.Count(pattern, "{")

	closeCount := strings.Count(pattern, "}")

	if openCount != closeCount {
		return fmt.Errorf("%w: %s", errUnbalancedBraces, pattern)
	}

	// Check that if there are closing braces, there must be opening braces
	// A pattern like "/api/id}" should not be valid
	if closeCount > 0 && openCount == 0 {
		return fmt.Errorf("%w: %s", errUnbalancedBraces, pattern)
	}

	// Basic validation: check that braces are properly formatted
	// More detailed validation would require parsing, which mux will do anyway
	return nil
}

// matchesEndpointPattern checks if the route matches the endpoint pattern.
// Method matching is handled separately in matchEndpoint before this function is called.
// Uses chi router matching for URL parameter patterns, exact match for non-pattern paths.
func matchesEndpointPattern(endpoint *EndpointMapping, route string, config *Config) bool {
	if endpoint.Path == "" {
		return false
	}

	pattern := endpoint.Path

	// Exact match for non-pattern paths
	if !isMuxPattern(pattern) {
		return pattern == route
	}

	// Use chi router matching for patterns
	// Method is handled separately, so pass empty string here
	return matchMuxPattern(pattern, "", route)
}

// checkEndpointAuthorization checks if the user's role is authorized for the endpoint.
// Pure permission-based: checks if role has ANY of the required permissions (OR logic).
// Uses the endpoint parameter directly instead of re-looking it up.
func checkEndpointAuthorization(role string, endpoint *EndpointMapping, config *Config) (allowed bool, reason string) {
	// Public endpoints are always allowed
	if endpoint.Public {
		return true, "public-endpoint"
	}

	// Get required permissions
	requiredPerms := endpoint.RequiredPermissions

	// If no permission requirement found, deny (fail secure)
	if len(requiredPerms) == 0 {
		return false, ""
	}

	// Get role's permissions (thread-safe)
	rolePerms := config.GetRolePermissions(role)
	if len(rolePerms) == 0 {
		return false, ""
	}

	// Check if role has ANY of the required permissions (OR logic)
	// Only exact matches are supported - wildcards are NOT supported in permissions
	for _, requiredPerm := range requiredPerms {
		for _, perm := range rolePerms {
			// Exact match only - no wildcard support
			if perm == requiredPerm {
				return true, "permission-based"
			}
		}
	}

	return false, ""
}

// getEndpointForRequest finds the matching endpoint configuration for a request.
// This is the primary function used by the middleware to determine authorization requirements.
// Uses optimized maps for O(1) exact matches, falls back to pattern matching for mux patterns.
func getEndpointForRequest(r *http.Request, config *Config) (*EndpointMapping, bool) {
	if len(config.Endpoints) == 0 {
		return nil, false
	}

	method := strings.ToUpper(r.Method)
	path := r.URL.Path
	key := fmt.Sprintf("%s:%s", method, path)

	// Try exact match first (O(1) lookup)
	if endpoint, isPublic := config.getExactEndpoint(key); endpoint != nil {
		return endpoint, isPublic
	}

	// Try pattern matching (O(n) but only for patterns, not exact matches)
	return config.findEndpointByPattern(method, path)
}

// ResolveRBACConfigPath resolves the RBAC config file path.
// If configFile is empty, tries default paths in order: configs/rbac.json, configs/rbac.yaml, configs/rbac.yml.
func ResolveRBACConfigPath(configFile string) string {
	// If custom path provided, use it
	if configFile != "" {
		return configFile
	}

	// Try default paths in order
	defaultPaths := []string{
		defaultRBACJSONPath,
		defaultRBACYAMLPath,
		defaultRBACYMLPath,
	}

	for _, path := range defaultPaths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return ""
}
