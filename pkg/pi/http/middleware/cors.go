package middleware

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const allowedHeaders = "Authorization, Content-Type, x-requested-with, origin, true-client-ip, X-Correlation-ID"

// ValidateCORS rejects ambiguous credential/origin combinations before startup.
func ValidateCORS(cfg map[string]string) error {
	credentials := cfg["Access-Control-Allow-Credentials"]
	if credentials != "" && credentials != "true" && credentials != "false" {
		return fmt.Errorf("CORS credentials must be true or false")
	}
	for _, origin := range csv(cfg["Access-Control-Allow-Origin"]) {
		if origin == "*" {
			if credentials == "true" {
				return fmt.Errorf("CORS wildcard origin cannot allow credentials")
			}
			continue
		}
		u, err := url.Parse(origin)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("invalid CORS origin %q", origin)
		}
	}
	if age := cfg["Access-Control-Max-Age"]; age != "" {
		seconds, err := strconv.Atoi(age)
		if err != nil || seconds < 0 {
			return fmt.Errorf("invalid CORS max age")
		}
	}
	return nil
}

// CORS is the single framework CORS policy. No configured origins means no
// cross-origin access. Non-CORS OPTIONS requests continue to the application.
func CORS(cfg map[string]string, routes *[]string) func(http.Handler) http.Handler {
	origins := csv(cfg["Access-Control-Allow-Origin"])
	invalid := ValidateCORS(cfg)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add("Vary", "Origin")
			origin := r.Header.Get("Origin")
			if origin == "" {
				next.ServeHTTP(w, r)
				return
			}
			preflight := r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != ""
			if preflight {
				w.Header().Add("Vary", "Access-Control-Request-Method")
				w.Header().Add("Vary", "Access-Control-Request-Headers")
			}
			if invalid != nil || (!contains(origins, origin) && !contains(origins, "*")) {
				http.Error(w, "CORS origin not allowed", http.StatusForbidden)
				return
			}
			methods := cfg["Access-Control-Allow-Methods"]
			if methods == "" && routes != nil {
				methods = strings.Join(*routes, ",")
			}
			if methods == "" {
				methods = "GET,HEAD,POST,PUT,PATCH,DELETE,OPTIONS"
			}
			headers := allowedHeaders + "," + cfg["Access-Control-Allow-Headers"]
			if preflight {
				if !contains(csv(methods), r.Header.Get("Access-Control-Request-Method")) {
					http.Error(w, "CORS method not allowed", http.StatusForbidden)
					return
				}
				for _, header := range csv(r.Header.Get("Access-Control-Request-Headers")) {
					if !containsFold(csv(headers), header) {
						http.Error(w, "CORS header not allowed", http.StatusForbidden)
						return
					}
				}
			}
			allowedOrigin := origin
			if contains(origins, "*") {
				allowedOrigin = "*"
			}
			w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
			if cfg["Access-Control-Allow-Credentials"] == "true" {
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			}
			if expose := cfg["Access-Control-Expose-Headers"]; expose != "" {
				w.Header().Set("Access-Control-Expose-Headers", expose)
			}
			if preflight {
				w.Header().Set("Access-Control-Allow-Methods", strings.Join(csv(methods), ", "))
				w.Header().Set("Access-Control-Allow-Headers", strings.Join(csv(headers), ", "))
				if age := cfg["Access-Control-Max-Age"]; age != "" {
					w.Header().Set("Access-Control-Max-Age", age)
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func csv(value string) []string {
	var values []string
	for _, v := range strings.Split(value, ",") {
		if v = strings.TrimSpace(v); v != "" {
			values = append(values, v)
		}
	}
	return values
}
func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
func containsFold(values []string, value string) bool {
	for _, v := range values {
		if strings.EqualFold(v, value) {
			return true
		}
	}
	return false
}
