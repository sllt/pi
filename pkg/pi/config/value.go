package config

import (
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Value represents a configuration value with pure, zero-allocation typed accessors.
// It performs no logging and has no side effects.
type Value string

// ValueOf wraps a raw string as a Value.
func ValueOf(raw string) Value {
	return Value(raw)
}

// Env reads the named environment variable via os.Getenv as a Value.
func Env(key string) Value {
	return Value(os.Getenv(key))
}

// Lookup reads key from cfg in a nil-safe manner and returns it as a Value.
func Lookup(cfg Config, key string) Value {
	if isNilConfig(cfg) {
		return ""
	}
	return Value(cfg.Get(key))
}

func isNilConfig(c Config) bool {
	if c == nil {
		return true
	}
	rv := reflect.ValueOf(c)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return rv.IsNil()
	default:
		return false
	}
}

func firstDefault[T any](def []T) T {
	var zero T
	if len(def) > 0 {
		return def[0]
	}
	return zero
}

// Raw returns the underlying unmodified string.
func (v Value) Raw() string {
	return string(v)
}

// IsSet reports whether the value contains any non-whitespace characters.
func (v Value) IsSet() bool {
	return strings.TrimSpace(string(v)) != ""
}

// String returns the raw string if non-empty (after whitespace check), or def[0] when unset.
func (v Value) String(def ...string) string {
	if !v.IsSet() {
		return firstDefault(def)
	}
	return string(v)
}

// Int parses the trimmed value as an int, returning def[0] (or 0) when unset or invalid.
func (v Value) Int(def ...int) int {
	n, err := v.IntE(def...)
	if err != nil {
		return firstDefault(def)
	}
	return n
}

// IntE parses the trimmed value as an int, returning def[0] (or 0) with nil error when unset.
func (v Value) IntE(def ...int) (int, error) {
	s := strings.TrimSpace(string(v))
	if s == "" {
		return firstDefault(def), nil
	}
	return strconv.Atoi(s)
}

// Int64 parses the trimmed value as an int64, returning def[0] (or 0) when unset or invalid.
func (v Value) Int64(def ...int64) int64 {
	n, err := v.Int64E(def...)
	if err != nil {
		return firstDefault(def)
	}
	return n
}

// Int64E parses the trimmed value as an int64, returning def[0] (or 0) with nil error when unset.
func (v Value) Int64E(def ...int64) (int64, error) {
	s := strings.TrimSpace(string(v))
	if s == "" {
		return firstDefault(def), nil
	}
	return strconv.ParseInt(s, 10, 64)
}

// Float64 parses the trimmed value as a float64, returning def[0] (or 0) when unset or invalid.
func (v Value) Float64(def ...float64) float64 {
	f, err := v.Float64E(def...)
	if err != nil {
		return firstDefault(def)
	}
	return f
}

// Float64E parses the trimmed value as a float64, returning def[0] (or 0) with nil error when unset.
func (v Value) Float64E(def ...float64) (float64, error) {
	s := strings.TrimSpace(string(v))
	if s == "" {
		return firstDefault(def), nil
	}
	return strconv.ParseFloat(s, 64)
}

// Bool parses the trimmed value as a bool, returning def[0] (or false) when unset or invalid.
func (v Value) Bool(def ...bool) bool {
	b, err := v.BoolE(def...)
	if err != nil {
		return firstDefault(def)
	}
	return b
}

// BoolE parses the trimmed value as a bool, returning def[0] (or false) with nil error when unset.
func (v Value) BoolE(def ...bool) (bool, error) {
	s := strings.TrimSpace(string(v))
	if s == "" {
		return firstDefault(def), nil
	}
	return strconv.ParseBool(s)
}

// Duration parses the trimmed value with time.ParseDuration, returning def[0] (or 0) when unset or invalid.
func (v Value) Duration(def ...time.Duration) time.Duration {
	d, err := v.DurationE(def...)
	if err != nil {
		return firstDefault(def)
	}
	return d
}

// DurationE parses the trimmed value with time.ParseDuration, returning def[0] (or 0) with nil error when unset.
func (v Value) DurationE(def ...time.Duration) (time.Duration, error) {
	s := strings.TrimSpace(string(v))
	if s == "" {
		return firstDefault(def), nil
	}
	return time.ParseDuration(s)
}

// Strings splits a comma-separated value, trims whitespace, and drops empty elements.
// When the resulting slice is empty, it returns a copy of def[0] (or nil if not provided).
func (v Value) Strings(def ...[]string) []string {
	s := strings.TrimSpace(string(v))
	if s == "" {
		if len(def) > 0 {
			return slices.Clone(def[0])
		}
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		if len(def) > 0 {
			return slices.Clone(def[0])
		}
		return nil
	}
	return out
}
