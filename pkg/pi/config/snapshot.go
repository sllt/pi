package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/joho/godotenv"
)

// Snapshot is immutable. Values and Redacted return independent copies.
type Snapshot struct{ values map[string]string }

func NewSnapshot(values map[string]string) *Snapshot {
	s := &Snapshot{values: make(map[string]string, len(values))}
	for k, v := range values {
		s.values[k] = v
	}
	return s
}
func (s *Snapshot) Get(k string) string {
	if s == nil {
		return ""
	}
	return s.values[k]
}
func (s *Snapshot) GetOrDefault(k, d string) string {
	if v := s.Get(k); v != "" {
		return v
	}
	return d
}
func (s *Snapshot) Values() map[string]string {
	m := map[string]string{}
	if s != nil {
		for k, v := range s.values {
			m[k] = v
		}
	}
	return m
}
func RedactMap(values map[string]string) map[string]string {
	m := make(map[string]string, len(values))
	for k, v := range values {
		u := strings.ToUpper(k)
		redacted := false
		isPublicHeaderList := u == "CORS_ALLOWED_HEADERS" || u == "CORS_EXPOSE_HEADERS"
		for _, word := range []string{"SECRET", "PASSWORD", "PASS", "PRIVATE", "TOKEN", "KEY", "DSN", "CREDENTIAL", "URL", "AUTH", "HEADER"} {
			if word == "HEADER" && isPublicHeaderList {
				continue
			}
			if strings.Contains(u, word) {
				redacted = true
				break
			}
		}
		if redacted {
			m[k] = "[redacted]"
		} else {
			m[k] = v
		}
	}
	return m
}

func (s *Snapshot) Redacted() map[string]string {
	return RedactMap(s.Values())
}

// LoadSnapshot uses .env < .<APP_ENV>.env (or .local.env) < explicit environment.
// Missing conventional files are allowed; malformed files return errors. It
// never writes process environment. Callers choose whether to pass os.Environ().
func LoadSnapshot(dir string, environment []string) (*Snapshot, error) {
	values := map[string]string{}
	env := map[string]string{}
	for _, v := range environment {
		if k, value, ok := strings.Cut(v, "="); ok {
			env[k] = value
		}
	}
	read := func(name string) error {
		m, err := godotenv.Read(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("config %s: %w", name, err)
		}
		for k, v := range m {
			values[k] = v
		}
		return nil
	}
	if err := read(".env"); err != nil {
		return nil, err
	}
	appEnv := values["APP_ENV"]
	if v, ok := env["APP_ENV"]; ok {
		appEnv = v
	}
	file := ".local.env"
	if appEnv != "" {
		if strings.ContainsAny(appEnv, "/\\") || appEnv == ".." {
			return nil, errors.New("APP_ENV must be a simple name")
		}
		file = "." + appEnv + ".env"
	}
	if err := read(file); err != nil {
		return nil, err
	}
	for k, v := range env {
		values[k] = v
	}
	return NewSnapshot(values), nil
}
