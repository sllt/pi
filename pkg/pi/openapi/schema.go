package openapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"gopkg.in/yaml.v3"
)

// AdditionalProperties represents an OpenAPI 3.0.3 schema additionalProperties field,
// which can be either a boolean or a Schema object.
type AdditionalProperties struct {
	Allowed *bool
	Schema  *Schema
}

// AdditionalPropertiesAllowed constructs an AdditionalProperties configured as a boolean flag.
func AdditionalPropertiesAllowed(allowed bool) *AdditionalProperties {
	return &AdditionalProperties{Allowed: &allowed}
}

// AdditionalPropertiesSchema constructs an AdditionalProperties configured as a Schema object.
func AdditionalPropertiesSchema(s *Schema) *AdditionalProperties {
	return &AdditionalProperties{Schema: s}
}

// MarshalJSON encodes AdditionalProperties as a Schema object when Schema is non-nil, or as a JSON boolean.
func (a AdditionalProperties) MarshalJSON() ([]byte, error) {
	if a.Schema != nil {
		return json.Marshal(a.Schema)
	}
	if a.Allowed != nil {
		return json.Marshal(*a.Allowed)
	}
	return []byte("null"), nil
}

// UnmarshalJSON decodes a JSON boolean or Schema object into AdditionalProperties.
func (a *AdditionalProperties) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return errors.New("openapi: empty additionalProperties JSON")
	}
	if bytes.Equal(trimmed, []byte("null")) {
		*a = AdditionalProperties{}
		return nil
	}
	if trimmed[0] == '{' {
		var schema Schema
		if err := json.Unmarshal(trimmed, &schema); err != nil {
			return fmt.Errorf("openapi: invalid additionalProperties schema: %w", err)
		}
		a.Schema = &schema
		a.Allowed = nil
		return nil
	}
	var allowed bool
	if err := json.Unmarshal(trimmed, &allowed); err != nil {
		return fmt.Errorf("openapi: invalid additionalProperties value: %w", err)
	}
	a.Allowed = &allowed
	a.Schema = nil
	return nil
}

// MarshalYAML encodes AdditionalProperties as a Schema mapping when Schema is non-nil, or as a YAML boolean.
func (a AdditionalProperties) MarshalYAML() (any, error) {
	if a.Schema != nil {
		return a.Schema, nil
	}
	if a.Allowed != nil {
		return *a.Allowed, nil
	}
	return nil, nil
}

// UnmarshalYAML decodes a YAML boolean or Schema mapping into AdditionalProperties.
func (a *AdditionalProperties) UnmarshalYAML(value *yaml.Node) error {
	for value != nil && value.Kind == yaml.AliasNode && value.Alias != nil {
		value = value.Alias
	}
	if value == nil || (value.Kind == yaml.ScalarNode && value.Tag == "!!null") {
		*a = AdditionalProperties{}
		return nil
	}
	switch value.Kind {
	case yaml.ScalarNode:
		var allowed bool
		if err := value.Decode(&allowed); err != nil {
			return fmt.Errorf("openapi: invalid additionalProperties YAML scalar: %w", err)
		}
		a.Allowed = &allowed
		a.Schema = nil
		return nil
	case yaml.MappingNode:
		var schema Schema
		if err := value.Decode(&schema); err != nil {
			return fmt.Errorf("openapi: invalid additionalProperties YAML schema: %w", err)
		}
		a.Schema = &schema
		a.Allowed = nil
		return nil
	default:
		return fmt.Errorf("openapi: invalid additionalProperties YAML node kind %d", value.Kind)
	}
}

// Schema represents an OpenAPI 3.0.3 Schema Object.
type Schema struct {
	Ref                  string                `json:"$ref,omitempty" yaml:"$ref,omitempty"`
	Type                 string                `json:"type,omitempty" yaml:"type,omitempty"`
	Format               string                `json:"format,omitempty" yaml:"format,omitempty"`
	Description          string                `json:"description,omitempty" yaml:"description,omitempty"`
	Nullable             bool                  `json:"nullable,omitempty" yaml:"nullable,omitempty"`
	Default              any                   `json:"default,omitempty" yaml:"default,omitempty"`
	Example              any                   `json:"example,omitempty" yaml:"example,omitempty"`
	Enum                 []any                 `json:"enum,omitempty" yaml:"enum,omitempty"`
	Minimum              *float64              `json:"minimum,omitempty" yaml:"minimum,omitempty"`
	Maximum              *float64              `json:"maximum,omitempty" yaml:"maximum,omitempty"`
	MinLength            *int                  `json:"minLength,omitempty" yaml:"minLength,omitempty"`
	MaxLength            *int                  `json:"maxLength,omitempty" yaml:"maxLength,omitempty"`
	MinItems             *int                  `json:"minItems,omitempty" yaml:"minItems,omitempty"`
	MaxItems             *int                  `json:"maxItems,omitempty" yaml:"maxItems,omitempty"`
	Pattern              string                `json:"pattern,omitempty" yaml:"pattern,omitempty"`
	Properties           map[string]*Schema    `json:"properties,omitempty" yaml:"properties,omitempty"`
	Required             []string              `json:"required,omitempty" yaml:"required,omitempty"`
	Items                *Schema               `json:"items,omitempty" yaml:"items,omitempty"`
	AdditionalProperties *AdditionalProperties `json:"additionalProperties,omitempty" yaml:"additionalProperties,omitempty"`
	AllOf                []*Schema             `json:"allOf,omitempty" yaml:"allOf,omitempty"`
}

// Clone returns a deep copy of s, or nil if s is nil.
func (s *Schema) Clone() *Schema {
	if s == nil {
		return nil
	}
	return cloneSchema(s, make(map[*Schema]*Schema))
}

func cloneSchema(s *Schema, active map[*Schema]*Schema) *Schema {
	if s == nil {
		return nil
	}
	if existing, ok := active[s]; ok {
		return existing
	}

	out := &Schema{
		Ref:         s.Ref,
		Type:        s.Type,
		Format:      s.Format,
		Description: s.Description,
		Nullable:    s.Nullable,
		Default:     cloneAny(s.Default, 0),
		Example:     cloneAny(s.Example, 0),
		Minimum:     clonePtr(s.Minimum),
		Maximum:     clonePtr(s.Maximum),
		MinLength:   clonePtr(s.MinLength),
		MaxLength:   clonePtr(s.MaxLength),
		MinItems:    clonePtr(s.MinItems),
		MaxItems:    clonePtr(s.MaxItems),
		Pattern:     s.Pattern,
		Required:    cloneStringSlice(s.Required),
	}

	active[s] = out
	defer delete(active, s)

	if s.Enum != nil {
		out.Enum = make([]any, len(s.Enum))
		for i, v := range s.Enum {
			out.Enum[i] = cloneAny(v, 0)
		}
	}
	if s.Properties != nil {
		out.Properties = make(map[string]*Schema, len(s.Properties))
		for k, prop := range s.Properties {
			out.Properties[k] = cloneSchema(prop, active)
		}
	}
	if s.Items != nil {
		out.Items = cloneSchema(s.Items, active)
	}
	if s.AdditionalProperties != nil {
		out.AdditionalProperties = &AdditionalProperties{
			Allowed: clonePtr(s.AdditionalProperties.Allowed),
			Schema:  cloneSchema(s.AdditionalProperties.Schema, active),
		}
	}
	if s.AllOf != nil {
		out.AllOf = make([]*Schema, len(s.AllOf))
		for i, elem := range s.AllOf {
			out.AllOf[i] = cloneSchema(elem, active)
		}
	}

	return out
}

func clonePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func cloneStringSlice(s []string) []string {
	if s == nil {
		return nil
	}
	out := make([]string, len(s))
	copy(out, s)
	return out
}

const maxCloneAnyDepth = 64

func cloneAny(v any, depth int) any {
	if v == nil || depth >= maxCloneAnyDepth {
		return v
	}

	switch val := v.(type) {
	case string, bool,
		int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, json.Number:
		return val
	case []byte:
		if val == nil {
			return ([]byte)(nil)
		}
		out := make([]byte, len(val))
		copy(out, val)
		return out
	case []string:
		return cloneStringSlice(val)
	case []any:
		if val == nil {
			return ([]any)(nil)
		}
		out := make([]any, len(val))
		for i, item := range val {
			out[i] = cloneAny(item, depth+1)
		}
		return out
	case map[string]any:
		if val == nil {
			return (map[string]any)(nil)
		}
		out := make(map[string]any, len(val))
		for k, item := range val {
			out[k] = cloneAny(item, depth+1)
		}
		return out
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice:
		if rv.IsNil() {
			return v
		}
		out := reflect.MakeSlice(rv.Type(), rv.Len(), rv.Len())
		for i := 0; i < rv.Len(); i++ {
			elem := cloneAny(rv.Index(i).Interface(), depth+1)
			if elem == nil {
				out.Index(i).Set(reflect.Zero(rv.Type().Elem()))
			} else {
				out.Index(i).Set(reflect.ValueOf(elem))
			}
		}
		return out.Interface()
	case reflect.Map:
		if rv.IsNil() {
			return v
		}
		out := reflect.MakeMapWithSize(rv.Type(), rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			val := cloneAny(iter.Value().Interface(), depth+1)
			if val == nil {
				out.SetMapIndex(iter.Key(), reflect.Zero(rv.Type().Elem()))
			} else {
				out.SetMapIndex(iter.Key(), reflect.ValueOf(val))
			}
		}
		return out.Interface()
	case reflect.Ptr:
		if rv.IsNil() {
			return v
		}
		elem := cloneAny(rv.Elem().Interface(), depth+1)
		out := reflect.New(rv.Type().Elem())
		if elem != nil {
			out.Elem().Set(reflect.ValueOf(elem))
		}
		return out.Interface()
	default:
		return v
	}
}
