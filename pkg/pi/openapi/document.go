package openapi

import "encoding/json"

// Version303 is the supported OpenAPI specification version.
const Version303 = "3.0.3"

// SecurityRequirement lists the required security schemes and scopes for an operation.
type SecurityRequirement map[string][]string

// Document represents an OpenAPI 3.0.3 Document Object.
type Document struct {
	OpenAPI    string               `json:"openapi" yaml:"openapi"`
	Info       Info                 `json:"info" yaml:"info"`
	Servers    []Server             `json:"servers,omitempty" yaml:"servers,omitempty"`
	Paths      map[string]*PathItem `json:"paths" yaml:"paths"`
	Components *Components          `json:"components,omitempty" yaml:"components,omitempty"`
	Tags       []Tag                `json:"tags,omitempty" yaml:"tags,omitempty"`
}

// Info provides metadata about the API.
type Info struct {
	Title       string `json:"title" yaml:"title"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	Version     string `json:"version" yaml:"version"`
}

// Server represents an OpenAPI 3.0.3 Server Object.
type Server struct {
	URL         string `json:"url" yaml:"url"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
}

// Tag adds metadata to a single tag used by an Operation.
type Tag struct {
	Name        string `json:"name" yaml:"name"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
}

// PathItem describes the operations available on a single path.
type PathItem struct {
	Get     *Operation `json:"get,omitempty" yaml:"get,omitempty"`
	Put     *Operation `json:"put,omitempty" yaml:"put,omitempty"`
	Post    *Operation `json:"post,omitempty" yaml:"post,omitempty"`
	Delete  *Operation `json:"delete,omitempty" yaml:"delete,omitempty"`
	Patch   *Operation `json:"patch,omitempty" yaml:"patch,omitempty"`
	Options *Operation `json:"options,omitempty" yaml:"options,omitempty"`
	Head    *Operation `json:"head,omitempty" yaml:"head,omitempty"`
}

// Operation describes a single API operation on a path.
type Operation struct {
	Tags        []string               `json:"tags,omitempty" yaml:"tags,omitempty"`
	Summary     string                 `json:"summary,omitempty" yaml:"summary,omitempty"`
	Description string                 `json:"description,omitempty" yaml:"description,omitempty"`
	OperationID string                 `json:"operationId,omitempty" yaml:"operationId,omitempty"`
	Parameters  []*Parameter           `json:"parameters,omitempty" yaml:"parameters,omitempty"`
	RequestBody *RequestBody           `json:"requestBody,omitempty" yaml:"requestBody,omitempty"`
	Responses   map[string]*Response   `json:"responses" yaml:"responses"`
	Deprecated  bool                   `json:"deprecated,omitempty" yaml:"deprecated,omitempty"`
	Security    *[]SecurityRequirement `json:"security,omitempty" yaml:"security,omitempty"`
}

// MarshalJSON ensures Security emits "security": [] when non-nil and empty, and omits it when nil.
func (o Operation) MarshalJSON() ([]byte, error) {
	type operationAlias Operation
	alias := operationAlias(o)
	if alias.Security != nil {
		normalized := normalizeSecuritySlice(*alias.Security)
		alias.Security = &normalized
	}
	return json.Marshal(alias)
}

// MarshalYAML ensures Security emits "security: []" when non-nil and empty, and omits it when nil.
func (o Operation) MarshalYAML() (any, error) {
	if o.Security != nil && len(*o.Security) == 0 {
		type operationWithEmptySecurity struct {
			Tags        []string              `yaml:"tags,omitempty"`
			Summary     string                `yaml:"summary,omitempty"`
			Description string                `yaml:"description,omitempty"`
			OperationID string                `yaml:"operationId,omitempty"`
			Parameters  []*Parameter          `yaml:"parameters,omitempty"`
			RequestBody *RequestBody          `yaml:"requestBody,omitempty"`
			Responses   map[string]*Response  `yaml:"responses"`
			Deprecated  bool                  `yaml:"deprecated,omitempty"`
			Security    []SecurityRequirement `yaml:"security"`
		}
		return operationWithEmptySecurity{
			Tags:        o.Tags,
			Summary:     o.Summary,
			Description: o.Description,
			OperationID: o.OperationID,
			Parameters:  o.Parameters,
			RequestBody: o.RequestBody,
			Responses:   o.Responses,
			Deprecated:  o.Deprecated,
			Security:    []SecurityRequirement{},
		}, nil
	}

	type operationAlias Operation
	alias := operationAlias(o)
	if alias.Security != nil {
		normalized := normalizeSecuritySlice(*alias.Security)
		alias.Security = &normalized
	}
	return alias, nil
}

// Parameter describes a single operation parameter.
type Parameter struct {
	Name        string  `json:"name" yaml:"name"`
	In          string  `json:"in" yaml:"in"`
	Description string  `json:"description,omitempty" yaml:"description,omitempty"`
	Required    bool    `json:"required,omitempty" yaml:"required,omitempty"`
	Deprecated  bool    `json:"deprecated,omitempty" yaml:"deprecated,omitempty"`
	Schema      *Schema `json:"schema,omitempty" yaml:"schema,omitempty"`
	Example     any     `json:"example,omitempty" yaml:"example,omitempty"`
}

// RequestBody describes a single request body.
type RequestBody struct {
	Description string                `json:"description,omitempty" yaml:"description,omitempty"`
	Required    bool                  `json:"required,omitempty" yaml:"required,omitempty"`
	Content     map[string]*MediaType `json:"content" yaml:"content"`
}

// Response describes a single response from an API Operation.
type Response struct {
	Description string                `json:"description" yaml:"description"`
	Content     map[string]*MediaType `json:"content,omitempty" yaml:"content,omitempty"`
}

// MediaType provides schema and examples for the media type identified by its key.
type MediaType struct {
	Schema  *Schema `json:"schema,omitempty" yaml:"schema,omitempty"`
	Example any     `json:"example,omitempty" yaml:"example,omitempty"`
}

// Components holds reusable objects for different aspects of the OAS.
type Components struct {
	Schemas         map[string]*Schema        `json:"schemas,omitempty" yaml:"schemas,omitempty"`
	SecuritySchemes map[string]SecurityScheme `json:"securitySchemes,omitempty" yaml:"securitySchemes,omitempty"`
}

// SecurityScheme defines a security scheme that can be used by the operations.
type SecurityScheme struct {
	Type             string `json:"type" yaml:"type"`
	Description      string `json:"description,omitempty" yaml:"description,omitempty"`
	Name             string `json:"name,omitempty" yaml:"name,omitempty"`
	In               string `json:"in,omitempty" yaml:"in,omitempty"`
	Scheme           string `json:"scheme,omitempty" yaml:"scheme,omitempty"`
	BearerFormat     string `json:"bearerFormat,omitempty" yaml:"bearerFormat,omitempty"`
	OpenIDConnectURL string `json:"openIdConnectUrl,omitempty" yaml:"openIdConnectUrl,omitempty"`
}

// Clone returns a full deep copy of d, or nil if d is nil.
func (d *Document) Clone() *Document {
	if d == nil {
		return nil
	}

	out := &Document{
		OpenAPI: d.OpenAPI,
		Info:    d.Info,
	}
	if d.Servers != nil {
		out.Servers = make([]Server, len(d.Servers))
		copy(out.Servers, d.Servers)
	}
	if d.Tags != nil {
		out.Tags = make([]Tag, len(d.Tags))
		copy(out.Tags, d.Tags)
	}
	if d.Paths != nil {
		out.Paths = make(map[string]*PathItem, len(d.Paths))
		for pathKey, item := range d.Paths {
			out.Paths[pathKey] = clonePathItem(item)
		}
	}
	if d.Components != nil {
		out.Components = cloneComponents(d.Components)
	}
	return out
}

func clonePathItem(p *PathItem) *PathItem {
	if p == nil {
		return nil
	}
	return &PathItem{
		Get:     cloneOperation(p.Get),
		Put:     cloneOperation(p.Put),
		Post:    cloneOperation(p.Post),
		Delete:  cloneOperation(p.Delete),
		Patch:   cloneOperation(p.Patch),
		Options: cloneOperation(p.Options),
		Head:    cloneOperation(p.Head),
	}
}

func cloneOperation(op *Operation) *Operation {
	if op == nil {
		return nil
	}
	out := &Operation{
		Tags:        cloneStringSlice(op.Tags),
		Summary:     op.Summary,
		Description: op.Description,
		OperationID: op.OperationID,
		RequestBody: cloneRequestBody(op.RequestBody),
		Deprecated:  op.Deprecated,
	}
	if op.Parameters != nil {
		out.Parameters = make([]*Parameter, len(op.Parameters))
		for i, param := range op.Parameters {
			out.Parameters[i] = cloneParameter(param)
		}
	}
	if op.Responses != nil {
		out.Responses = make(map[string]*Response, len(op.Responses))
		for code, resp := range op.Responses {
			out.Responses[code] = cloneResponse(resp)
		}
	}
	if op.Security != nil {
		sec := cloneSecuritySlice(*op.Security)
		out.Security = &sec
	}
	return out
}

func cloneParameter(p *Parameter) *Parameter {
	if p == nil {
		return nil
	}
	return &Parameter{
		Name:        p.Name,
		In:          p.In,
		Description: p.Description,
		Required:    p.Required,
		Deprecated:  p.Deprecated,
		Schema:      p.Schema.Clone(),
		Example:     cloneAny(p.Example, 0),
	}
}

func cloneRequestBody(rb *RequestBody) *RequestBody {
	if rb == nil {
		return nil
	}
	return &RequestBody{
		Description: rb.Description,
		Required:    rb.Required,
		Content:     cloneMediaTypeMap(rb.Content),
	}
}

func cloneResponse(r *Response) *Response {
	if r == nil {
		return nil
	}
	return &Response{
		Description: r.Description,
		Content:     cloneMediaTypeMap(r.Content),
	}
}

func cloneMediaTypeMap(m map[string]*MediaType) map[string]*MediaType {
	if m == nil {
		return nil
	}
	out := make(map[string]*MediaType, len(m))
	for k, mt := range m {
		if mt == nil {
			out[k] = nil
			continue
		}
		out[k] = &MediaType{
			Schema:  mt.Schema.Clone(),
			Example: cloneAny(mt.Example, 0),
		}
	}
	return out
}

func cloneComponents(c *Components) *Components {
	if c == nil {
		return nil
	}
	out := &Components{}
	if c.Schemas != nil {
		out.Schemas = make(map[string]*Schema, len(c.Schemas))
		for k, s := range c.Schemas {
			out.Schemas[k] = s.Clone()
		}
	}
	if c.SecuritySchemes != nil {
		out.SecuritySchemes = make(map[string]SecurityScheme, len(c.SecuritySchemes))
		for k, scheme := range c.SecuritySchemes {
			out.SecuritySchemes[k] = scheme
		}
	}
	return out
}

func cloneSecuritySlice(in []SecurityRequirement) []SecurityRequirement {
	out := make([]SecurityRequirement, len(in))
	for i, item := range in {
		if item == nil {
			continue
		}
		copied := make(SecurityRequirement, len(item))
		for k, scopes := range item {
			copied[k] = cloneStringSlice(scopes)
		}
		out[i] = copied
	}
	return out
}

func normalizeSecuritySlice(in []SecurityRequirement) []SecurityRequirement {
	out := make([]SecurityRequirement, len(in))
	for i, item := range in {
		if item == nil {
			out[i] = SecurityRequirement{}
			continue
		}
		copied := make(SecurityRequirement, len(item))
		for k, scopes := range item {
			if scopes == nil {
				copied[k] = []string{}
			} else {
				copied[k] = cloneStringSlice(scopes)
			}
		}
		out[i] = copied
	}
	return out
}
