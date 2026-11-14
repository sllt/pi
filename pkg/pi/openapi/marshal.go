package openapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"gopkg.in/yaml.v3"
)

// MarshalJSONBytes normalizes the document without mutating d and serializes deterministic indented JSON.
func (d *Document) MarshalJSONBytes() ([]byte, error) {
	if d == nil {
		return nil, errors.New("openapi: cannot marshal nil document")
	}
	normalized := normalizeDocument(d)
	return json.MarshalIndent(normalized, "", "  ")
}

// MarshalYAMLBytes serializes deterministic YAML from the exact same normalized document structure as MarshalJSONBytes.
func (d *Document) MarshalYAMLBytes() ([]byte, error) {
	jsonBytes, err := d.MarshalJSONBytes()
	if err != nil {
		return nil, err
	}

	dec := json.NewDecoder(bytes.NewReader(jsonBytes))
	dec.UseNumber()

	root, err := decodeJSONTokenToYAMLNode(dec)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(root); err != nil {
		_ = enc.Close()
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func normalizeDocument(d *Document) *Document {
	out := d.Clone()
	if out.OpenAPI == "" {
		out.OpenAPI = Version303
	}
	if out.Paths == nil {
		out.Paths = map[string]*PathItem{}
	}
	for _, item := range out.Paths {
		if item == nil {
			continue
		}
		normalizeOperation(item.Get)
		normalizeOperation(item.Put)
		normalizeOperation(item.Post)
		normalizeOperation(item.Delete)
		normalizeOperation(item.Patch)
	}
	return out
}

func normalizeOperation(op *Operation) {
	if op == nil {
		return
	}
	if op.Responses == nil {
		op.Responses = map[string]*Response{}
	}
	if op.Security != nil {
		normalized := normalizeSecuritySlice(*op.Security)
		op.Security = &normalized
	}
}

func decodeJSONTokenToYAMLNode(dec *json.Decoder) (*yaml.Node, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	return jsonValueToYAMLNode(dec, tok)
}

func jsonValueToYAMLNode(dec *json.Decoder, tok json.Token) (*yaml.Node, error) {
	switch v := tok.(type) {
	case json.Delim:
		switch v {
		case '{':
			node := &yaml.Node{
				Kind: yaml.MappingNode,
				Tag:  "!!map",
			}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyTok.(string)
				if !ok {
					return nil, fmt.Errorf("openapi: expected JSON object key string, got %T", keyTok)
				}
				valNode, err := decodeJSONTokenToYAMLNode(dec)
				if err != nil {
					return nil, err
				}
				node.Content = append(node.Content,
					&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
					valNode,
				)
			}
			endTok, err := dec.Token()
			if err != nil {
				return nil, err
			}
			if endDelim, ok := endTok.(json.Delim); !ok || endDelim != '}' {
				return nil, fmt.Errorf("openapi: expected '}', got %v", endTok)
			}
			return node, nil
		case '[':
			node := &yaml.Node{
				Kind: yaml.SequenceNode,
				Tag:  "!!seq",
			}
			for dec.More() {
				elemNode, err := decodeJSONTokenToYAMLNode(dec)
				if err != nil {
					return nil, err
				}
				node.Content = append(node.Content, elemNode)
			}
			endTok, err := dec.Token()
			if err != nil {
				return nil, err
			}
			if endDelim, ok := endTok.(json.Delim); !ok || endDelim != ']' {
				return nil, fmt.Errorf("openapi: expected ']', got %v", endTok)
			}
			return node, nil
		default:
			return nil, fmt.Errorf("openapi: unexpected JSON delimiter %q", v)
		}
	case string:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: v}, nil
	case bool:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(v)}, nil
	case json.Number:
		numStr := v.String()
		tag := "!!float"
		if isJSONInteger(numStr) {
			tag = "!!int"
		}
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: numStr}, nil
	case nil:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"}, nil
	default:
		return nil, fmt.Errorf("openapi: unexpected JSON token %T", tok)
	}
}

func isJSONInteger(s string) bool {
	if s == "" {
		return false
	}
	if s[0] == '-' {
		s = s[1:]
		if s == "" {
			return false
		}
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
