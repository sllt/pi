package bootstrap

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// Rewrite serialized descriptor options as protobuf data, not text: changing
// the package string without its length prefix corrupts reflection metadata.
func rewriteDescriptor(data []byte, module string) ([]byte, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "generated.pb.go", data, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	for _, decl := range f.Decls {
		g, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range g.Specs {
			v, ok := spec.(*ast.ValueSpec)
			if !ok || len(v.Names) != 1 || len(v.Values) != 1 || !strings.HasSuffix(v.Names[0].Name, "_rawDesc") {
				continue
			}
			var raw []byte
			switch val := v.Values[0].(type) {
			case *ast.CompositeLit:
				for _, e := range val.Elts {
					lit, ok := e.(*ast.BasicLit)
					if !ok {
						return nil, fmt.Errorf("unsupported descriptor expression")
					}
					n, err := strconv.ParseUint(lit.Value, 0, 8)
					if err != nil {
						return nil, err
					}
					raw = append(raw, byte(n))
				}
			default:
				var eval func(ast.Expr) (string, error)
				eval = func(e ast.Expr) (string, error) {
					switch x := e.(type) {
					case *ast.BasicLit:
						return strconv.Unquote(x.Value)
					case *ast.BinaryExpr:
						if x.Op != token.ADD {
							break
						}
						a, e := eval(x.X)
						if e != nil {
							return "", e
						}
						b, e := eval(x.Y)
						return a + b, e
					}
					return "", fmt.Errorf("unsupported descriptor expression")
				}
				s, e := eval(val)
				if e != nil {
					return nil, e
				}
				raw = []byte(s)
			}
			fd := new(descriptorpb.FileDescriptorProto)
			if err := proto.Unmarshal(raw, fd); err != nil {
				return nil, err
			}
			if fd.Options == nil {
				continue
			}
			fd.Options.GoPackage = proto.String(strings.ReplaceAll(fd.GetOptions().GetGoPackage(), oldPackageName, module))
			raw, err = proto.Marshal(fd)
			if err != nil {
				return nil, err
			}
			if _, ok := v.Values[0].(*ast.CompositeLit); ok {
				elts := make([]ast.Expr, len(raw))
				for i, b := range raw {
					elts[i] = &ast.BasicLit{Kind: token.INT, Value: fmt.Sprintf("0x%02x", b)}
				}
				v.Values[0] = &ast.CompositeLit{Type: &ast.ArrayType{Elt: ast.NewIdent("byte")}, Elts: elts}
			} else {
				v.Values[0] = &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(string(raw))}
			}
		}
	}
	var buf bytes.Buffer
	if err := format.Node(&buf, fset, f); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
