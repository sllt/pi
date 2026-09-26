package migration

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/sllt/pi/pkg/pi/cli/helper"
)

// Migrate prepares both files before delivery and never changes process cwd.
func Migrate(name string) (string, error) {
	if !token.IsIdentifier(name) || name == "_" {
		return "", fmt.Errorf("migration name must be a Go identifier: %q", name)
	}
	stamp := time.Now().Format("20060102150405")
	registry := filepath.Join("migrations", "all.go")
	data, err := os.ReadFile(registry)
	replace := err == nil
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	if !replace {
		data = []byte("package migrations\nimport \"github.com/sllt/pi/pkg/pi/migration\"\nfunc All() map[int64]migration.Migrate { return map[int64]migration.Migrate{} }\n")
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, registry, data, parser.ParseComments)
	if err != nil {
		return "", err
	}
	var entries *ast.CompositeLit
	for _, d := range file.Decls {
		f, ok := d.(*ast.FuncDecl)
		if !ok || f.Name.Name != "All" || f.Body == nil {
			continue
		}
		for _, stmt := range f.Body.List {
			r, ok := stmt.(*ast.ReturnStmt)
			if !ok || len(r.Results) != 1 {
				continue
			}
			entries, _ = r.Results[0].(*ast.CompositeLit)
		}
	}
	if entries == nil {
		return "", fmt.Errorf("all.go All must return a map literal; edit this registry manually")
	}
	for _, entry := range entries.Elts {
		kv, ok := entry.(*ast.KeyValueExpr)
		if !ok {
			return "", fmt.Errorf("unsupported migration registry entry")
		}
		key, ok := kv.Key.(*ast.BasicLit)
		if !ok {
			return "", fmt.Errorf("unsupported migration version expression")
		}
		if key.Value == stamp {
			return "", fmt.Errorf("migration timestamp %s already exists; retry after one second", stamp)
		}
		if call, ok := kv.Value.(*ast.CallExpr); ok {
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == name {
				return "", fmt.Errorf("migration %s already registered", name)
			}
		}
	}
	entries.Elts = append(entries.Elts, &ast.KeyValueExpr{Key: &ast.BasicLit{Kind: token.INT, Value: stamp}, Value: &ast.CallExpr{Fun: ast.NewIdent(name)}})
	var rendered bytes.Buffer
	if err = format.Node(&rendered, set, file); err != nil {
		return "", err
	}
	source := fmt.Sprintf("package migrations\nimport (\"context\"; \"github.com/sllt/pi/pkg/pi/migration\")\nfunc %s() migration.Migrate { return migration.Migrate{Name: %s, UpContext: func(ctx context.Context, d migration.Datasource) error {\n// Add migration operations using ctx.\nreturn nil\n}} }\n", name, strconv.Quote(name))
	destination := filepath.Join("migrations", stamp+"_"+name+".go")
	if err = helper.WriteFiles(map[string]helper.File{registry: {Data: rendered.Bytes(), Replace: replace}, destination: {Data: []byte(source)}}); err != nil {
		return "", err
	}
	return "Created migration: " + destination, nil
}
