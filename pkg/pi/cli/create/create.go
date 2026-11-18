package create

import (
	"bytes"
	"errors"
	"fmt"
	"go/format"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/sllt/pi/pkg/pi/cli/helper"
)

var (
	ErrNameEmpty       = errors.New("please provide a name")
	ErrInvalidType     = errors.New("invalid create type")
	ErrNoProjectName   = errors.New("cannot determine project name, ensure go.mod exists")
	ErrCreateFile      = errors.New("failed to create file")
	ErrExecuteTemplate = errors.New("failed to execute template")
)

type CreateData struct {
	ProjectName, CreateType, FilePath, FileName, StructName, StructNameLowerFirst, StructNameSnakeCase string
}

// Options configures component generation behavior.
type Options struct {
	DryRun bool
}

func DTO(name string) (string, error)        { return CreateComponent(name, "dto") }
func Handler(name string) (string, error)    { return CreateComponent(name, "handler") }
func Router(name string) (string, error)     { return CreateComponent(name, "router") }
func Service(name string) (string, error)    { return CreateComponent(name, "service") }
func Repository(name string) (string, error) { return CreateComponent(name, "repository") }
func Model(name string) (string, error)      { return CreateComponent(name, "model") }
func All(name string) (string, error)        { return AllWithOptions(name, Options{}) }
func AllWithOptions(name string, opts Options) (string, error) {
	return generateWithOptions(name, []string{"dto", "handler", "router", "service", "repository", "model"}, opts)
}
func CreateComponent(name, kind string) (string, error) {
	return CreateComponentWithOptions(name, kind, Options{})
}
func CreateComponentWithOptions(name, kind string, opts Options) (string, error) {
	return generateWithOptions(name, []string{kind}, opts)
}

func defaultOutputDir(kind string) string {
	switch kind {
	case "dto":
		return filepath.Join("api", "v1")
	case "router":
		return filepath.Join("internal", "router")
	default:
		return filepath.Join("internal", kind)
	}
}

func generateWithOptions(name string, kinds []string, opts Options) (string, error) {
	if name == "" {
		return "", ErrNameEmpty
	}
	if filepath.IsAbs(name) || name == ".." || strings.HasPrefix(filepath.Clean(name), ".."+string(filepath.Separator)) {
		return "", errors.New("component path must stay inside the project")
	}
	project := helper.GetProjectName(".")
	if project == "" {
		return "", ErrNoProjectName
	}
	dir, base := filepath.Split(name)
	base = strings.TrimSuffix(base, ".go")
	structName := helper.ToCamelCase(base)
	if !token.IsIdentifier(structName) {
		return "", fmt.Errorf("invalid component name %q", name)
	}
	files := map[string]helper.File{}
	var messages []string
	for _, kind := range kinds {
		data := CreateData{project, kind, dir, base, structName, helper.ToLowerFirst(structName), helper.ToSnakeCase(structName)}
		source := GetTemplate(kind)
		if source == "" {
			return "", ErrInvalidType
		}
		tmpl, err := template.New(kind).Parse(source)
		if err != nil {
			return "", fmt.Errorf("%w: %w", ErrExecuteTemplate, err)
		}
		var buf bytes.Buffer
		if err = tmpl.Execute(&buf, &data); err != nil {
			return "", err
		}
		formatted, err := format.Source(buf.Bytes())
		if err != nil {
			return "", fmt.Errorf("format %s: %w", kind, err)
		}
		outDir := dir
		if outDir == "" {
			outDir = defaultOutputDir(kind)
		}
		file := filepath.Join(outDir, data.StructNameSnakeCase+".go")
		if _, err = os.Lstat(file); err == nil {
			if opts.DryRun {
				messages = append(messages, "Would preserve: "+file)
			} else {
				messages = append(messages, "Preserved: "+file)
			}
			continue
		} else if !os.IsNotExist(err) {
			return "", err
		}
		if _, exists := files[file]; exists {
			return "", fmt.Errorf("component output collision: %s; omit the custom directory for create all", file)
		}
		files[file] = helper.File{Data: formatted}
		if opts.DryRun {
			messages = append(messages, "Would create: "+file)
		} else {
			messages = append(messages, "Created: "+file)
		}
	}
	if !opts.DryRun && len(files) > 0 {
		if err := helper.WriteFiles(files); err != nil {
			return "", fmt.Errorf("%w: %w", ErrCreateFile, err)
		}
	}
	return strings.Join(messages, "\n"), nil
}
