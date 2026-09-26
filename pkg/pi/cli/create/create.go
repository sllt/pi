package create

import (
	"bytes"
	"errors"
	"fmt"
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

func Handler(name string) (string, error)    { return CreateComponent(name, "handler") }
func Service(name string) (string, error)    { return CreateComponent(name, "service") }
func Repository(name string) (string, error) { return CreateComponent(name, "repository") }
func Model(name string) (string, error)      { return CreateComponent(name, "model") }
func All(name string) (string, error) {
	return generate(name, []string{"handler", "service", "repository", "model"})
}
func CreateComponent(name, kind string) (string, error) { return generate(name, []string{kind}) }

func generate(name string, kinds []string) (string, error) {
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
		outDir := dir
		if outDir == "" {
			outDir = filepath.Join("internal", kind)
		}
		file := filepath.Join(outDir, strings.ToLower(base)+".go")
		if _, err = os.Lstat(file); err == nil {
			messages = append(messages, "Preserved: "+file)
			continue
		} else if !os.IsNotExist(err) {
			return "", err
		}
		if _, exists := files[file]; exists {
			return "", fmt.Errorf("component output collision: %s; omit the custom directory for create all", file)
		}
		files[file] = helper.File{Data: buf.Bytes()}
		messages = append(messages, "Created: "+file)
	}
	if err := helper.WriteFiles(files); err != nil {
		return "", fmt.Errorf("%w: %w", ErrCreateFile, err)
	}
	return strings.Join(messages, "\n"), nil
}
