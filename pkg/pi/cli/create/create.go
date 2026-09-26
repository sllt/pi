package create

import (
	"errors"
	"fmt"
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

// CreateData holds the data structure for template execution.
type CreateData struct {
	ProjectName          string
	CreateType           string
	FilePath             string
	FileName             string
	StructName           string
	StructNameLowerFirst string
	StructNameSnakeCase  string
}

// Handler creates a new handler file.
func Handler(name string) (string, error) {
	return CreateComponent(name, "handler")
}

// Service creates a new service file.
func Service(name string) (string, error) {
	return CreateComponent(name, "service")
}

// Repository creates a new repository file.
func Repository(name string) (string, error) {
	return CreateComponent(name, "repository")
}

// Model creates a new model file.
func Model(name string) (string, error) {
	return CreateComponent(name, "model")
}

// All creates handler, service, repository, and model files.
func All(name string) (string, error) {
	if name == "" {
		return "", ErrNameEmpty
	}

	results := make([]string, 0, 4)
	types := []string{"handler", "service", "repository", "model"}

	for _, t := range types {
		result, err := CreateComponent(name, t)
		if err != nil {
			return "", err
		}
		results = append(results, result)
	}

	return strings.Join(results, "\n"), nil
}

// CreateComponent creates a component file for the given type.
func CreateComponent(name, createType string) (string, error) {
	if name == "" {
		return "", ErrNameEmpty
	}

	projectName := helper.GetProjectName(".")
	if projectName == "" {
		return "", ErrNoProjectName
	}

	// Parse name (may include path like "user/profile")
	filePath, fileName := filepath.Split(name)
	fileName = strings.TrimSuffix(fileName, ".go")

	// Generate struct name and variants
	structName := helper.ToCamelCase(fileName)
	structNameLowerFirst := helper.ToLowerFirst(structName)
	structNameSnakeCase := helper.ToSnakeCase(structName)

	data := &CreateData{
		ProjectName:          projectName,
		CreateType:           createType,
		FilePath:             filePath,
		FileName:             fileName,
		StructName:           structName,
		StructNameLowerFirst: structNameLowerFirst,
		StructNameSnakeCase:  structNameSnakeCase,
	}

	return generateFile(data)
}

// generateFile generates the file for the given CreateData.
func generateFile(data *CreateData) (string, error) {
	// Determine output directory
	dirPath := data.FilePath
	if dirPath == "" {
		dirPath = fmt.Sprintf("internal/%s/", data.CreateType)
	}

	// Create directory if not exists
	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return "", fmt.Errorf("%w: %v", ErrCreateFile, err)
	}

	// Generate file path
	outputFile := filepath.Join(dirPath, strings.ToLower(data.FileName)+".go")

	// Check if file already exists
	if _, err := os.Stat(outputFile); err == nil {
		return fmt.Sprintf("Skipped: %s (already exists)", outputFile), nil
	}

	// Create the file
	f, err := os.Create(outputFile)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrCreateFile, err)
	}
	defer f.Close()

	// Get template
	tmplContent := GetTemplate(data.CreateType)
	if tmplContent == "" {
		return "", ErrInvalidType
	}

	// Parse and execute template
	tmpl, err := template.New(data.CreateType).Parse(tmplContent)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrExecuteTemplate, err)
	}

	if err := tmpl.Execute(f, data); err != nil {
		return "", fmt.Errorf("%w: %v", ErrExecuteTemplate, err)
	}

	return fmt.Sprintf("Created new %s: %s", data.CreateType, outputFile), nil
}
