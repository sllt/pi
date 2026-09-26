package bootstrap

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

const (
	repoURL        = "https://github.com/sllt/pi-layout.git"
	oldPackageName = "github.com/sllt/pi-layout"
)

var (
	ErrNameEmpty     = errors.New("please provide the project name")
	ErrCloneFailed   = errors.New("failed to clone pi-layout repository")
	ErrReplaceFailed = errors.New("failed to replace package name")
	ErrModEditFailed = errors.New("failed to update go.mod module name")
	ErrModTidyFailed = errors.New("failed to run go mod tidy")
	ErrProjectExists = errors.New("project directory already exists")
)

// Create initializes a new Pi project by cloning pi-layout and replacing package names.
func Create(projectName string) error {
	if projectName == "" {
		return ErrNameEmpty
	}

	// Check if directory already exists
	if stat, _ := os.Stat(projectName); stat != nil {
		return ErrProjectExists
	}

	// Step 1: Clone the repository
	fmt.Printf("Cloning pi-layout from %s...\n", repoURL)
	if err := gitClone(projectName); err != nil {
		return fmt.Errorf("%w: %v", ErrCloneFailed, err)
	}

	// Step 2: Replace package names in all .go files
	fmt.Printf("Replacing package name to %s...\n", projectName)
	if err := replacePackageName(projectName); err != nil {
		return fmt.Errorf("%w: %v", ErrReplaceFailed, err)
	}

	// Step 3: Update go.mod module name
	fmt.Println("Updating go.mod module name...")
	if err := updateGoMod(projectName); err != nil {
		return fmt.Errorf("%w: %v", ErrModEditFailed, err)
	}

	// Step 4: Remove .git directory
	fmt.Println("Removing .git directory...")
	os.RemoveAll(filepath.Join(projectName, ".git"))

	// Step 5: Run go mod tidy
	fmt.Println("Running go mod tidy...")
	if err := goModTidy(projectName); err != nil {
		fmt.Printf("Warning: go mod tidy failed: %v (you may need to run it manually)\n", err)
	}

	fmt.Printf("\nProject %s created successfully!\n\n", projectName)
	fmt.Printf("Next steps:\n")
	fmt.Printf("  cd %s\n", projectName)
	fmt.Printf("  go run ./cmd/server\n\n")

	return nil
}

// gitClone clones the pi-layout repository to the specified directory.
func gitClone(projectName string) error {
	cmd := exec.Command("git", "clone", repoURL, projectName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, string(output))
	}
	return nil
}

// replacePackageName replaces all occurrences of the old package name with the new project name.
func replacePackageName(projectName string) error {
	return filepath.Walk(projectName, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		newData := bytes.ReplaceAll(data, []byte(oldPackageName), []byte(projectName))
		if err := os.WriteFile(path, newData, 0644); err != nil {
			return err
		}
		return nil
	})
}

// updateGoMod updates the module name in go.mod.
func updateGoMod(projectName string) error {
	cmd := exec.Command("go", "mod", "edit", "-module", projectName)
	cmd.Dir = projectName
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, string(output))
	}
	return nil
}

// goModTidy runs go mod tidy in the project directory.
func goModTidy(projectName string) error {
	cmd := exec.Command("go", "mod", "tidy")
	cmd.Dir = projectName
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, string(output))
	}
	return nil
}
