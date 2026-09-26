package helper

import (
	"fmt"
	"os"
	"strings"
	"unicode"
)

// GetProjectName reads go.mod and extracts the module name.
func GetProjectName(dir string) string {
	modPath := dir + "/go.mod"
	modFile, err := os.Open(modPath)
	if err != nil {
		fmt.Println("go.mod does not exist:", err)
		return ""
	}
	defer modFile.Close()

	var moduleName string
	_, err = fmt.Fscanf(modFile, "module %s", &moduleName)
	if err != nil {
		fmt.Println("read go mod error:", err)
		return ""
	}
	return moduleName
}

// ToCamelCase converts a string to CamelCase (e.g., "user_profile" -> "UserProfile").
func ToCamelCase(s string) string {
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, ".", "_")
	parts := strings.Split(s, "_")
	var result strings.Builder
	for _, part := range parts {
		if part == "" {
			continue
		}
		result.WriteString(strings.ToUpper(part[:1]) + part[1:])
	}
	return result.String()
}

// ToLowerFirst converts the first character to lowercase.
func ToLowerFirst(s string) string {
	if s == "" {
		return ""
	}
	r := []rune(s)
	r[0] = unicode.ToLower(r[0])
	return string(r)
}

// ToSnakeCase converts a string to snake_case (e.g., "UserProfile" -> "user_profile").
func ToSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				result.WriteRune('_')
			}
			result.WriteRune(unicode.ToLower(r))
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}
