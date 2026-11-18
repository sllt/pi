package helper

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

// GetProjectName reads go.mod and extracts the module name.
func GetProjectName(dir string) string {
	modPath := filepath.Join(dir, "go.mod")
	modFile, err := os.Open(modPath)
	if err != nil {
		return ""
	}
	defer modFile.Close()

	scanner := bufio.NewScanner(modFile)
	for scanner.Scan() {
		line := scanner.Text()
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "module" {
			return strings.Trim(fields[1], `"`)
		}
	}
	return ""
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

// ToSnakeCase converts a string to snake_case (e.g., "UserProfile" -> "user_profile",
// "OrderID" -> "order_id", "HTTPServer" -> "http_server").
func ToSnakeCase(s string) string {
	runes := []rune(s)
	var result strings.Builder
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := runes[i-1]
				if prev != '_' && (!unicode.IsUpper(prev) || (i+1 < len(runes) && unicode.IsLower(runes[i+1]))) {
					result.WriteRune('_')
				}
			}
			result.WriteRune(unicode.ToLower(r))
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}
