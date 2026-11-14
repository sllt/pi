package contract

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"

	"github.com/sllt/pi/pkg/pi/apperror"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/logging"
)

const maxValidationDetails = 32

type fieldViolation struct {
	Field string
	Rule  string
}

type unsupportedMediaTypeError struct{}

func (unsupportedMediaTypeError) Error() string         { return "unsupported media type" }
func (unsupportedMediaTypeError) PublicMessage() string { return "unsupported media type" }
func (unsupportedMediaTypeError) StatusCode() int       { return http.StatusUnsupportedMediaType }
func (unsupportedMediaTypeError) Code() int             { return http.StatusUnsupportedMediaType }
func (unsupportedMediaTypeError) LogLevel() logging.Level {
	return logging.INFO
}

func newInvalidArgumentError(publicMsg string, violations []fieldViolation) *apperror.Error {
	if publicMsg == "" {
		publicMsg = "invalid request parameters"
	}
	err := apperror.New(apperror.InvalidArgument, http.StatusBadRequest, publicMsg)
	if len(violations) == 0 {
		return err
	}
	details := sanitizeAndSortDetails(violations)
	if len(details) == 0 {
		return err
	}
	return err.WithDetails(details...)
}

func sanitizeAndSortDetails(violations []fieldViolation) []apperror.Detail {
	if len(violations) == 0 {
		return nil
	}
	details := make([]apperror.Detail, 0, len(violations))
	for _, v := range violations {
		field := strings.TrimSpace(v.Field)
		rule := strings.TrimSpace(v.Rule)
		if field == "" && rule == "" {
			continue
		}
		if rule == "" {
			rule = "invalid"
		}
		details = append(details, apperror.Detail{
			Field: field,
			Rule:  rule,
		})
	}
	if len(details) == 0 {
		return nil
	}
	sort.Slice(details, func(i, j int) bool {
		if details[i].Field != details[j].Field {
			return details[i].Field < details[j].Field
		}
		return details[i].Rule < details[j].Rule
	})

	deduped := details[:0]
	for _, d := range details {
		if len(deduped) > 0 && deduped[len(deduped)-1] == d {
			continue
		}
		deduped = append(deduped, d)
	}
	if len(deduped) > maxValidationDetails {
		deduped = deduped[:maxValidationDetails]
	}
	return deduped
}

func splitValidatorRules(tag string) []string {
	if tag == "" {
		return nil
	}
	var rules []string
	start := 0
	inQuote := false
	for i := 0; i < len(tag); i++ {
		switch tag[i] {
		case '\'':
			inQuote = !inQuote
		case ',':
			if !inQuote {
				rules = append(rules, tag[start:i])
				start = i + 1
			}
		}
	}
	rules = append(rules, tag[start:])
	return rules
}

func hasUnconditionalRequired(tag string) bool {
	for _, rawRule := range splitValidatorRules(tag) {
		rule := strings.TrimSpace(rawRule)
		switch rule {
		case "dive", "omitempty", "omitnil", "omitzero":
			return false
		case "required":
			return true
		}
	}
	return false
}

var (
	singleCrossFieldTags = map[string]struct{}{
		"eqfield":                       {},
		"nefield":                       {},
		"gtfield":                       {},
		"gtefield":                      {},
		"ltfield":                       {},
		"ltefield":                      {},
		"fieldcontains":                 {},
		"fieldexcludes":                 {},
		"postcode_iso3166_alpha2_field": {},
	}
	firstTokenCrossFieldTags = map[string]struct{}{
		"required_if":     {},
		"required_unless": {},
		"excluded_if":     {},
		"excluded_unless": {},
		"skip_unless":     {},
	}
	allTokensCrossFieldTags = map[string]struct{}{
		"required_with":        {},
		"required_with_all":    {},
		"required_without":     {},
		"required_without_all": {},
		"excluded_with":        {},
		"excluded_with_all":    {},
		"excluded_without":     {},
		"excluded_without_all": {},
	}
	rootCrossFieldTags = map[string]struct{}{
		"eqcsfield":  {},
		"necsfield":  {},
		"gtcsfield":  {},
		"gtecsfield": {},
		"ltcsfield":  {},
		"ltecsfield": {},
	}
)

func isCrossFieldRuleName(name string) bool {
	if _, ok := singleCrossFieldTags[name]; ok {
		return true
	}
	if _, ok := firstTokenCrossFieldTags[name]; ok {
		return true
	}
	if _, ok := allTokensCrossFieldTags[name]; ok {
		return true
	}
	if _, ok := rootCrossFieldTags[name]; ok {
		return true
	}
	return false
}

func splitValidatorBranches(rule string) []string {
	var branches []string
	start := 0
	inQuote := false
	for i := 0; i < len(rule); i++ {
		switch rule[i] {
		case '\'':
			inQuote = !inQuote
		case '|':
			if !inQuote {
				branches = append(branches, rule[start:i])
				start = i + 1
			}
		}
	}
	branches = append(branches, rule[start:])
	return branches
}

func parseValidatorTokens(param string) ([]string, bool) {
	var tokens []string
	var cur strings.Builder
	inQuote := false
	hasToken := false
	for i := 0; i < len(param); i++ {
		c := param[i]
		if c == '\'' {
			inQuote = !inQuote
			hasToken = true
			continue
		}
		if !inQuote && (c == ' ' || c == '\t') {
			if hasToken {
				tokens = append(tokens, cur.String())
				cur.Reset()
				hasToken = false
			}
			continue
		}
		cur.WriteByte(c)
		hasToken = true
	}
	if inQuote {
		return nil, false
	}
	if hasToken {
		tokens = append(tokens, cur.String())
	}
	return tokens, true
}

func validateCrossFieldRefsForStruct(rootType, t reflect.Type, visited map[reflect.Type]bool, getValidator func(usedBinding bool) *validator.Validate) error {
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		t = t.Elem()
	}
	if t.Kind() == reflect.Map {
		if err := validateCrossFieldRefsForStruct(rootType, t.Key(), visited, getValidator); err != nil {
			return err
		}
		return validateCrossFieldRefsForStruct(rootType, t.Elem(), visited, getValidator)
	}
	if t.Kind() != reflect.Struct || isSpecialStructType(t) {
		return nil
	}
	if visited[t] {
		return nil
	}
	visited[t] = true

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			ft := f.Type
			for ft.Kind() == reflect.Ptr {
				ft = ft.Elem()
			}
			if f.Anonymous && ft.Kind() == reflect.Struct && !isSpecialStructType(ft) {
				if err := validateCrossFieldRefsForStruct(rootType, ft, visited, getValidator); err != nil {
					return err
				}
			}
			continue
		}

		tagExpr, usedBinding, err := resolveFieldValidationTag(f, t)
		if err != nil {
			return err
		}
		if tagExpr != "" {
			tagName := "validate"
			if usedBinding {
				tagName = "binding"
			}
			if err := checkFieldValidationTagSyntax(rootType, t, f, tagExpr, tagName, getValidator(usedBinding)); err != nil {
				return err
			}
		}

		if err := validateCrossFieldRefsForStruct(rootType, f.Type, visited, getValidator); err != nil {
			return err
		}
	}
	return nil
}

func isNilPassThroughConditionalRule(name string) bool {
	if _, ok := firstTokenCrossFieldTags[name]; ok {
		return true
	}
	_, ok := allTokensCrossFieldTags[name]
	return ok
}

func checkFieldValidationTagSyntax(rootType, enclosingStruct reflect.Type, f reflect.StructField, tagExpr, tagName string, v *validator.Validate) error {
	if strings.TrimSpace(tagExpr) == "-" {
		return nil
	}

	curType := f.Type
	inDive := false
	inMapDive := false
	inMapKeys := false
	sawOmitOrDefault := false
	sawNilPassThroughCond := false
	var mapKeyType, mapValType reflect.Type

	rules := splitValidatorRules(tagExpr)
	for _, rawRule := range rules {
		rule := strings.TrimSpace(rawRule)
		if rule == "" {
			return fmt.Errorf("field %s.%s has empty rule segment in validation tag %q", typeDisplayName(enclosingStruct), f.Name, tagExpr)
		}

		switch rule {
		case "dive":
			if curType.Kind() == reflect.Ptr && sawNilPassThroughCond && !sawOmitOrDefault {
				return fmt.Errorf("field %s.%s cannot use dive on pointer type %s after conditional validation without an intervening omitempty, omitnil, or omitzero in validation tag %q", typeDisplayName(enclosingStruct), f.Name, typeDisplayName(curType), tagExpr)
			}
			elemBase := curType
			for elemBase.Kind() == reflect.Ptr {
				elemBase = elemBase.Elem()
			}
			switch elemBase.Kind() {
			case reflect.Slice, reflect.Array:
				curType = elemBase.Elem()
				inDive = true
				inMapDive = false
				inMapKeys = false
				mapKeyType = nil
				mapValType = nil
			case reflect.Map:
				inDive = true
				inMapDive = true
				inMapKeys = false
				mapKeyType = elemBase.Key()
				mapValType = elemBase.Elem()
				curType = mapValType
			default:
				return fmt.Errorf("field %s.%s uses dive on non-iterable type %s", typeDisplayName(enclosingStruct), f.Name, typeDisplayName(curType))
			}
			sawOmitOrDefault = false
			sawNilPassThroughCond = false
			continue

		case "keys":
			if !inMapDive || inMapKeys {
				return fmt.Errorf("field %s.%s uses keys outside active map dive in validation tag %q", typeDisplayName(enclosingStruct), f.Name, tagExpr)
			}
			inMapKeys = true
			curType = mapKeyType
			sawOmitOrDefault = false
			sawNilPassThroughCond = false
			continue

		case "endkeys":
			if !inMapKeys {
				return fmt.Errorf("field %s.%s uses endkeys without matching keys in validation tag %q", typeDisplayName(enclosingStruct), f.Name, tagExpr)
			}
			inMapKeys = false
			curType = mapValType
			sawOmitOrDefault = false
			sawNilPassThroughCond = false
			continue

		case "omitempty", "omitnil", "omitzero":
			sawOmitOrDefault = true
			sawNilPassThroughCond = false
			continue

		case "structonly", "nostructlevel":
			base := curType
			for base.Kind() == reflect.Ptr {
				base = base.Elem()
			}
			if base.Kind() != reflect.Struct {
				return fmt.Errorf("field %s.%s uses %s on non-struct type %s", typeDisplayName(enclosingStruct), f.Name, rule, typeDisplayName(curType))
			}
			continue
		}

		branches := splitValidatorBranches(rule)
		for _, rawBranch := range branches {
			branch := strings.TrimSpace(rawBranch)
			if branch == "" {
				return fmt.Errorf("field %s.%s has empty OR branch in validation tag %q", typeDisplayName(enclosingStruct), f.Name, tagExpr)
			}
			if len(branches) > 1 {
				switch branch {
				case "dive", "keys", "endkeys", "omitempty", "omitnil", "omitzero", "structonly", "nostructlevel":
					return fmt.Errorf("field %s.%s cannot use control tag %q inside OR expression %q", typeDisplayName(enclosingStruct), f.Name, branch, rule)
				}
			}

			name, param, hasEq := strings.Cut(branch, "=")
			name = strings.TrimSpace(name)

			isNilCond := isNilPassThroughConditionalRule(name)
			if curType.Kind() == reflect.Ptr && sawNilPassThroughCond && !sawOmitOrDefault &&
				name != "isdefault" && name != "required" && !isNilCond {
				return fmt.Errorf("field %s.%s rule %q after conditional validation on pointer type %s requires omitempty, omitnil, or omitzero to prevent nil runtime panics", typeDisplayName(enclosingStruct), f.Name, branch, typeDisplayName(curType))
			}

			if isCrossFieldRuleName(name) {
				if err := checkSingleRuleCrossRef(rootType, enclosingStruct, f, branch, inDive || inMapKeys); err != nil {
					return err
				}
				if !inDive && !inMapKeys {
					if _, isRoot := rootCrossFieldTags[name]; !isRoot || enclosingStruct == rootType {
						if err := checkSingleStructRule(v, enclosingStruct, f, tagName, branch); err != nil {
							return err
						}
						if _, isPairTag := firstTokenCrossFieldTags[name]; isPairTag && hasEq {
							decodedParam := decodeValidatorEscapes(strings.TrimSpace(param))
							if fields, ok := parseValidatorTokens(decodedParam); ok {
								for i := 0; i+1 < len(fields); i += 2 {
									pairRule := formatSinglePairRule(name, fields[i], fields[i+1])
									if err := checkSingleStructRule(v, enclosingStruct, f, tagName, pairRule); err != nil {
										return err
									}
								}
							}
						}
					}
				}
				if isNilCond && !sawOmitOrDefault {
					sawNilPassThroughCond = true
				}
				continue
			}

			if err := checkSingleVarRule(v, curType, branch, enclosingStruct, f); err != nil {
				return err
			}
			if len(branches) == 1 && name == "isdefault" && !sawNilPassThroughCond {
				sawOmitOrDefault = true
			} else if len(branches) == 1 && name == "required" {
				sawNilPassThroughCond = false
			}
		}
	}

	if inMapKeys {
		return fmt.Errorf("field %s.%s has unclosed keys without endkeys in validation tag %q", typeDisplayName(enclosingStruct), f.Name, tagExpr)
	}
	return nil
}

func checkSingleRuleCrossRef(rootType, enclosingStruct reflect.Type, f reflect.StructField, rule string, inNestedElem bool) error {
	if rule == "" {
		return fmt.Errorf("field %s.%s has empty OR branch in validation tag", typeDisplayName(enclosingStruct), f.Name)
	}
	name, param, hasEq := strings.Cut(rule, "=")
	name = strings.TrimSpace(name)
	param = strings.TrimSpace(param)

	if _, ok := singleCrossFieldTags[name]; ok {
		if inNestedElem {
			return fmt.Errorf("field %s.%s cannot use sibling cross-field rule %s inside dive or map keys", typeDisplayName(enclosingStruct), f.Name, name)
		}
		if name == "postcode_iso3166_alpha2_field" {
			if !hasEq || param == "" {
				return fmt.Errorf("field %s.%s requires target field reference in %s", typeDisplayName(enclosingStruct), f.Name, name)
			}
			tokens, ok := parseValidatorTokens(decodeValidatorEscapes(param))
			if !ok || len(tokens) != 1 || tokens[0] == "" {
				return fmt.Errorf("field %s.%s requires single field reference in %s", typeDisplayName(enclosingStruct), f.Name, name)
			}
			targetType, ok := resolveStructFieldPathType(enclosingStruct, tokens[0])
			if !ok {
				return fmt.Errorf("field %s.%s references unknown sibling field %q in %s", typeDisplayName(enclosingStruct), f.Name, tokens[0], name)
			}
			if targetType.Kind() != reflect.String {
				return fmt.Errorf("field %s.%s target field %q in %s must be non-pointer string, got %s", typeDisplayName(enclosingStruct), f.Name, tokens[0], name, typeDisplayName(targetType))
			}
			if fieldBase := unwrapPtrType(f.Type); fieldBase.Kind() != reflect.String {
				return fmt.Errorf("field %s.%s must be string to use %s, got %s", typeDisplayName(enclosingStruct), f.Name, name, typeDisplayName(f.Type))
			}
			return nil
		}
		if !hasEq || param == "" {
			return fmt.Errorf("field %s.%s references unknown sibling field %q in %s", typeDisplayName(enclosingStruct), f.Name, param, name)
		}
		if _, ok := resolveStructFieldPathType(enclosingStruct, param); !ok {
			return fmt.Errorf("field %s.%s references unknown sibling field %q in %s", typeDisplayName(enclosingStruct), f.Name, param, name)
		}
		return nil
	}
	if _, ok := firstTokenCrossFieldTags[name]; ok {
		if inNestedElem {
			return fmt.Errorf("field %s.%s cannot use sibling cross-field rule %s inside dive or map keys", typeDisplayName(enclosingStruct), f.Name, name)
		}
		if !hasEq {
			return fmt.Errorf("field %s.%s requires field/value pairs in %s", typeDisplayName(enclosingStruct), f.Name, name)
		}
		fields, ok := parseValidatorTokens(decodeValidatorEscapes(param))
		if !ok || len(fields) < 2 || len(fields)%2 != 0 {
			return fmt.Errorf("field %s.%s requires even number of field/value tokens in %s", typeDisplayName(enclosingStruct), f.Name, name)
		}
		var seenFields map[string]struct{}
		if name == "required_if" {
			seenFields = make(map[string]struct{}, len(fields)/2)
		}
		for i := 0; i < len(fields); i += 2 {
			fieldPath := fields[i]
			lit := fields[i+1]
			if fieldPath == "" {
				return fmt.Errorf("field %s.%s references unknown sibling field %q in %s", typeDisplayName(enclosingStruct), f.Name, fieldPath, name)
			}
			if seenFields != nil {
				if _, dup := seenFields[fieldPath]; dup {
					return fmt.Errorf("field %s.%s has duplicate field %q in %s", typeDisplayName(enclosingStruct), f.Name, fieldPath, name)
				}
				seenFields[fieldPath] = struct{}{}
			}
			targetType, ok := resolveStructFieldPathType(enclosingStruct, fieldPath)
			if !ok {
				return fmt.Errorf("field %s.%s references unknown sibling field %q in %s", typeDisplayName(enclosingStruct), f.Name, fieldPath, name)
			}
			if err := validateConditionalFieldLiteral(enclosingStruct, f, name, fieldPath, targetType, lit); err != nil {
				return err
			}
		}
		return nil
	}
	if _, ok := allTokensCrossFieldTags[name]; ok {
		if inNestedElem {
			return fmt.Errorf("field %s.%s cannot use sibling cross-field rule %s inside dive or map keys", typeDisplayName(enclosingStruct), f.Name, name)
		}
		if !hasEq {
			return fmt.Errorf("field %s.%s requires field reference in %s", typeDisplayName(enclosingStruct), f.Name, name)
		}
		fields, ok := parseValidatorTokens(param)
		if !ok || len(fields) == 0 {
			return fmt.Errorf("field %s.%s requires field reference in %s", typeDisplayName(enclosingStruct), f.Name, name)
		}
		for _, tok := range fields {
			if tok == "" {
				return fmt.Errorf("field %s.%s references unknown sibling field %q in %s", typeDisplayName(enclosingStruct), f.Name, tok, name)
			}
			if _, ok := resolveStructFieldPathType(enclosingStruct, tok); !ok {
				return fmt.Errorf("field %s.%s references unknown sibling field %q in %s", typeDisplayName(enclosingStruct), f.Name, tok, name)
			}
		}
		return nil
	}
	if _, ok := rootCrossFieldTags[name]; ok {
		if !hasEq || param == "" {
			return fmt.Errorf("field %s.%s references unknown struct field %q in %s", typeDisplayName(enclosingStruct), f.Name, param, name)
		}
		if _, ok := resolveStructFieldPathType(rootType, param); !ok {
			return fmt.Errorf("field %s.%s references unknown struct field %q in %s", typeDisplayName(enclosingStruct), f.Name, param, name)
		}
	}
	return nil
}

func decodeValidatorEscapes(s string) string {
	s = strings.ReplaceAll(s, "0x2C", ",")
	s = strings.ReplaceAll(s, "0x7C", "|")
	return s
}

func formatSinglePairRule(name, fieldPath, lit string) string {
	encodedLit := strings.ReplaceAll(lit, ",", "0x2C")
	encodedLit = strings.ReplaceAll(encodedLit, "|", "0x7C")
	if encodedLit == "" || strings.ContainsAny(encodedLit, " \t") {
		encodedLit = "'" + encodedLit + "'"
	}
	return name + "=" + fieldPath + " " + encodedLit
}

func unwrapPtrType(t reflect.Type) reflect.Type {
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func checkValidatorAsInt(lit string) error {
	_, err := strconv.ParseInt(lit, 0, 64)
	return err
}

func validateConditionalFieldLiteral(enclosingStruct reflect.Type, f reflect.StructField, ruleName, fieldPath string, targetType reflect.Type, lit string) error {
	base := unwrapPtrType(targetType)
	switch base.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if err := checkValidatorAsInt(lit); err != nil {
			return fmt.Errorf("field %s.%s rule %s uses invalid integer literal %q for %s (%s): %w", typeDisplayName(enclosingStruct), f.Name, ruleName, lit, fieldPath, typeDisplayName(targetType), err)
		}
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if _, err := strconv.ParseUint(lit, 0, 64); err != nil {
			return fmt.Errorf("field %s.%s rule %s uses invalid unsigned integer literal %q for %s (%s): %w", typeDisplayName(enclosingStruct), f.Name, ruleName, lit, fieldPath, typeDisplayName(targetType), err)
		}
		return nil
	case reflect.Float32:
		v, err := strconv.ParseFloat(lit, 32)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("field %s.%s rule %s uses invalid float32 literal %q for %s (%s)", typeDisplayName(enclosingStruct), f.Name, ruleName, lit, fieldPath, typeDisplayName(targetType))
		}
		return nil
	case reflect.Float64:
		v, err := strconv.ParseFloat(lit, 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("field %s.%s rule %s uses invalid float64 literal %q for %s (%s)", typeDisplayName(enclosingStruct), f.Name, ruleName, lit, fieldPath, typeDisplayName(targetType))
		}
		return nil
	case reflect.Bool:
		if lit != "true" && lit != "false" {
			return fmt.Errorf("field %s.%s rule %s uses invalid boolean literal %q for %s (%s)", typeDisplayName(enclosingStruct), f.Name, ruleName, lit, fieldPath, typeDisplayName(targetType))
		}
		return nil
	case reflect.Slice, reflect.Map:
		if lit == "nil" {
			return nil
		}
		if err := checkValidatorAsInt(lit); err != nil {
			return fmt.Errorf("field %s.%s rule %s uses invalid length literal %q for %s (%s): %w", typeDisplayName(enclosingStruct), f.Name, ruleName, lit, fieldPath, typeDisplayName(targetType), err)
		}
		return nil
	case reflect.Array:
		if err := checkValidatorAsInt(lit); err != nil {
			return fmt.Errorf("field %s.%s rule %s uses invalid array length literal %q for %s (%s): %w", typeDisplayName(enclosingStruct), f.Name, ruleName, lit, fieldPath, typeDisplayName(targetType), err)
		}
		return nil
	case reflect.String, reflect.Struct:
		return nil
	default:
		return fmt.Errorf("field %s.%s rule %s uses unsupported target field type %s (%s)", typeDisplayName(enclosingStruct), f.Name, ruleName, typeDisplayName(targetType), fieldPath)
	}
}

func checkSingleVarRule(v *validator.Validate, fieldType reflect.Type, rule string, enclosingStruct reflect.Type, f reflect.StructField) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("field %s.%s has invalid validation rule %q for type %s: %v", typeDisplayName(enclosingStruct), f.Name, rule, typeDisplayName(fieldType), r)
		}
	}()

	name, param, _ := strings.Cut(rule, "=")
	name = strings.TrimSpace(name)
	param = strings.TrimSpace(param)
	if name == "unique" {
		if err := checkUniqueRule(fieldType, param, enclosingStruct, f); err != nil {
			return err
		}
	}

	sample := buildSyntheticValue(fieldType, make(map[reflect.Type]int))
	if !sample.IsValid() || (sample.Kind() == reflect.Interface && sample.IsNil()) {
		return fmt.Errorf("field %s.%s validation rule %q cannot be checked on type %s", typeDisplayName(enclosingStruct), f.Name, rule, typeDisplayName(fieldType))
	}

	verr := v.Var(sample.Interface(), rule)
	if verr == nil {
		return nil
	}
	if _, ok := verr.(validator.ValidationErrors); ok {
		return nil
	}
	return fmt.Errorf("field %s.%s has invalid validation rule %q for type %s: %w", typeDisplayName(enclosingStruct), f.Name, rule, typeDisplayName(fieldType), verr)
}

func checkUniqueRule(fieldType reflect.Type, param string, enclosingStruct reflect.Type, f reflect.StructField) error {
	base := unwrapPtrType(fieldType)
	switch base.Kind() {
	case reflect.Slice, reflect.Array:
		if base.Elem().Kind() == reflect.Ptr {
			return fmt.Errorf("field %s.%s validation rule \"unique\" cannot be used on pointer element type %s", typeDisplayName(enclosingStruct), f.Name, typeDisplayName(base.Elem()))
		}
		elem := base.Elem()
		if param == "" {
			if !isStaticHashableType(elem, make(map[reflect.Type]bool)) {
				return fmt.Errorf("field %s.%s validation rule \"unique\" cannot be used on non-comparable or interface element type %s", typeDisplayName(enclosingStruct), f.Name, typeDisplayName(base.Elem()))
			}
			return nil
		}
		if elem.Kind() != reflect.Struct {
			return fmt.Errorf("field %s.%s validation rule \"unique=%s\" requires struct element type, got %s", typeDisplayName(enclosingStruct), f.Name, param, typeDisplayName(base.Elem()))
		}
		sf, ok := elem.FieldByName(param)
		if !ok || !sf.IsExported() {
			return fmt.Errorf("field %s.%s validation rule \"unique=%s\" references unknown or unexported field on %s", typeDisplayName(enclosingStruct), f.Name, param, typeDisplayName(elem))
		}
		if sf.Type.Kind() == reflect.Ptr && sf.Type.Elem().Kind() == reflect.Ptr {
			return fmt.Errorf("field %s.%s validation rule \"unique=%s\" cannot be used on multi-pointer field type %s", typeDisplayName(enclosingStruct), f.Name, param, typeDisplayName(sf.Type))
		}
		sfBase := unwrapPtrType(sf.Type)
		if !isStaticHashableType(sfBase, make(map[reflect.Type]bool)) {
			return fmt.Errorf("field %s.%s validation rule \"unique=%s\" cannot be used on non-comparable or interface field type %s", typeDisplayName(enclosingStruct), f.Name, param, typeDisplayName(sf.Type))
		}
		return nil
	case reflect.Map:
		if base.Elem().Kind() == reflect.Ptr {
			return fmt.Errorf("field %s.%s validation rule \"unique\" cannot be used on pointer map value type %s", typeDisplayName(enclosingStruct), f.Name, typeDisplayName(base.Elem()))
		}
		val := base.Elem()
		if !isStaticHashableType(val, make(map[reflect.Type]bool)) {
			return fmt.Errorf("field %s.%s validation rule \"unique\" cannot be used on non-comparable or interface map value type %s", typeDisplayName(enclosingStruct), f.Name, typeDisplayName(base.Elem()))
		}
		return nil
	}
	return nil
}

func isStaticHashableType(t reflect.Type, visited map[reflect.Type]bool) bool {
	if t == nil || t.Kind() == reflect.Interface || !t.Comparable() {
		return false
	}
	switch t.Kind() {
	case reflect.Array:
		return isStaticHashableType(t.Elem(), visited)
	case reflect.Struct:
		if visited[t] {
			return true
		}
		visited[t] = true
		for i := 0; i < t.NumField(); i++ {
			if !isStaticHashableType(t.Field(i).Type, visited) {
				return false
			}
		}
	}
	return true
}

func checkSingleStructRule(v *validator.Validate, enclosingStruct reflect.Type, targetField reflect.StructField, tagName, singleRule string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("field %s.%s has invalid cross-field validation rule %q: %v", typeDisplayName(enclosingStruct), targetField.Name, singleRule, r)
		}
	}()

	synthType, ok := buildStructOfForSingleFieldRule(enclosingStruct, targetField, tagName, singleRule)
	if !ok {
		return nil
	}

	ptr := reflect.New(synthType)
	ptr.Elem().Set(buildSyntheticValue(synthType, make(map[reflect.Type]int)))

	verr := v.Struct(ptr.Interface())
	if verr == nil {
		return nil
	}
	if _, ok := verr.(validator.ValidationErrors); ok {
		return nil
	}
	return fmt.Errorf("field %s.%s has invalid cross-field validation rule %q: %w", typeDisplayName(enclosingStruct), targetField.Name, singleRule, verr)
}

func buildStructOfForSingleFieldRule(enclosingStruct reflect.Type, targetField reflect.StructField, tagName, singleRule string) (reflect.Type, bool) {
	seen := make(map[string]struct{}, enclosingStruct.NumField())
	fields := make([]reflect.StructField, 0, enclosingStruct.NumField())

	fields = append(fields, reflect.StructField{
		Name: targetField.Name,
		Type: targetField.Type,
		Tag:  reflect.StructTag(tagName + ":" + strconv.Quote(singleRule)),
	})
	seen[targetField.Name] = struct{}{}

	for i := 0; i < enclosingStruct.NumField(); i++ {
		sf := enclosingStruct.Field(i)
		if sf.IsExported() {
			if _, exists := seen[sf.Name]; !exists {
				fields = append(fields, reflect.StructField{
					Name: sf.Name,
					Type: sf.Type,
				})
				seen[sf.Name] = struct{}{}
			}
		}
	}

	for i := 0; i < enclosingStruct.NumField(); i++ {
		sf := enclosingStruct.Field(i)
		if sf.Anonymous {
			collectPromotedExportedFields(sf.Type, seen, &fields, make(map[reflect.Type]bool))
		}
	}

	if len(fields) == 0 {
		return nil, false
	}
	return reflect.StructOf(fields), true
}

func collectPromotedExportedFields(t reflect.Type, seen map[string]struct{}, fields *[]reflect.StructField, visited map[reflect.Type]bool) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || visited[t] {
		return
	}
	visited[t] = true
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.IsExported() {
			if _, exists := seen[sf.Name]; !exists {
				*fields = append(*fields, reflect.StructField{
					Name: sf.Name,
					Type: sf.Type,
				})
				seen[sf.Name] = struct{}{}
			}
			continue
		}
		if sf.Anonymous {
			collectPromotedExportedFields(sf.Type, seen, fields, visited)
		}
	}
}

func resolveStructFieldPathType(t reflect.Type, path string) (reflect.Type, bool) {
	if path == "" || strings.ContainsAny(path, "[]") {
		return nil, false
	}
	cur := t
	for _, seg := range strings.Split(path, ".") {
		if seg == "" {
			return nil, false
		}
		for cur.Kind() == reflect.Ptr {
			cur = cur.Elem()
		}
		if cur.Kind() != reflect.Struct || cur == timeType || cur.ConvertibleTo(timeType) {
			return nil, false
		}
		sf, ok := cur.FieldByName(seg)
		if !ok || !sf.IsExported() {
			return nil, false
		}
		cur = sf.Type
	}
	return cur, true
}

func buildOperationValidators(reqType reflect.Type, hasValidate, hasBinding bool) (*validator.Validate, *validator.Validate, error) {
	var valV *validator.Validate
	var bindV *validator.Validate
	if hasValidate {
		valV = validator.New(validator.WithRequiredStructEnabled())
		valV.SetTagName("validate")
	}
	if hasBinding {
		bindV = validator.New(validator.WithRequiredStructEnabled())
		bindV.SetTagName("binding")
	}

	var lazyValV, lazyBindV *validator.Validate
	getValidator := func(usedBinding bool) *validator.Validate {
		if usedBinding {
			if bindV != nil {
				return bindV
			}
			if lazyBindV == nil {
				lazyBindV = validator.New(validator.WithRequiredStructEnabled())
				lazyBindV.SetTagName("binding")
			}
			return lazyBindV
		}
		if valV != nil {
			return valV
		}
		if lazyValV == nil {
			lazyValV = validator.New(validator.WithRequiredStructEnabled())
			lazyValV.SetTagName("validate")
		}
		return lazyValV
	}

	if err := validateCrossFieldRefsForStruct(reqType, reqType, make(map[reflect.Type]bool), getValidator); err != nil {
		return nil, nil, err
	}

	if !hasValidate && !hasBinding {
		return nil, nil, nil
	}

	sample := buildSyntheticValue(reqType, make(map[reflect.Type]int))

	if valV != nil {
		if err := precheckValidatorInstance(valV, reqType, sample); err != nil {
			return nil, nil, err
		}
	}

	if bindV != nil {
		if err := precheckValidatorInstance(bindV, reqType, sample); err != nil {
			return nil, nil, err
		}
	}

	return valV, bindV, nil
}

func precheckValidatorInstance(v *validator.Validate, reqType reflect.Type, sample reflect.Value) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("invalid validation rule on %s: %v", typeDisplayName(reqType), r)
		}
	}()

	ptr := reflect.New(reqType)
	ptr.Elem().Set(sample)

	verr := v.Struct(ptr.Interface())
	if verr == nil {
		return nil
	}
	if _, ok := verr.(validator.ValidationErrors); ok {
		return nil
	}
	return fmt.Errorf("invalid validation setup on %s: %w", typeDisplayName(reqType), verr)
}

func buildSyntheticValue(t reflect.Type, active map[reflect.Type]int) reflect.Value {
	switch t.Kind() {
	case reflect.Ptr:
		elem := t.Elem()
		if elem.Kind() == reflect.Struct && active[elem] >= 1 {
			return reflect.Zero(t)
		}
		ptr := reflect.New(elem)
		ptr.Elem().Set(buildSyntheticValue(elem, active))
		return ptr
	case reflect.Struct:
		if t == timeType {
			return reflect.ValueOf(time.Unix(1700000000, 0).UTC())
		}
		if active[t] >= 1 {
			return reflect.Zero(t)
		}
		active[t]++
		defer func() { active[t]-- }()

		v := reflect.New(t).Elem()
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			fv := v.Field(i)
			if !fv.CanSet() {
				if sf.Anonymous && sf.Type.Kind() == reflect.Struct && !isSpecialStructType(sf.Type) {
					populateUnexportedEmbeddedStruct(fv, sf.Type, active)
				}
				continue
			}
			fv.Set(buildSyntheticValue(sf.Type, active))
		}
		return v
	case reflect.Slice:
		if t == jsonRawMessageType {
			return reflect.ValueOf(json.RawMessage("{}"))
		}
		s := reflect.MakeSlice(t, 1, 1)
		s.Index(0).Set(buildSyntheticValue(t.Elem(), active))
		return s
	case reflect.Array:
		if t == uuidType {
			u := uuid.MustParse("11111111-1111-4111-8111-111111111111")
			return reflect.ValueOf(u)
		}
		a := reflect.New(t).Elem()
		if a.Len() > 0 {
			a.Index(0).Set(buildSyntheticValue(t.Elem(), active))
		}
		return a
	case reflect.Map:
		m := reflect.MakeMapWithSize(t, 1)
		k := buildSyntheticValue(t.Key(), active)
		val := buildSyntheticValue(t.Elem(), active)
		m.SetMapIndex(k, val)
		return m
	case reflect.String:
		v := reflect.New(t).Elem()
		v.SetString("a")
		return v
	case reflect.Bool:
		v := reflect.New(t).Elem()
		v.SetBool(true)
		return v
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v := reflect.New(t).Elem()
		v.SetInt(1)
		return v
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v := reflect.New(t).Elem()
		v.SetUint(1)
		return v
	case reflect.Float32, reflect.Float64:
		v := reflect.New(t).Elem()
		v.SetFloat(1)
		return v
	case reflect.Interface:
		if t.NumMethod() == 0 {
			return reflect.ValueOf("a")
		}
		return reflect.Zero(t)
	default:
		return reflect.Zero(t)
	}
}

func populateUnexportedEmbeddedStruct(fv reflect.Value, t reflect.Type, active map[reflect.Type]int) {
	if active[t] >= 1 {
		return
	}
	active[t]++
	defer func() { active[t]-- }()

	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		subFV := fv.Field(i)
		if subFV.CanSet() {
			subFV.Set(buildSyntheticValue(sf.Type, active))
			continue
		}
		if sf.Anonymous && sf.Type.Kind() == reflect.Struct && !isSpecialStructType(sf.Type) {
			populateUnexportedEmbeddedStruct(subFV, sf.Type, active)
		}
	}
}

func (op *CompiledOperation) runStructValidation(reqPtr any, customValidator func(any) error) error {
	var violations []fieldViolation

	if op.valValidator != nil {
		if err := op.valValidator.Struct(reqPtr); err != nil {
			ves, ok := err.(validator.ValidationErrors)
			if !ok {
				return newInvalidArgumentError("invalid request parameters", nil)
			}
			for _, fe := range ves {
				violations = append(violations, fieldViolation{
					Field: op.resolveValidatorFieldPath(fe.StructNamespace()),
					Rule:  fe.Tag(),
				})
			}
		}
	}

	if op.bindValidator != nil {
		if err := op.bindValidator.Struct(reqPtr); err != nil {
			ves, ok := err.(validator.ValidationErrors)
			if !ok {
				return newInvalidArgumentError("invalid request parameters", nil)
			}
			for _, fe := range ves {
				violations = append(violations, fieldViolation{
					Field: op.resolveValidatorFieldPath(fe.StructNamespace()),
					Rule:  fe.Tag(),
				})
			}
		}
	}

	if len(violations) > 0 {
		return newInvalidArgumentError("invalid request parameters", violations)
	}

	if customValidator != nil {
		if err := customValidator(reqPtr); err != nil {
			return op.sanitizeCustomValidatorError(err)
		}
	}
	return nil
}

func (op *CompiledOperation) sanitizeCustomValidatorError(err error) error {
	var ve *piHTTP.ValidationError
	if errors.As(err, &ve) && ve != nil {
		var violations []fieldViolation
		for _, fe := range ve.Errors {
			violations = append(violations, fieldViolation{
				Field: op.resolveValidatorFieldPath(fe.StructNamespace()),
				Rule:  fe.Tag(),
			})
		}
		return newInvalidArgumentError("invalid request parameters", violations)
	}
	var ves validator.ValidationErrors
	if errors.As(err, &ves) {
		var violations []fieldViolation
		for _, fe := range ves {
			violations = append(violations, fieldViolation{
				Field: op.resolveValidatorFieldPath(fe.StructNamespace()),
				Rule:  fe.Tag(),
			})
		}
		return newInvalidArgumentError("invalid request parameters", violations)
	}
	return err
}

func (op *CompiledOperation) resolveValidatorFieldPath(structNS string) string {
	rel := structNS
	if dot := findDotOutsideBrackets(structNS); dot >= 0 {
		rel = structNS[dot+1:]
	}
	if rel == "" {
		return ""
	}

	segments := splitNamespaceSegments(rel)
	curType := op.ReqType
	atTopLevel := true
	var chain []int
	outParts := make([]string, 0, len(segments))

	for _, seg := range segments {
		fieldName, bracket := splitFieldAndBracket(seg)
		for curType.Kind() == reflect.Ptr || curType.Kind() == reflect.Slice || curType.Kind() == reflect.Array {
			curType = curType.Elem()
		}
		if curType.Kind() == reflect.Map {
			curType = curType.Elem()
			for curType.Kind() == reflect.Ptr || curType.Kind() == reflect.Slice || curType.Kind() == reflect.Array {
				curType = curType.Elem()
			}
		}
		if curType.Kind() != reflect.Struct {
			outParts = append(outParts, fieldName+bracket)
			continue
		}

		sf, found := findDirectStructField(curType, fieldName)
		if !found {
			outParts = append(outParts, fieldName+bracket)
			continue
		}

		if atTopLevel {
			chain = append(chain, sf.Index[0])
		}

		if isFlattenedAnonymousStructField(sf) {
			curType = sf.Type
			continue
		}

		if atTopLevel {
			if cf, ok := op.fieldByChain[formatIndexChain(chain)]; ok {
				outParts = append(outParts, cf.errField+bracket)
			} else {
				extName, _, _, _ := parseJSONTag(sf)
				if extName == "" {
					extName = sf.Name
				}
				outParts = append(outParts, extName+bracket)
			}
			atTopLevel = false
		} else {
			extName, _, _, _ := parseJSONTag(sf)
			if extName == "" {
				extName = sf.Name
			}
			outParts = append(outParts, extName+bracket)
		}

		curType = sf.Type
		if bracket != "" {
			for curType.Kind() == reflect.Ptr || curType.Kind() == reflect.Slice || curType.Kind() == reflect.Array || curType.Kind() == reflect.Map {
				curType = curType.Elem()
			}
		}
	}

	return strings.Join(outParts, ".")
}

func findDotOutsideBrackets(s string) int {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '[':
			depth++
		case ']':
			if depth > 0 {
				depth--
			}
		case '.':
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func splitNamespaceSegments(s string) []string {
	var parts []string
	start := 0
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '[':
			depth++
		case ']':
			if depth > 0 {
				depth--
			}
		case '.':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func splitFieldAndBracket(seg string) (string, string) {
	if idx := strings.IndexByte(seg, '['); idx >= 0 {
		return seg[:idx], seg[idx:]
	}
	return seg, ""
}

func findDirectStructField(t reflect.Type, name string) (reflect.StructField, bool) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Name == name {
			return f, true
		}
	}
	return reflect.StructField{}, false
}

func formatIndexChain(chain []int) string {
	if len(chain) == 0 {
		return ""
	}
	var b strings.Builder
	for i, idx := range chain {
		if i > 0 {
			b.WriteByte('.')
		}
		b.WriteString(strconv.Itoa(idx))
	}
	return b.String()
}
