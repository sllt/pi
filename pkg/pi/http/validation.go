package http

import (
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/go-playground/locales/en"
	"github.com/go-playground/locales/zh"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	enTrans "github.com/go-playground/validator/v10/translations/en"
	zhTrans "github.com/go-playground/validator/v10/translations/zh"
)

var (
	validate     *validator.Validate
	trans        ut.Translator
	validateOnce sync.Once
)

func initValidator() {
	validate = validator.New(validator.WithRequiredStructEnabled())
	validate.SetTagName("binding")

	// Use "label" tag for field display name, fallback to "json" tag
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		label := fld.Tag.Get("label")
		if label != "" {
			return label
		}

		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "-" || name == "" {
			return fld.Name
		}

		return name
	})

	// Initialize translator based on VALIDATION_LOCALE env
	locale := os.Getenv("VALIDATION_LOCALE")

	switch locale {
	case "zh":
		loc := zh.New()
		uni := ut.New(loc, loc)
		trans, _ = uni.GetTranslator("zh")
		_ = zhTrans.RegisterDefaultTranslations(validate, trans)
	default: // "en" or unset
		loc := en.New()
		uni := ut.New(loc, loc)
		trans, _ = uni.GetTranslator("en")
		_ = enTrans.RegisterDefaultTranslations(validate, trans)
	}
}

func getValidator() *validator.Validate {
	validateOnce.Do(initValidator)

	return validate
}

func getTranslator() ut.Translator {
	validateOnce.Do(initValidator)

	return trans
}

// validateStruct validates the given struct using "binding" tags.
// Priority: msg tag > label + translator > default translator.
func validateStruct(i any) error {
	val := reflect.ValueOf(i)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil
	}

	if !hasValidationTags(val.Type()) {
		return nil
	}

	v := getValidator()

	err := v.Struct(i)
	if err == nil {
		return nil
	}

	if validationErrors, ok := err.(validator.ValidationErrors); ok {
		return &ValidationError{
			Errors:     validationErrors,
			structType: val.Type(),
		}
	}

	return err
}

// hasValidationTags checks if a struct type has any "binding" or "validate" tags.
func hasValidationTags(t reflect.Type) bool {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Tag.Get("binding") != "" || field.Tag.Get("validate") != "" {
			return true
		}

		if field.Type.Kind() == reflect.Struct && field.Anonymous {
			if hasValidationTags(field.Type) {
				return true
			}
		}
	}

	return false
}

// ValidationError wraps validator.ValidationErrors with struct type info for tag resolution.
type ValidationError struct {
	Errors     validator.ValidationErrors
	structType reflect.Type
}

func (e *ValidationError) Error() string {
	t := getTranslator()

	var msgs []string

	for _, fe := range e.Errors {
		msgs = append(msgs, e.resolveMessage(fe, t))
	}

	return strings.Join(msgs, "; ")
}

// StatusCode returns 400 Bad Request for validation errors.
func (e *ValidationError) StatusCode() int {
	return http.StatusBadRequest
}

// resolveMessage resolves the error message for a single field error.
// Priority: msg tag (per-rule > wildcard) > translator.
func (e *ValidationError) resolveMessage(fe validator.FieldError, t ut.Translator) string {
	field := findStructField(e.structType, fe.StructField())
	if field != nil {
		msgTag := field.Tag.Get("msg")
		if msgTag != "" {
			msgMap := parseMsgTag(msgTag)

			// Per-rule message: msg:"required:不能为空;email:格式不正确"
			if msg, ok := msgMap[fe.Tag()]; ok {
				return replaceVars(msg, fe, field)
			}

			// Wildcard message: msg:"请输入正确的邮箱"
			if msg, ok := msgMap["*"]; ok {
				return replaceVars(msg, fe, field)
			}
		}
	}

	// Fallback to translator
	return fe.Translate(t)
}

// parseMsgTag parses the msg tag value.
//
// Formats:
//   - "验证码必须是6位数字"                             → all rules use this message
//   - "required:邮箱不能为空;email:邮箱格式不正确"         → per-rule messages
//   - "min:{label}至少{param}位;max:{label}最多{param}位" → per-rule with variables
func parseMsgTag(msgTag string) map[string]string {
	msgs := make(map[string]string)
	if msgTag == "" {
		return msgs
	}

	parts := strings.Split(msgTag, ";")

	// Single message without colon → wildcard
	if len(parts) == 1 && !strings.Contains(parts[0], ":") {
		msgs["*"] = msgTag
		return msgs
	}

	for _, part := range parts {
		part = strings.TrimSpace(part)
		idx := strings.Index(part, ":")
		if idx > 0 {
			rule := strings.TrimSpace(part[:idx])
			msg := strings.TrimSpace(part[idx+1:])
			msgs[rule] = msg
		} else if part != "" {
			msgs["*"] = part
		}
	}

	return msgs
}

// replaceVars replaces template variables in msg.
//
// Supported variables:
//   - {field} : json tag name
//   - {label} : label tag name (or json tag if no label)
//   - {tag}   : validation rule name (e.g. "required", "email", "min")
//   - {param} : rule parameter (e.g. "6" for min=6)
//   - {value} : current field value
func replaceVars(msg string, fe validator.FieldError, field *reflect.StructField) string {
	jsonName := strings.SplitN(field.Tag.Get("json"), ",", 2)[0]
	if jsonName == "" || jsonName == "-" {
		jsonName = field.Name
	}

	labelName := field.Tag.Get("label")
	if labelName == "" {
		labelName = jsonName
	}

	r := strings.NewReplacer(
		"{field}", jsonName,
		"{label}", labelName,
		"{tag}", fe.Tag(),
		"{param}", fe.Param(),
		"{value}", fmt.Sprintf("%v", fe.Value()),
	)

	return r.Replace(msg)
}

// findStructField finds a struct field by its Go name, including embedded structs.
func findStructField(t reflect.Type, name string) *reflect.StructField {
	if t == nil {
		return nil
	}

	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil
	}

	// Direct field lookup
	if f, ok := t.FieldByName(name); ok {
		return &f
	}

	// Search embedded structs
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			if f := findStructField(field.Type, name); f != nil {
				return f
			}
		}
	}

	return nil
}
