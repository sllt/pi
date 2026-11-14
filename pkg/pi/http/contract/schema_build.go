package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/sllt/pi/pkg/pi/openapi"
)

// SchemaRegistry collects and deduplicates OpenAPI 3.0.3 component schemas
// with deterministic generic component naming and collision disambiguation.
type SchemaRegistry struct {
	schemas   map[string]*openapi.Schema
	byTypeKey map[string]string
	usedNames map[string]string
}

// NewSchemaRegistry creates an empty SchemaRegistry.
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		schemas:   make(map[string]*openapi.Schema),
		byTypeKey: make(map[string]string),
		usedNames: make(map[string]string),
	}
}

// Schemas returns a deep-cloned map of all registered component schemas.
func (sr *SchemaRegistry) Schemas() map[string]*openapi.Schema {
	if sr == nil || len(sr.schemas) == 0 {
		return nil
	}
	out := make(map[string]*openapi.Schema, len(sr.schemas))
	for k, v := range sr.schemas {
		out[k] = v.Clone()
	}
	return out
}

func (op *CompiledOperation) validateAndRegisterSchemas(sr *SchemaRegistry) error {
	if op.HasBodyFields {
		if _, err := registerOrBuildBodySchema(op.ReqType, op.bodyPlan, op.StrictJSON, sr); err != nil {
			return err
		}
	}
	if !op.EmptyResponse {
		if _, err := registerOrBuildResponseSchema(op.ResType, sr); err != nil {
			return err
		}
	}
	return nil
}

// BuildOpenAPIOperation builds an OpenAPI 3.0.3 Operation from the compiled
// contract, registering referenced DTO schemas into sr.
func (op *CompiledOperation) BuildOpenAPIOperation(sr *SchemaRegistry) *openapi.Operation {
	if op == nil {
		return nil
	}
	if sr == nil {
		sr = NewSchemaRegistry()
	}

	out := &openapi.Operation{
		Summary:     op.Summary,
		Description: op.Description,
		OperationID: op.OperationID,
		Deprecated:  op.Deprecated,
		Responses:   make(map[string]*openapi.Response),
	}
	if len(op.Tags) > 0 {
		out.Tags = append([]string(nil), op.Tags...)
	}
	if op.Security != nil {
		clonedSec := make([]openapi.SecurityRequirement, len(*op.Security))
		for i, reqMap := range *op.Security {
			copiedMap := make(openapi.SecurityRequirement, len(reqMap))
			for k, scopes := range reqMap {
				if scopes == nil {
					copiedMap[k] = []string{}
				} else {
					copiedMap[k] = append([]string{}, scopes...)
				}
			}
			clonedSec[i] = copiedMap
		}
		out.Security = &clonedSec
	}

	out.Parameters = op.buildOpenAPIParameters()

	if op.HasBodyFields {
		bodySchema, _ := registerOrBuildBodySchema(op.ReqType, op.bodyPlan, op.StrictJSON, sr)
		bodyRequired := false
		for _, cf := range op.bodyFields {
			if cf.required {
				bodyRequired = true
				break
			}
		}
		out.RequestBody = &openapi.RequestBody{
			Required: bodyRequired,
			Content: map[string]*openapi.MediaType{
				"application/json": {
					Schema: bodySchema,
				},
			},
		}
	}

	statusKey := strconv.Itoa(op.Status)
	if op.Status == http.StatusNoContent {
		out.Responses[statusKey] = &openapi.Response{
			Description: statusDescription(http.StatusNoContent),
		}
	} else {
		var dataSchema *openapi.Schema
		if op.EmptyResponse {
			dataSchema = &openapi.Schema{
				Type:     "object",
				Nullable: true,
			}
		} else {
			resSchema, _ := registerOrBuildResponseSchema(op.ResType, sr)
			if op.NullableRes {
				if resSchema.Ref != "" {
					dataSchema = &openapi.Schema{
						Nullable: true,
						AllOf:    []*openapi.Schema{{Ref: resSchema.Ref}},
					}
				} else if resSchema.Type != "" || len(resSchema.AllOf) > 0 {
					resSchema.Nullable = true
					dataSchema = resSchema
				} else {
					dataSchema = resSchema
				}
			} else {
				dataSchema = resSchema
			}
		}
		out.Responses[statusKey] = &openapi.Response{
			Description: statusDescription(op.Status),
			Content: map[string]*openapi.MediaType{
				"application/json": {
					Schema: buildSuccessEnvelopeSchema(dataSchema),
				},
			},
		}
	}

	errCodes := op.collectErrorStatusCodes()
	for _, code := range errCodes {
		key := strconv.Itoa(code)
		if _, exists := out.Responses[key]; exists {
			continue
		}
		out.Responses[key] = &openapi.Response{
			Description: statusDescription(code),
			Content: map[string]*openapi.MediaType{
				"application/json": {
					Schema: buildErrorEnvelopeSchema(),
				},
			},
		}
	}

	return out
}

func (op *CompiledOperation) collectErrorStatusCodes() []int {
	seen := make(map[int]struct{}, 8+len(op.ErrorStatuses))
	codes := []int{
		http.StatusBadRequest,
		http.StatusRequestTimeout,
		http.StatusRequestEntityTooLarge,
		http.StatusInternalServerError,
	}
	if op.HasBodyFields || op.Method == http.MethodPost || op.Method == http.MethodPut || op.Method == http.MethodPatch {
		codes = append(codes, http.StatusUnsupportedMediaType)
	}
	for _, c := range codes {
		seen[c] = struct{}{}
	}
	for _, c := range op.ErrorStatuses {
		if _, ok := seen[c]; !ok {
			seen[c] = struct{}{}
			codes = append(codes, c)
		}
	}
	sort.Ints(codes)
	return codes
}

func (op *CompiledOperation) buildOpenAPIParameters() []*openapi.Parameter {
	total := len(op.pathFields) + len(op.queryFields) + len(op.headerFields)
	if total == 0 {
		return nil
	}
	params := make([]*openapi.Parameter, 0, total)

	if len(op.pathFields) > 0 {
		pathByName := make(map[string]*compiledField, len(op.pathFields))
		for _, cf := range op.pathFields {
			pathByName[cf.externalName] = cf
		}
		for _, pName := range op.pathParamsOrder {
			if cf, ok := pathByName[pName]; ok {
				params = append(params, buildCompiledFieldParameter(cf))
			}
		}
	}

	for _, cf := range op.queryFields {
		params = append(params, buildCompiledFieldParameter(cf))
	}
	for _, cf := range op.headerFields {
		params = append(params, buildCompiledFieldParameter(cf))
	}
	return params
}

func buildCompiledFieldParameter(cf *compiledField) *openapi.Parameter {
	desc := cf.description
	if desc == "" {
		desc = cf.label
	}

	var schema *openapi.Schema
	if cf.isSlice {
		itemSchema := schemaForScalar(cf.scalar, false)
		schema = &openapi.Schema{
			Type:  "array",
			Items: itemSchema,
		}
	} else {
		schema = schemaForScalar(cf.scalar, false)
	}
	if cf.hasDefault {
		schema.Default = cf.schemaDefault
	}
	var paramSpec *jsonTypeSpec
	if cf.isSlice {
		elemScalar := cf.scalar
		elemScalar.isPtr = false
		paramSpec = &jsonTypeSpec{
			kind: jsonSlice,
			elemSpec: &jsonTypeSpec{
				kind:   jsonScalar,
				scalar: elemScalar,
			},
		}
	} else {
		ptrDepth := 0
		if cf.scalar.isPtr {
			ptrDepth = 1
		}
		paramSpec = &jsonTypeSpec{
			kind:     jsonScalar,
			ptrDepth: ptrDepth,
			scalar:   cf.scalar,
		}
	}
	applyValidatorConstraintsWithSpec(cf.validateTag, schema, paramSpec, false, false)

	param := &openapi.Parameter{
		Name:        cf.externalName,
		In:          cf.source.prefix(),
		Description: desc,
		Required:    cf.required,
		Schema:      schema,
	}
	if cf.hasExample {
		param.Example = cf.schemaExample
	}
	return param
}

func registerOrBuildBodySchema(reqType reflect.Type, bodyPlan *jsonStructPlan, strict bool, sr *SchemaRegistry) (*openapi.Schema, error) {
	if reqType.Name() == "" {
		return buildStructObjectSchema(bodyPlan, true, strict, sr)
	}
	mode := "in:lax"
	if strict {
		mode = "in:strict"
	}
	typeKey := mode + ":" + canonicalTypeKey(reqType)
	if compName, ok := sr.byTypeKey[typeKey]; ok {
		return &openapi.Schema{Ref: "#/components/schemas/" + compName}, nil
	}

	compName := sr.allocateComponentName(reqType, typeKey)
	sr.byTypeKey[typeKey] = compName
	sr.usedNames[compName] = typeKey
	sr.schemas[compName] = &openapi.Schema{Type: "object"}

	objSchema, err := buildStructObjectSchema(bodyPlan, true, strict, sr)
	if err != nil {
		delete(sr.byTypeKey, typeKey)
		delete(sr.usedNames, compName)
		delete(sr.schemas, compName)
		return nil, err
	}
	sr.schemas[compName] = objSchema
	return &openapi.Schema{Ref: "#/components/schemas/" + compName}, nil
}

func registerOrBuildResponseSchema(resType reflect.Type, sr *SchemaRegistry) (*openapi.Schema, error) {
	plan, err := buildResponseStructPlan(resType)
	if err != nil {
		return nil, err
	}
	if resType.Name() == "" {
		return buildStructObjectSchema(plan, false, false, sr)
	}
	typeKey := "out:" + canonicalTypeKey(resType)
	if compName, ok := sr.byTypeKey[typeKey]; ok {
		return &openapi.Schema{Ref: "#/components/schemas/" + compName}, nil
	}

	compName := sr.allocateComponentName(resType, typeKey)
	sr.byTypeKey[typeKey] = compName
	sr.usedNames[compName] = typeKey
	sr.schemas[compName] = &openapi.Schema{Type: "object"}

	objSchema, err := buildStructObjectSchema(plan, false, false, sr)
	if err != nil {
		delete(sr.byTypeKey, typeKey)
		delete(sr.usedNames, compName)
		delete(sr.schemas, compName)
		return nil, err
	}
	sr.schemas[compName] = objSchema
	return &openapi.Schema{Ref: "#/components/schemas/" + compName}, nil
}

func buildResponseStructPlan(t reflect.Type) (*jsonStructPlan, error) {
	rc := &reqCollector{
		reqType:    t,
		structMemo: make(map[reflect.Type]*jsonStructPlan),
	}
	sp := &jsonStructPlan{
		goType:     t,
		byJSONName: make(map[string]*jsonFieldPlan),
	}
	rc.structMemo[t] = sp
	if err := collectResponseFieldsForSchema(t, sp, rc); err != nil {
		return nil, err
	}
	return sp, nil
}

func collectResponseFieldsForSchema(t reflect.Type, sp *jsonStructPlan, rc *reqCollector) error {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			if isFlattenedAnonymousStructField(f) {
				if err := collectResponseFieldsForSchema(f.Type, sp, rc); err != nil {
					return err
				}
			}
			continue
		}
		if isFlattenedAnonymousStructField(f) {
			if err := collectResponseFieldsForSchema(f.Type, sp, rc); err != nil {
				return err
			}
			continue
		}
		jsonName, skipJSON, omitempty, isJSONString := parseJSONTag(f)
		if skipJSON {
			continue
		}
		if jsonName == "" {
			jsonName = f.Name
		}
		spec, err := rc.buildJSONTypeSpec(f.Type, isJSONString, false, rc.structMemo)
		if err != nil {
			return err
		}
		desc := strings.TrimSpace(f.Tag.Get("dc"))
		if desc == "" {
			desc = strings.TrimSpace(f.Tag.Get("desc"))
		}
		if desc == "" {
			desc = strings.TrimSpace(f.Tag.Get("description"))
		}
		var schemaEx any
		var hasEx bool
		if exRaw, ok := f.Tag.Lookup("example"); ok {
			var scalar compiledScalarType
			if spec.ptrDepth <= 1 && spec.kind == jsonScalar {
				scalar = spec.scalar
			}
			parsedEx, err := parseAndValidateExample(exRaw, f.Type, scalar, false, isJSONString)
			if err != nil {
				return err
			}
			schemaEx = parsedEx
			hasEx = true
		}

		jfp := &jsonFieldPlan{
			goName:       f.Name,
			fieldType:    f.Type,
			jsonName:     jsonName,
			omitempty:    omitempty,
			isJSONString: isJSONString,
			description:  desc,
			label:        strings.TrimSpace(f.Tag.Get("label")),
			hasExample:   hasEx,
			schemaEx:     schemaEx,
			typeSpec:     spec,
		}
		sp.fields = append(sp.fields, jfp)
		sp.byJSONName[jsonName] = jfp
	}
	return nil
}

func buildStructObjectSchema(plan *jsonStructPlan, isInput bool, strict bool, sr *SchemaRegistry) (*openapi.Schema, error) {
	obj := &openapi.Schema{
		Type:       "object",
		Properties: make(map[string]*openapi.Schema, len(plan.fields)),
	}
	if isInput && strict {
		obj.AdditionalProperties = openapi.AdditionalPropertiesAllowed(false)
	}

	required := make([]string, 0, len(plan.fields))
	for _, fp := range plan.fields {
		propSchema, err := buildFieldPropertySchema(fp, isInput, strict, sr)
		if err != nil {
			return nil, err
		}
		obj.Properties[fp.jsonName] = propSchema

		if isInput {
			if fp.required {
				required = append(required, fp.jsonName)
			}
		} else {
			if !fp.omitempty {
				required = append(required, fp.jsonName)
			}
		}
	}
	if len(required) > 0 {
		obj.Required = required
	}
	return obj, nil
}

func buildFieldPropertySchema(fp *jsonFieldPlan, isInput bool, strict bool, sr *SchemaRegistry) (*openapi.Schema, error) {
	desc := fp.description
	if desc == "" {
		desc = fp.label
	}

	s, err := buildSchemaFromTypeSpec(fp.typeSpec, isInput, strict, sr)
	if err != nil {
		return nil, err
	}
	if isInput && fp.validateTag != "" {
		applyValidatorConstraintsWithSpec(fp.validateTag, s, fp.typeSpec, fp.isJSONString, true)
	}

	isRef := s.Ref != ""
	if isRef {
		needsWrapper := desc != "" || fp.hasDefault || fp.hasExample || s.Nullable
		if !needsWrapper {
			return s, nil
		}
		wrapper := &openapi.Schema{
			Description: desc,
			Nullable:    s.Nullable,
			AllOf:       []*openapi.Schema{{Ref: s.Ref}},
		}
		if fp.hasDefault {
			wrapper.Default = fp.schemaDef
		}
		if fp.hasExample {
			wrapper.Example = fp.schemaEx
		}
		return wrapper, nil
	}

	if desc != "" && s.Description == "" {
		s.Description = desc
	}
	if fp.hasDefault {
		s.Default = fp.schemaDef
	}
	if fp.hasExample {
		s.Example = fp.schemaEx
	}
	return s, nil
}

func buildSchemaFromTypeSpec(spec *jsonTypeSpec, isInput bool, strict bool, sr *SchemaRegistry) (*openapi.Schema, error) {
	nullable := spec.ptrDepth > 0 || (!isInput && (spec.kind == jsonSlice || spec.kind == jsonBytes || spec.kind == jsonMap))

	switch spec.kind {
	case jsonScalar:
		s := schemaForScalar(spec.scalar, spec.isJSONString)
		s.Nullable = nullable
		return s, nil

	case jsonBytes:
		return &openapi.Schema{
			Type:     "string",
			Format:   "byte",
			Nullable: nullable,
		}, nil

	case jsonRawMessage, jsonAny:
		return &openapi.Schema{}, nil

	case jsonSlice:
		elemSchema, err := buildSchemaFromTypeSpec(spec.elemSpec, isInput, strict, sr)
		if err != nil {
			return nil, err
		}
		return &openapi.Schema{
			Type:     "array",
			Nullable: nullable,
			Items:    elemSchema,
		}, nil

	case jsonArray:
		elemSchema, err := buildSchemaFromTypeSpec(spec.elemSpec, isInput, strict, sr)
		if err != nil {
			return nil, err
		}
		n := spec.arrayLen
		return &openapi.Schema{
			Type:     "array",
			Nullable: nullable,
			Items:    elemSchema,
			MinItems: &n,
			MaxItems: &n,
		}, nil

	case jsonMap:
		valSchema, err := buildSchemaFromTypeSpec(spec.elemSpec, isInput, strict, sr)
		if err != nil {
			return nil, err
		}
		return &openapi.Schema{
			Type:                 "object",
			Nullable:             nullable,
			AdditionalProperties: openapi.AdditionalPropertiesSchema(valSchema),
		}, nil

	case jsonStruct:
		if spec.elemType.Name() == "" {
			obj, err := buildStructObjectSchema(spec.structPlan, isInput, strict, sr)
			if err != nil {
				return nil, err
			}
			obj.Nullable = nullable
			return obj, nil
		}

		mode := "out"
		if isInput {
			if strict {
				mode = "in:strict"
			} else {
				mode = "in:lax"
			}
		}
		typeKey := mode + ":" + canonicalTypeKey(spec.elemType)
		compName, ok := sr.byTypeKey[typeKey]
		if !ok {
			compName = sr.allocateComponentName(spec.elemType, typeKey)
			sr.byTypeKey[typeKey] = compName
			sr.usedNames[compName] = typeKey
			sr.schemas[compName] = &openapi.Schema{Type: "object"}

			obj, err := buildStructObjectSchema(spec.structPlan, isInput, strict, sr)
			if err != nil {
				delete(sr.byTypeKey, typeKey)
				delete(sr.usedNames, compName)
				delete(sr.schemas, compName)
				return nil, err
			}
			sr.schemas[compName] = obj
		}

		ref := "#/components/schemas/" + compName
		if nullable {
			return &openapi.Schema{
				Nullable: true,
				AllOf:    []*openapi.Schema{{Ref: ref}},
			}, nil
		}
		return &openapi.Schema{Ref: ref}, nil

	default:
		return &openapi.Schema{}, nil
	}
}

func schemaForScalar(sc compiledScalarType, asJSONString bool) *openapi.Schema {
	if asJSONString {
		s := &openapi.Schema{Type: "string"}
		switch sc.kind {
		case scalarInt:
			if sc.bitSize <= 32 {
				s.Format = "int32"
			} else {
				s.Format = "int64"
			}
		}
		return s
	}

	switch sc.kind {
	case scalarString:
		return &openapi.Schema{Type: "string"}
	case scalarBool:
		return &openapi.Schema{Type: "boolean"}
	case scalarTime:
		return &openapi.Schema{Type: "string", Format: "date-time"}
	case scalarUUID:
		return &openapi.Schema{Type: "string", Format: "uuid"}
	case scalarFloat:
		if sc.bitSize == 32 {
			return &openapi.Schema{Type: "number", Format: "float"}
		}
		return &openapi.Schema{Type: "number", Format: "double"}
	case scalarInt:
		s := &openapi.Schema{Type: "integer"}
		switch sc.bitSize {
		case 8:
			s.Format = "int32"
			s.Minimum = float64Ptr(-128)
			s.Maximum = float64Ptr(127)
		case 16:
			s.Format = "int32"
			s.Minimum = float64Ptr(-32768)
			s.Maximum = float64Ptr(32767)
		case 32:
			s.Format = "int32"
		default:
			s.Format = "int64"
		}
		return s
	case scalarUint:
		s := &openapi.Schema{
			Type:    "integer",
			Minimum: float64Ptr(0),
		}
		switch sc.bitSize {
		case 8:
			s.Format = "int32"
			s.Maximum = float64Ptr(255)
		case 16:
			s.Format = "int32"
			s.Maximum = float64Ptr(65535)
		case 32:
			s.Format = "int64"
			s.Maximum = float64Ptr(4294967295)
		}
		return s
	default:
		return &openapi.Schema{Type: "string"}
	}
}

func clearSchemaNullable(s *openapi.Schema) {
	if s == nil {
		return
	}
	s.Nullable = false
	if s.Ref == "" && len(s.AllOf) == 1 && s.AllOf[0] != nil && s.AllOf[0].Ref != "" &&
		s.Description == "" && s.Default == nil && s.Example == nil {
		s.Ref = s.AllOf[0].Ref
		s.AllOf = nil
	}
}

func isNilSafeConditionalRule(name string) bool {
	if _, ok := firstTokenCrossFieldTags[name]; ok {
		return true
	}
	_, ok := allTokensCrossFieldTags[name]
	return ok
}

func applyValidatorConstraintsWithSpec(tagExpr string, s *openapi.Schema, spec *jsonTypeSpec, asJSONString bool, isBodySchema bool) {
	if tagExpr == "" || s == nil {
		return
	}
	target := s
	curSpec := spec
	inDive := false
	inMapKeys := false
	sawOmitEmpty := false
	sawOmitNil := false
	sawOmitZero := false

	for _, rawRule := range splitValidatorRules(tagExpr) {
		rule := strings.TrimSpace(rawRule)
		if rule == "" {
			continue
		}
		if inMapKeys {
			if rule == "endkeys" {
				inMapKeys = false
				sawOmitEmpty = false
				sawOmitNil = false
				sawOmitZero = false
			}
			continue
		}

		branches := splitValidatorBranches(rule)
		if len(branches) > 1 {
			if isBodySchema && curSpec != nil && curSpec.ptrDepth > 0 && !sawOmitEmpty && !sawOmitNil && !sawOmitZero {
				firstName, _, _ := strings.Cut(strings.TrimSpace(branches[0]), "=")
				if !isNilSafeConditionalRule(strings.TrimSpace(firstName)) {
					clearSchemaNullable(target)
				}
			}
			continue
		}

		name, param, hasEq := strings.Cut(rule, "=")
		name = strings.TrimSpace(name)
		param = strings.TrimSpace(param)

		switch name {
		case "omitempty":
			sawOmitEmpty = true
			continue
		case "omitnil":
			sawOmitNil = true
			continue
		case "omitzero":
			sawOmitZero = true
			continue
		case "isdefault":
			// isdefault passes nil pointers in validator/v10 while rejecting non-zero values.
			continue
		case "dive":
			if isBodySchema && curSpec != nil && curSpec.ptrDepth > 0 && !sawOmitEmpty && !sawOmitNil && !sawOmitZero {
				clearSchemaNullable(target)
			}
			if curSpec == nil {
				return
			}
			var nextTarget *openapi.Schema
			switch curSpec.kind {
			case jsonSlice, jsonArray:
				if target.Type == "array" && target.Items != nil {
					nextTarget = target.Items
				}
			case jsonMap:
				if target.Type == "object" && target.AdditionalProperties != nil && target.AdditionalProperties.Schema != nil {
					nextTarget = target.AdditionalProperties.Schema
				}
			}
			if nextTarget == nil || curSpec.elemSpec == nil {
				return
			}
			target = nextTarget
			curSpec = curSpec.elemSpec
			inDive = true
			sawOmitEmpty = false
			sawOmitNil = false
			sawOmitZero = false
			continue
		case "keys":
			if inDive {
				inMapKeys = true
			}
			continue
		case "endkeys":
			sawOmitEmpty = false
			sawOmitNil = false
			sawOmitZero = false
			continue
		}

		isPtr := curSpec != nil && curSpec.ptrDepth > 0
		if isBodySchema && isPtr && !sawOmitEmpty && !sawOmitNil && !sawOmitZero && !isNilSafeConditionalRule(name) {
			clearSchemaNullable(target)
		}

		if target.Ref != "" || len(target.AllOf) > 0 {
			continue
		}

		skipValueConstraints := sawOmitZero || (sawOmitEmpty && curSpec != nil && curSpec.ptrDepth == 0 && (curSpec.kind == jsonScalar || curSpec.kind == jsonArray))
		if skipValueConstraints {
			continue
		}

		var scalar compiledScalarType
		elemJSONString := asJSONString
		if curSpec != nil {
			elemJSONString = elemJSONString || curSpec.isJSONString
			if curSpec.kind == jsonScalar {
				scalar = curSpec.scalar
			}
		}
		applySingleConstraint(name, param, hasEq, target, scalar, elemJSONString, inDive, isPtr)
	}
}

func applySingleConstraint(name, param string, hasEq bool, s *openapi.Schema, scalar compiledScalarType, asJSONString bool, inDive bool, isPtr bool) {
	switch name {
	case "required":
		if inDive && !isPtr && s.Type == "string" && !asJSONString && s.Format != "byte" {
			if s.MinLength == nil || *s.MinLength < 1 {
				s.MinLength = intPtr(1)
			}
		}
	case "email":
		if s.Type == "string" {
			s.Format = "email"
		}
	case "uuid", "uuid3", "uuid4", "uuid5":
		if s.Type == "string" {
			s.Format = "uuid"
		}
	case "url", "uri", "http_url":
		if s.Type == "string" {
			s.Format = "uri"
		}
	case "min", "gte":
		if !hasEq {
			return
		}
		applyBoundMin(s, param, asJSONString)
	case "max", "lte":
		if !hasEq {
			return
		}
		applyBoundMax(s, param, asJSONString)
	case "len":
		if !hasEq {
			return
		}
		applyBoundMin(s, param, asJSONString)
		applyBoundMax(s, param, asJSONString)
	case "oneof":
		if !hasEq {
			return
		}
		tokens := parseOneOfTokens(param)
		if len(tokens) == 0 {
			return
		}
		enumVals := convertEnumTokens(tokens, s.Type, scalar, asJSONString)
		if len(enumVals) > 0 {
			s.Enum = enumVals
		}
	}
}

func applyBoundMin(s *openapi.Schema, param string, asJSONString bool) {
	switch s.Type {
	case "string":
		if asJSONString {
			return
		}
		if n, err := strconv.Atoi(param); err == nil && n >= 0 {
			if s.MinLength == nil || n > *s.MinLength {
				s.MinLength = intPtr(n)
			}
		}
	case "array":
		if n, err := strconv.Atoi(param); err == nil && n >= 0 {
			if s.MinItems == nil || n > *s.MinItems {
				s.MinItems = intPtr(n)
			}
		}
	case "integer", "number":
		if f, err := strconv.ParseFloat(param, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			if s.Minimum == nil || f > *s.Minimum {
				s.Minimum = float64Ptr(f)
			}
		}
	}
}

func applyBoundMax(s *openapi.Schema, param string, asJSONString bool) {
	switch s.Type {
	case "string":
		if asJSONString {
			return
		}
		if n, err := strconv.Atoi(param); err == nil && n >= 0 {
			if s.MaxLength == nil || n < *s.MaxLength {
				s.MaxLength = intPtr(n)
			}
		}
	case "array":
		if n, err := strconv.Atoi(param); err == nil && n >= 0 {
			if s.MaxItems == nil || n < *s.MaxItems {
				s.MaxItems = intPtr(n)
			}
		}
	case "integer", "number":
		if f, err := strconv.ParseFloat(param, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			if s.Maximum == nil || f < *s.Maximum {
				s.Maximum = float64Ptr(f)
			}
		}
	}
}

func parseOneOfTokens(param string) []string {
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
	if hasToken {
		tokens = append(tokens, cur.String())
	}
	return tokens
}

func convertEnumTokens(tokens []string, schemaType string, scalar compiledScalarType, asJSONString bool) []any {
	if asJSONString || schemaType == "string" {
		out := make([]any, 0, len(tokens))
		for _, tok := range tokens {
			out = append(out, tok)
		}
		return out
	}
	if schemaType == "integer" {
		out := make([]any, 0, len(tokens))
		for _, tok := range tokens {
			if scalar.kind == scalarUint {
				u, err := strconv.ParseUint(tok, 10, 64)
				if err != nil {
					return nil
				}
				out = append(out, u)
			} else {
				n, err := strconv.ParseInt(tok, 10, 64)
				if err != nil {
					return nil
				}
				out = append(out, n)
			}
		}
		return out
	}
	if schemaType == "number" {
		out := make([]any, 0, len(tokens))
		for _, tok := range tokens {
			f, err := strconv.ParseFloat(tok, 64)
			if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
				return nil
			}
			out = append(out, f)
		}
		return out
	}
	return nil
}

func canonicalTypeKey(t reflect.Type) string {
	if t == nil {
		return ""
	}
	return t.PkgPath() + "|" + t.Name() + "|" + t.String()
}

func (sr *SchemaRegistry) allocateComponentName(t reflect.Type, typeKey string) string {
	base := formatTypeComponentName(t)
	if existingKey, used := sr.usedNames[base]; !used || existingKey == typeKey {
		return base
	}
	hash := shortTypeHash(typeKey)
	candidate := base + "_" + hash
	for i := 2; ; i++ {
		if existingKey, used := sr.usedNames[candidate]; !used || existingKey == typeKey {
			return candidate
		}
		candidate = base + "_" + hash + "_" + strconv.Itoa(i)
	}
}

func shortTypeHash(typeKey string) string {
	sum := sha256.Sum256([]byte(typeKey))
	return hex.EncodeToString(sum[:4])
}

func formatTypeComponentName(t reflect.Type) string {
	if t == nil || t.Name() == "" {
		return "Schema"
	}
	formatted := formatGoTypeNameExpr(t.Name())
	sanitized := sanitizeOpenAPIComponentName(formatted)
	if sanitized == "" {
		return "Schema"
	}
	return sanitized
}

func formatGoTypeNameExpr(expr string) string {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "Schema"
	}
	if strings.HasPrefix(expr, "*") {
		return "Ptr_" + formatGoTypeNameExpr(expr[1:])
	}
	if strings.HasPrefix(expr, "[]") {
		return "List_" + formatGoTypeNameExpr(expr[2:])
	}
	if strings.HasPrefix(expr, "map[") {
		end := findMatchingBracket(expr, 3)
		if end > 3 && end+1 < len(expr) {
			return "Map_" + formatGoTypeNameExpr(expr[4:end]) + "_" + formatGoTypeNameExpr(expr[end+1:])
		}
	}
	if strings.HasPrefix(expr, "[") {
		end := findMatchingBracket(expr, 0)
		if end > 0 && end+1 < len(expr) {
			return "Arr" + expr[1:end] + "_" + formatGoTypeNameExpr(expr[end+1:])
		}
	}
	if strings.HasPrefix(expr, "struct {") || strings.HasPrefix(expr, "struct{") {
		return "Struct"
	}
	if expr == "interface {}" || expr == "interface{}" || expr == "any" {
		return "Any"
	}

	if openIdx := strings.IndexByte(expr, '['); openIdx >= 0 && strings.HasSuffix(expr, "]") {
		basePart := stripPkgPrefix(expr[:openIdx])
		argsRaw := expr[openIdx+1 : len(expr)-1]
		args := splitGenericTypeArgs(argsRaw)
		parts := make([]string, 0, 1+len(args))
		parts = append(parts, basePart)
		for _, arg := range args {
			parts = append(parts, formatGoTypeNameExpr(arg))
		}
		return strings.Join(parts, "_")
	}

	return stripPkgPrefix(expr)
}

func findMatchingBracket(s string, openPos int) int {
	depth := 0
	for i := openPos; i < len(s); i++ {
		switch s[i] {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func splitGenericTypeArgs(argsRaw string) []string {
	var args []string
	start := 0
	bracketDepth := 0
	braceDepth := 0
	inQuote := false
	escaped := false

	for i := 0; i < len(argsRaw); i++ {
		c := argsRaw[i]
		if inQuote {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == '"' {
				inQuote = false
			}
			continue
		}
		switch c {
		case '"':
			inQuote = true
		case '[':
			bracketDepth++
		case ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
		case '{':
			braceDepth++
		case '}':
			if braceDepth > 0 {
				braceDepth--
			}
		case ',':
			if bracketDepth == 0 && braceDepth == 0 {
				args = append(args, strings.TrimSpace(argsRaw[start:i]))
				start = i + 1
			}
		}
	}
	if start <= len(argsRaw) {
		args = append(args, strings.TrimSpace(argsRaw[start:]))
	}
	return args
}

func stripPkgPrefix(s string) string {
	s = strings.TrimSpace(s)
	if dot := strings.LastIndexByte(s, '.'); dot >= 0 {
		s = s[dot+1:]
	}
	if slash := strings.LastIndexByte(s, '/'); slash >= 0 {
		s = s[slash+1:]
	}
	return s
}

func sanitizeOpenAPIComponentName(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-' {
			b.WriteByte(c)
		} else {
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

func buildSuccessEnvelopeSchema(dataSchema *openapi.Schema) *openapi.Schema {
	return &openapi.Schema{
		Type:     "object",
		Required: []string{"code", "data", "message"},
		Properties: map[string]*openapi.Schema{
			"code":    {Type: "integer", Format: "int64"},
			"data":    dataSchema,
			"message": {Type: "string"},
			"meta": {
				Type:                 "object",
				AdditionalProperties: openapi.AdditionalPropertiesAllowed(true),
			},
		},
	}
}

func buildErrorEnvelopeSchema() *openapi.Schema {
	return &openapi.Schema{
		Type:     "object",
		Required: []string{"code", "data", "message"},
		Properties: map[string]*openapi.Schema{
			"code":    {Type: "integer", Format: "int64"},
			"data":    {Type: "object", Nullable: true},
			"message": {Type: "string"},
			"details": {
				Type: "array",
				Items: &openapi.Schema{
					Type:     "object",
					Required: []string{"field", "rule"},
					Properties: map[string]*openapi.Schema{
						"field": {Type: "string"},
						"rule":  {Type: "string"},
					},
				},
			},
		},
	}
}

func statusDescription(code int) string {
	if text := http.StatusText(code); text != "" {
		return text
	}
	return "Error"
}

func intPtr(v int) *int {
	return &v
}

func float64Ptr(v float64) *float64 {
	return &v
}
