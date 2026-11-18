package contract

import (
	"encoding"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"mime/multipart"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"

	"github.com/sllt/pi/pkg/pi/openapi"
)

const (
	defaultMaxBodyBytes     int64 = 1 << 20  // 1 MiB
	defaultFileMaxBodyBytes int64 = 32 << 20 // 32 MiB
	maxAllowedBodyBytes     int64 = 1 << 30  // 1 GiB
)

var (
	timeType                   = reflect.TypeOf(time.Time{})
	uuidType                   = reflect.TypeOf(uuid.UUID{})
	jsonRawMessageType         = reflect.TypeOf(json.RawMessage(nil))
	multipartFileHeaderType    = reflect.TypeOf(multipart.FileHeader{})
	multipartFileHeaderPtrType = reflect.TypeOf((*multipart.FileHeader)(nil))
	jsonMarshalerType          = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	jsonUnmarshalerType        = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
	textMarshalerType          = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	textUnmarshalerType        = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
)

var reservedHeaderNames = map[string]struct{}{
	"authorization": {},
	"accept":        {},
	"content-type":  {},
}

var unexportedForbiddenTags = []string{
	"path",
	"query",
	"header",
	"form",
	"file",
	"default",
	"validate",
	"binding",
	"dc",
	"desc",
	"example",
}

// ResponseKind identifies the wire response format produced by a typed operation.
type ResponseKind uint8

const (
	ResponseKindJSON ResponseKind = iota
	ResponseKindFile
	ResponseKindXML
	ResponseKindStream
	ResponseKindSSE
)

// CompileOptions supplies group/endpoint inheritance and strict JSON configuration
// when compiling a typed operation contract.
type CompileOptions struct {
	GroupPrefix             string
	HasExplicitRoute        bool
	ExplicitMethod          string
	ExplicitPattern         string
	EndpointMaxBodyBytes    int64
	ExplicitAppMaxBodyBytes int64
	DefaultStrictJSON       bool
	StrictJSONOverride      *bool
	InheritedTags           []string
	HasInheritedSec         bool
	InheritedSecurity       []string // nil if unset; empty non-nil slice []string{} if DocNoSecurity; scheme names if DocSecurity
	HasEndpointTags         bool
	EndpointTags            []string
	HasEndpointSec          bool
	EndpointSecurity        []string // nil if unset; empty non-nil slice []string{} if DocNoSecurity; scheme names if DocSecurity
}

// CompiledOperation holds the immutable registration-time contract, FieldPlan,
// validators, and OpenAPI metadata for a single typed HTTP handler.
type CompiledOperation struct {
	HandlerName          string
	Method               string
	MetaPath             string
	ChiPattern           string
	CanonicalPath        string
	ConflictShape        string
	OperationID          string
	Status               int
	Summary              string
	Description          string
	Tags                 []string
	Deprecated           bool
	ErrorStatuses        []int
	Security             *[]openapi.SecurityRequirement
	ReferencedSecSchemes []string
	StrictJSON           bool
	MaxBodyBytes         int64
	HasBodyFields        bool
	HasFormFields        bool
	HasFileFields        bool
	ResponseKind         ResponseKind
	Produces             []string
	EmptyResponse        bool
	NullableRes          bool
	ReqType              reflect.Type // struct Elem of *Req
	ResType              reflect.Type // struct Elem of *Res

	hasExplicitProduces bool
	pathParamsOrder     []string
	pathFields          []*compiledField
	queryFields         []*compiledField
	headerFields        []*compiledField
	formFields          []*compiledField
	formByName          map[string]*compiledField
	bodyFields          []*compiledField
	bodyPlan            *jsonStructPlan
	fieldByChain        map[string]*compiledField
	valValidator        *validator.Validate
	bindValidator       *validator.Validate
}

// EffectiveMaxBodyBytes returns the compiled max request body byte limit,
// falling back to 32 MiB for file-upload routes or 1 MiB otherwise.
func (op *CompiledOperation) EffectiveMaxBodyBytes() int64 {
	if op != nil && op.MaxBodyBytes > 0 {
		return op.MaxBodyBytes
	}
	if op != nil && op.HasFileFields {
		return defaultFileMaxBodyBytes
	}
	return defaultMaxBodyBytes
}

type fieldSource uint8

const (
	sourcePath fieldSource = iota + 1
	sourceQuery
	sourceHeader
	sourceForm
	sourceBody
)

func (s fieldSource) prefix() string {
	switch s {
	case sourcePath:
		return "path"
	case sourceQuery:
		return "query"
	case sourceHeader:
		return "header"
	case sourceForm:
		return "form"
	case sourceBody:
		return "body"
	default:
		return ""
	}
}

type formFileKind uint8

const (
	fileNone formFileKind = iota
	fileSingle
	fileMulti
)

type scalarKind uint8

const (
	scalarNone scalarKind = iota
	scalarString
	scalarBool
	scalarInt
	scalarUint
	scalarFloat
	scalarTime
	scalarUUID
)

type compiledScalarType struct {
	kind     scalarKind
	baseType reflect.Type
	isPtr    bool
	bitSize  int
}

type compiledField struct {
	indexChain    []int
	goName        string
	fieldType     reflect.Type
	source        fieldSource
	externalName  string
	errField      string
	description   string
	label         string
	validateTag   string
	hasValidate   bool
	hasBinding    bool
	required      bool
	isJSONString  bool
	omitempty     bool
	isSlice       bool
	isFile        bool
	fileKind      formFileKind
	scalar        compiledScalarType
	hasDefault    bool
	defaultRaw    string
	defaultVal    reflect.Value
	schemaDefault any
	hasExample    bool
	schemaExample any
	jsonSpec      *jsonTypeSpec
}

type jsonKind uint8

const (
	jsonScalar jsonKind = iota + 1
	jsonBytes
	jsonRawMessage
	jsonSlice
	jsonArray
	jsonMap
	jsonStruct
	jsonAny
)

type jsonTypeSpec struct {
	goType       reflect.Type
	elemType     reflect.Type
	ptrDepth     int
	kind         jsonKind
	isJSONString bool
	scalar       compiledScalarType
	elemSpec     *jsonTypeSpec
	mapKeySpec   compiledScalarType
	arrayLen     int
	structPlan   *jsonStructPlan
}

type jsonFieldPlan struct {
	indexChain   []int
	goName       string
	fieldType    reflect.Type
	jsonName     string
	errField     string
	omitempty    bool
	isJSONString bool
	description  string
	label        string
	validateTag  string
	required     bool
	hasDefault   bool
	defaultVal   reflect.Value
	schemaDef    any
	hasExample   bool
	schemaEx     any
	typeSpec     *jsonTypeSpec
}

type jsonStructPlan struct {
	goType     reflect.Type
	fields     []*jsonFieldPlan
	byJSONName map[string]*jsonFieldPlan
}

// CompileOperation compiles and validates the full request/response contract for
// a typed HTTP handler at registration time.
func CompileOperation(
	handlerName string,
	reqPtrType reflect.Type,
	resPtrType reflect.Type,
	opts CompileOptions,
	sr *SchemaRegistry,
) (*CompiledOperation, error) {
	if opts.EndpointMaxBodyBytes < 0 || opts.EndpointMaxBodyBytes > maxAllowedBodyBytes {
		return nil, wrapCompileErr(handlerName, fmt.Errorf("endpoint maxBodyBytes %d must be between 0 and %d", opts.EndpointMaxBodyBytes, maxAllowedBodyBytes))
	}

	reqType, resType, resKind, err := validateReqResPtrTypes(reqPtrType, resPtrType)
	if err != nil {
		return nil, wrapCompileErr(handlerName, err)
	}
	if err := checkNoCustomCodec(reqType); err != nil {
		return nil, wrapCompileErr(handlerName, fmt.Errorf("request struct %s: %w", typeDisplayName(reqType), err))
	}

	meta, err := extractAndValidateMeta(reqType, opts, resKind)
	if err != nil {
		return nil, wrapCompileErr(handlerName, err)
	}

	collector := &reqCollector{
		reqType:       reqType,
		fieldByChain:  make(map[string]*compiledField),
		seenPathNames: make(map[string]struct{}),
		seenQuery:     make(map[string]struct{}),
		seenHeaders:   make(map[string]struct{}),
		seenForm:      make(map[string]struct{}),
		seenBody:      make(map[string]struct{}),
		structMemo:    make(map[reflect.Type]*jsonStructPlan),
	}

	if err := collector.collectTopLevel(reqType, nil); err != nil {
		return nil, wrapCompileErr(handlerName, err)
	}

	hasBodyFields := len(collector.bodyFields) > 0
	hasFormFields := len(collector.formFields) > 0
	if hasBodyFields && hasFormFields {
		return nil, wrapCompileErr(handlerName, fmt.Errorf("request struct %s cannot mix form fields with JSON body fields", typeDisplayName(reqType)))
	}
	if isBodylessMethod(meta.method) && (hasBodyFields || hasFormFields) {
		if hasFormFields {
			return nil, wrapCompileErr(handlerName, fmt.Errorf("%s %s (%s) cannot declare form/file fields; use query/path/header tags", meta.method, meta.canonicalPath, typeDisplayName(reqType)))
		}
		return nil, wrapCompileErr(handlerName, fmt.Errorf("%s %s (%s) cannot declare JSON body fields; use query/path/header tags", meta.method, meta.canonicalPath, typeDisplayName(reqType)))
	}

	hasFileFields := false
	var formByName map[string]*compiledField
	if hasFormFields {
		formByName = make(map[string]*compiledField, len(collector.formFields))
		for _, cf := range collector.formFields {
			formByName[cf.externalName] = cf
			if cf.isFile {
				hasFileFields = true
			}
		}
	}

	var maxBodyBytes int64
	switch {
	case opts.EndpointMaxBodyBytes > 0:
		maxBodyBytes = opts.EndpointMaxBodyBytes
	case meta.maxBodyBytes > 0:
		maxBodyBytes = meta.maxBodyBytes
	case opts.ExplicitAppMaxBodyBytes > 0:
		maxBodyBytes = opts.ExplicitAppMaxBodyBytes
	case hasFileFields:
		maxBodyBytes = defaultFileMaxBodyBytes
	default:
		maxBodyBytes = defaultMaxBodyBytes
	}

	if err := validatePathParamPairing(reqType, meta.pathParams, collector.pathFields); err != nil {
		return nil, wrapCompileErr(handlerName, err)
	}

	assignErrFields(collector.allFields)

	bodyPlan := buildTopLevelBodyPlan(reqType, collector.bodyFields)

	valV, bindV, err := buildOperationValidators(reqType, collector.hasValidate, collector.hasBindingFallback)
	if err != nil {
		return nil, wrapCompileErr(handlerName, err)
	}

	emptyResponse := false
	if resKind == ResponseKindJSON {
		resFieldCount, err := validateResponseType(resType, make(map[reflect.Type]bool), collector.structMemo)
		if err != nil {
			return nil, wrapCompileErr(handlerName, err)
		}
		emptyResponse = resFieldCount == 0
		if meta.status == http.StatusNoContent && !emptyResponse {
			return nil, wrapCompileErr(handlerName, fmt.Errorf("status 204 requires empty response struct, but %s has %d exported JSON field(s)", typeDisplayName(resType), resFieldCount))
		}
	}

	op := &CompiledOperation{
		HandlerName:          handlerName,
		Method:               meta.method,
		MetaPath:             meta.metaPath,
		ChiPattern:           meta.chiPattern,
		CanonicalPath:        meta.canonicalPath,
		ConflictShape:        meta.conflictShape,
		OperationID:          meta.operationID,
		Status:               meta.status,
		Summary:              meta.summary,
		Description:          meta.description,
		Tags:                 meta.tags,
		Deprecated:           meta.deprecated,
		ErrorStatuses:        meta.errorStatuses,
		Security:             meta.security,
		ReferencedSecSchemes: meta.referencedSecSchemes,
		StrictJSON:           meta.strictJSON,
		MaxBodyBytes:         maxBodyBytes,
		HasBodyFields:        hasBodyFields,
		HasFormFields:        hasFormFields,
		HasFileFields:        hasFileFields,
		ResponseKind:         resKind,
		Produces:             meta.produces,
		EmptyResponse:        emptyResponse,
		NullableRes:          meta.nullableRes,
		ReqType:              reqType,
		ResType:              resType,
		hasExplicitProduces:  meta.hasExplicitProduces,
		pathParamsOrder:      meta.pathParams,
		pathFields:           collector.pathFields,
		queryFields:          collector.queryFields,
		headerFields:         collector.headerFields,
		formFields:           collector.formFields,
		formByName:           formByName,
		bodyFields:           collector.bodyFields,
		bodyPlan:             bodyPlan,
		fieldByChain:         collector.fieldByChain,
		valValidator:         valV,
		bindValidator:        bindV,
	}

	// Pre-validate and optionally populate SchemaRegistry at compile time.
	targetSR := sr
	if targetSR == nil {
		targetSR = NewSchemaRegistry()
	}
	if err := op.validateAndRegisterSchemas(targetSR); err != nil {
		return nil, wrapCompileErr(handlerName, err)
	}

	return op, nil
}

func wrapCompileErr(handlerName string, err error) error {
	if handlerName == "" {
		return err
	}
	return fmt.Errorf("%s: %w", handlerName, err)
}

type reqCollector struct {
	reqType            reflect.Type
	allFields          []*compiledField
	pathFields         []*compiledField
	queryFields        []*compiledField
	headerFields       []*compiledField
	formFields         []*compiledField
	bodyFields         []*compiledField
	fieldByChain       map[string]*compiledField
	seenPathNames      map[string]struct{}
	seenQuery          map[string]struct{}
	seenHeaders        map[string]struct{}
	seenForm           map[string]struct{}
	seenBody           map[string]struct{}
	structMemo         map[reflect.Type]*jsonStructPlan
	hasValidate        bool
	hasBindingFallback bool
}

func (rc *reqCollector) collectTopLevel(t reflect.Type, prefixChain []int) error {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if len(prefixChain) == 0 && IsPiMetaType(f.Type) {
			continue
		}

		chain := append(append([]int(nil), prefixChain...), i)

		if f.Anonymous && f.Type.Kind() == reflect.Ptr && f.Type.Elem().Kind() == reflect.Struct {
			if isResponsePackageStruct(f.Type.Elem()) || hasAnyContractTags(f) || structHasTaggedOrExportedFields(f.Type.Elem(), make(map[reflect.Type]bool)) {
				return fmt.Errorf("request struct %s cannot embed anonymous pointer struct *%s", typeDisplayName(rc.reqType), typeDisplayName(f.Type.Elem()))
			}
			continue
		}

		if !f.IsExported() {
			if err := checkUnexportedFieldTags(t, f); err != nil {
				return err
			}
			if isFlattenedAnonymousStructField(f) {
				if hasAnySourceOrValidationOrDefaultTag(f) {
					return fmt.Errorf("anonymous embedded struct %s.%s cannot carry source/default/validation tags", typeDisplayName(t), f.Name)
				}
				if err := rc.collectTopLevel(f.Type, chain); err != nil {
					return err
				}
			}
			continue
		}

		if hasOmitZeroTag(f) {
			return fmt.Errorf("field %s.%s uses unsupported json \"omitzero\" option", typeDisplayName(t), f.Name)
		}

		if isFlattenedAnonymousStructField(f) {
			if hasAnySourceOrValidationOrDefaultTag(f) {
				return fmt.Errorf("anonymous embedded struct %s.%s cannot carry source/default/validation tags", typeDisplayName(t), f.Name)
			}
			if err := rc.collectTopLevel(f.Type, chain); err != nil {
				return err
			}
			continue
		}

		cf, skipped, err := rc.compileTopLevelField(t, f, chain)
		if err != nil {
			return err
		}
		if skipped {
			continue
		}

		rc.allFields = append(rc.allFields, cf)
		rc.fieldByChain[formatIndexChain(chain)] = cf

		switch cf.source {
		case sourcePath:
			rc.pathFields = append(rc.pathFields, cf)
		case sourceQuery:
			rc.queryFields = append(rc.queryFields, cf)
		case sourceHeader:
			rc.headerFields = append(rc.headerFields, cf)
		case sourceForm:
			rc.formFields = append(rc.formFields, cf)
		case sourceBody:
			rc.bodyFields = append(rc.bodyFields, cf)
		}
	}
	return nil
}

func (rc *reqCollector) compileTopLevelField(ownerType reflect.Type, f reflect.StructField, chain []int) (*compiledField, bool, error) {
	if ft := unwrapPtrType(f.Type); ft != nil && ft.PkgPath() == httpResponsePackagePath {
		return nil, false, fmt.Errorf("field %s.%s cannot use control response type %s.%s", typeDisplayName(ownerType), f.Name, ft.PkgPath(), ft.Name())
	}
	if _, hasFileTag := f.Tag.Lookup("file"); hasFileTag {
		return nil, false, fmt.Errorf("field %s.%s uses legacy \"file\" tag; use form:\"name\" instead", typeDisplayName(ownerType), f.Name)
	}

	pathVal, hasPath := f.Tag.Lookup("path")
	queryVal, hasQuery := f.Tag.Lookup("query")
	headerVal, hasHeader := f.Tag.Lookup("header")
	formVal, hasForm := f.Tag.Lookup("form")

	sourceCount := 0
	if hasPath {
		sourceCount++
	}
	if hasQuery {
		sourceCount++
	}
	if hasHeader {
		sourceCount++
	}
	if hasForm {
		sourceCount++
	}
	if sourceCount > 1 {
		return nil, false, fmt.Errorf("field %s.%s cannot declare multiple source tags (path/query/header/form)", typeDisplayName(ownerType), f.Name)
	}

	validateTag, usedBindingFallback, err := resolveFieldValidationTag(f, ownerType)
	if err != nil {
		return nil, false, err
	}
	if _, hasVal := f.Tag.Lookup("validate"); hasVal && validateTag != "" {
		rc.hasValidate = true
	} else if usedBindingFallback && validateTag != "" {
		rc.hasBindingFallback = true
	}

	jsonName, skipJSON, omitempty, isJSONString := parseJSONTag(f)
	defaultRaw, hasDefault := f.Tag.Lookup("default")
	exampleRaw, hasExample := f.Tag.Lookup("example")
	desc := strings.TrimSpace(f.Tag.Get("dc"))
	if desc == "" {
		desc = strings.TrimSpace(f.Tag.Get("desc"))
	}
	if desc == "" {
		desc = strings.TrimSpace(f.Tag.Get("description"))
	}
	label := strings.TrimSpace(f.Tag.Get("label"))

	var src fieldSource
	var extName string

	switch {
	case hasPath:
		if err := validateParamTagName("path", pathVal, ownerType, f); err != nil {
			return nil, false, err
		}
		if _, dup := rc.seenPathNames[pathVal]; dup {
			return nil, false, fmt.Errorf("duplicate path parameter %q on %s", pathVal, typeDisplayName(rc.reqType))
		}
		rc.seenPathNames[pathVal] = struct{}{}
		src = sourcePath
		extName = pathVal
	case hasQuery:
		if err := validateParamTagName("query", queryVal, ownerType, f); err != nil {
			return nil, false, err
		}
		if _, dup := rc.seenQuery[queryVal]; dup {
			return nil, false, fmt.Errorf("duplicate query parameter %q on %s", queryVal, typeDisplayName(rc.reqType))
		}
		rc.seenQuery[queryVal] = struct{}{}
		src = sourceQuery
		extName = queryVal
	case hasHeader:
		if err := validateHeaderTagName(headerVal, ownerType, f); err != nil {
			return nil, false, err
		}
		lowerName := strings.ToLower(headerVal)
		if _, dup := rc.seenHeaders[lowerName]; dup {
			return nil, false, fmt.Errorf("duplicate header parameter %q on %s", headerVal, typeDisplayName(rc.reqType))
		}
		rc.seenHeaders[lowerName] = struct{}{}
		src = sourceHeader
		extName = headerVal
	case hasForm:
		if rawJSON, hasJSON := f.Tag.Lookup("json"); hasJSON && rawJSON != "-" {
			return nil, false, fmt.Errorf("form field %s.%s cannot combine form tag with json:%q", typeDisplayName(ownerType), f.Name, rawJSON)
		}
		if err := validateParamTagName("form", formVal, ownerType, f); err != nil {
			return nil, false, err
		}
		if _, dup := rc.seenForm[formVal]; dup {
			return nil, false, fmt.Errorf("duplicate form parameter %q on %s", formVal, typeDisplayName(rc.reqType))
		}
		rc.seenForm[formVal] = struct{}{}
		src = sourceForm
		extName = formVal
	default:
		if skipJSON {
			if hasDefault {
				return nil, false, fmt.Errorf("ignored field %s.%s (json:\"-\") cannot declare default tag", typeDisplayName(ownerType), f.Name)
			}
			// Even ignored fields might exist in struct; ensure nested structs don't hide illegal tags if inspected.
			return nil, true, nil
		}
		src = sourceBody
		extName = jsonName
		if extName == "" {
			extName = f.Name
		}
		if _, dup := rc.seenBody[extName]; dup {
			return nil, false, fmt.Errorf("duplicate JSON body field %q on %s", extName, typeDisplayName(rc.reqType))
		}
		rc.seenBody[extName] = struct{}{}
	}

	required := hasUnconditionalRequired(validateTag)
	if src == sourcePath {
		required = true
	}

	cf := &compiledField{
		indexChain:   chain,
		goName:       f.Name,
		fieldType:    f.Type,
		source:       src,
		externalName: extName,
		errField:     extName,
		description:  desc,
		label:        label,
		validateTag:  validateTag,
		hasValidate:  !usedBindingFallback && validateTag != "",
		hasBinding:   usedBindingFallback && validateTag != "",
		required:     required,
		isJSONString: isJSONString,
		omitempty:    omitempty,
	}

	switch src {
	case sourcePath, sourceQuery, sourceHeader:
		if err := compileNonBodyFieldType(cf, ownerType, f); err != nil {
			return nil, false, err
		}
	case sourceForm:
		if err := compileFormFieldType(cf, ownerType, f); err != nil {
			return nil, false, err
		}
	default:
		spec, err := rc.buildJSONTypeSpec(f.Type, isJSONString, false, rc.structMemo)
		if err != nil {
			return nil, false, fmt.Errorf("field %s.%s: %w", typeDisplayName(ownerType), f.Name, err)
		}
		cf.jsonSpec = spec
		if spec.ptrDepth <= 1 && spec.kind == jsonScalar {
			cf.scalar = spec.scalar
		}
	}

	if hasDefault {
		if cf.isFile {
			return nil, false, fmt.Errorf("file form field %s.%s cannot declare default value", typeDisplayName(ownerType), f.Name)
		}
		if src == sourcePath {
			return nil, false, fmt.Errorf("path field %s.%s cannot declare default value", typeDisplayName(ownerType), f.Name)
		}
		if hasUnconditionalRequired(validateTag) {
			return nil, false, fmt.Errorf("field %s.%s cannot combine default with unconditional required validation", typeDisplayName(ownerType), f.Name)
		}
		if cf.isSlice || cf.scalar.kind == scalarNone {
			return nil, false, fmt.Errorf("field %s.%s default tag only supports scalar types", typeDisplayName(ownerType), f.Name)
		}
		defVal, schemaDef, err := parseScalarDefaultOrExample(defaultRaw, cf.scalar, src == sourceBody && cf.isJSONString)
		if err != nil {
			return nil, false, fmt.Errorf("field %s.%s has invalid default %q: %w", typeDisplayName(ownerType), f.Name, defaultRaw, err)
		}
		cf.hasDefault = true
		cf.defaultRaw = defaultRaw
		cf.defaultVal = defVal
		cf.schemaDefault = schemaDef
	}

	if hasExample {
		if cf.isFile {
			return nil, false, fmt.Errorf("file form field %s.%s cannot declare example", typeDisplayName(ownerType), f.Name)
		}
		exVal, err := parseAndValidateExample(exampleRaw, f.Type, cf.scalar, cf.isSlice, src == sourceBody && cf.isJSONString)
		if err != nil {
			return nil, false, fmt.Errorf("field %s.%s has invalid example %q: %w", typeDisplayName(ownerType), f.Name, exampleRaw, err)
		}
		cf.hasExample = true
		cf.schemaExample = exVal
	}

	return cf, false, nil
}

func compileFormFieldType(cf *compiledField, ownerType reflect.Type, f reflect.StructField) error {
	ft := f.Type
	if ft == multipartFileHeaderPtrType {
		cf.isFile = true
		cf.fileKind = fileSingle
		return nil
	}
	if ft == multipartFileHeaderType {
		return fmt.Errorf("form field %s.%s must use *multipart.FileHeader pointer instead of value multipart.FileHeader", typeDisplayName(ownerType), f.Name)
	}
	if ft.Kind() == reflect.Slice {
		if ft.Elem() == multipartFileHeaderPtrType {
			cf.isFile = true
			cf.isSlice = true
			cf.fileKind = fileMulti
			return nil
		}
		if ft.Elem() == multipartFileHeaderType {
			return fmt.Errorf("form field %s.%s must use []*multipart.FileHeader pointer slice instead of []multipart.FileHeader", typeDisplayName(ownerType), f.Name)
		}
		if err := checkNoCustomCodec(ft.Elem()); err != nil {
			return fmt.Errorf("form field %s.%s: %w", typeDisplayName(ownerType), f.Name, err)
		}
		if ft == jsonRawMessageType || (ft.Elem().Kind() == reflect.Uint8 && ft.Elem() == reflect.TypeOf(byte(0))) {
			return fmt.Errorf("form field %s.%s cannot use raw byte slice %s", typeDisplayName(ownerType), f.Name, ft.String())
		}
		scalar, ok := classifyScalarType(ft.Elem(), false)
		if !ok || scalar.isPtr {
			return fmt.Errorf("form slice field %s.%s has unsupported element type %s", typeDisplayName(ownerType), f.Name, ft.Elem().String())
		}
		cf.isSlice = true
		cf.scalar = scalar
		return nil
	}

	if err := checkNoCustomCodec(ft); err != nil {
		return fmt.Errorf("form field %s.%s: %w", typeDisplayName(ownerType), f.Name, err)
	}
	scalar, ok := classifyScalarType(ft, true)
	if !ok {
		return fmt.Errorf("form field %s.%s has unsupported type %s", typeDisplayName(ownerType), f.Name, ft.String())
	}
	cf.scalar = scalar
	return nil
}

func compileNonBodyFieldType(cf *compiledField, ownerType reflect.Type, f reflect.StructField) error {
	ft := f.Type
	if err := checkNoCustomCodec(ft); err != nil {
		return fmt.Errorf("%s field %s.%s: %w", cf.source.prefix(), typeDisplayName(ownerType), f.Name, err)
	}
	if cf.source == sourceQuery && ft.Kind() == reflect.Slice {
		if err := checkNoCustomCodec(ft.Elem()); err != nil {
			return fmt.Errorf("%s field %s.%s: %w", cf.source.prefix(), typeDisplayName(ownerType), f.Name, err)
		}
		if ft == jsonRawMessageType || ft.Elem().Kind() == reflect.Uint8 && ft.Elem() == reflect.TypeOf(byte(0)) {
			return fmt.Errorf("query field %s.%s cannot use raw byte slice %s", typeDisplayName(ownerType), f.Name, ft.String())
		}
		scalar, ok := classifyScalarType(ft.Elem(), false)
		if !ok || scalar.isPtr {
			return fmt.Errorf("query slice field %s.%s has unsupported element type %s", typeDisplayName(ownerType), f.Name, ft.Elem().String())
		}
		cf.isSlice = true
		cf.scalar = scalar
		return nil
	}

	scalar, ok := classifyScalarType(ft, true)
	if !ok {
		return fmt.Errorf("%s field %s.%s has unsupported type %s", cf.source.prefix(), typeDisplayName(ownerType), f.Name, ft.String())
	}
	cf.scalar = scalar
	return nil
}

func (rc *reqCollector) buildJSONTypeSpec(t reflect.Type, asJSONString bool, allowRequestTags bool, memo map[reflect.Type]*jsonStructPlan) (*jsonTypeSpec, error) {
	if err := checkNoCustomCodec(t); err != nil {
		return nil, err
	}
	if asJSONString {
		scalar, ok := classifyScalarType(t, true)
		if !ok || scalar.kind == scalarTime || scalar.kind == scalarUUID {
			return nil, fmt.Errorf("json \",string\" option is only supported on string/bool/integer/float types, got %s", t.String())
		}
	}

	ptrDepth := 0
	cur := t
	for cur.Kind() == reflect.Ptr {
		ptrDepth++
		cur = cur.Elem()
	}

	if cur == jsonRawMessageType {
		return &jsonTypeSpec{
			goType:       t,
			elemType:     cur,
			ptrDepth:     ptrDepth,
			kind:         jsonRawMessage,
			isJSONString: asJSONString,
		}, nil
	}

	if scalar, ok := classifyScalarType(cur, false); ok {
		return &jsonTypeSpec{
			goType:       t,
			elemType:     cur,
			ptrDepth:     ptrDepth,
			kind:         jsonScalar,
			isJSONString: asJSONString,
			scalar:       scalar,
		}, nil
	}

	switch cur.Kind() {
	case reflect.Slice:
		if cur.Elem().Kind() == reflect.Uint8 {
			return &jsonTypeSpec{
				goType:   t,
				elemType: cur,
				ptrDepth: ptrDepth,
				kind:     jsonBytes,
			}, nil
		}
		elemSpec, err := rc.buildJSONTypeSpec(cur.Elem(), false, false, memo)
		if err != nil {
			return nil, err
		}
		return &jsonTypeSpec{
			goType:   t,
			elemType: cur,
			ptrDepth: ptrDepth,
			kind:     jsonSlice,
			elemSpec: elemSpec,
		}, nil

	case reflect.Array:
		elemSpec, err := rc.buildJSONTypeSpec(cur.Elem(), false, false, memo)
		if err != nil {
			return nil, err
		}
		return &jsonTypeSpec{
			goType:   t,
			elemType: cur,
			ptrDepth: ptrDepth,
			kind:     jsonArray,
			arrayLen: cur.Len(),
			elemSpec: elemSpec,
		}, nil

	case reflect.Map:
		if err := checkNoCustomCodec(cur.Key()); err != nil {
			return nil, err
		}
		keyScalar, ok := classifyMapKeyType(cur.Key())
		if !ok {
			return nil, fmt.Errorf("unsupported map key type %s in %s", cur.Key().String(), cur.String())
		}
		valSpec, err := rc.buildJSONTypeSpec(cur.Elem(), false, false, memo)
		if err != nil {
			return nil, err
		}
		return &jsonTypeSpec{
			goType:     t,
			elemType:   cur,
			ptrDepth:   ptrDepth,
			kind:       jsonMap,
			mapKeySpec: keyScalar,
			elemSpec:   valSpec,
		}, nil

	case reflect.Struct:
		if IsPiMetaType(cur) {
			return nil, fmt.Errorf("pi.Meta cannot appear inside nested body struct")
		}
		if cur == multipartFileHeaderType {
			return nil, fmt.Errorf("multipart.FileHeader requires a form:\"name\" tag on a top-level request field and cannot be used in JSON request or response types")
		}
		if cur.PkgPath() == httpResponsePackagePath {
			return nil, fmt.Errorf("type %s.%s from package response cannot be nested inside a request or JSON response struct", cur.PkgPath(), cur.Name())
		}
		if sp, ok := memo[cur]; ok {
			return &jsonTypeSpec{
				goType:     t,
				elemType:   cur,
				ptrDepth:   ptrDepth,
				kind:       jsonStruct,
				structPlan: sp,
			}, nil
		}
		sp := &jsonStructPlan{
			goType:     cur,
			byJSONName: make(map[string]*jsonFieldPlan),
		}
		memo[cur] = sp
		if err := rc.populateNestedStructPlan(cur, nil, sp, allowRequestTags, memo); err != nil {
			delete(memo, cur)
			return nil, err
		}
		return &jsonTypeSpec{
			goType:     t,
			elemType:   cur,
			ptrDepth:   ptrDepth,
			kind:       jsonStruct,
			structPlan: sp,
		}, nil

	case reflect.Interface:
		if cur.NumMethod() != 0 {
			return nil, fmt.Errorf("non-empty interface type %s is not supported", cur.String())
		}
		return &jsonTypeSpec{
			goType:   t,
			elemType: cur,
			ptrDepth: ptrDepth,
			kind:     jsonAny,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported type %s", cur.String())
	}
}

func (rc *reqCollector) populateNestedStructPlan(t reflect.Type, prefixChain []int, sp *jsonStructPlan, allowRequestTags bool, memo map[reflect.Type]*jsonStructPlan) error {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		chain := append(append([]int(nil), prefixChain...), i)

		if f.Anonymous && f.Type.Kind() == reflect.Ptr && f.Type.Elem().Kind() == reflect.Struct {
			if isResponsePackageStruct(f.Type.Elem()) || hasAnyContractTags(f) || structHasTaggedOrExportedFields(f.Type.Elem(), make(map[reflect.Type]bool)) {
				return fmt.Errorf("nested struct %s cannot embed anonymous pointer struct *%s", typeDisplayName(t), typeDisplayName(f.Type.Elem()))
			}
			continue
		}

		if !f.IsExported() {
			if err := checkUnexportedFieldTags(t, f); err != nil {
				return err
			}
			if isFlattenedAnonymousStructField(f) {
				if hasAnySourceOrValidationOrDefaultTag(f) {
					return fmt.Errorf("anonymous embedded struct %s.%s cannot carry source/default/validation tags", typeDisplayName(t), f.Name)
				}
				if err := rc.populateNestedStructPlan(f.Type, chain, sp, allowRequestTags, memo); err != nil {
					return err
				}
			}
			continue
		}

		if _, ok := f.Tag.Lookup("file"); ok {
			return fmt.Errorf("nested struct field %s.%s cannot declare file tag; use form:\"name\" on top-level request field", typeDisplayName(t), f.Name)
		}
		if _, ok := f.Tag.Lookup("form"); ok {
			return fmt.Errorf("nested struct field %s.%s cannot declare form tag", typeDisplayName(t), f.Name)
		}

		if !allowRequestTags {
			if _, ok := f.Tag.Lookup("path"); ok {
				return fmt.Errorf("nested struct field %s.%s cannot declare path tag", typeDisplayName(t), f.Name)
			}
			if _, ok := f.Tag.Lookup("query"); ok {
				return fmt.Errorf("nested struct field %s.%s cannot declare query tag", typeDisplayName(t), f.Name)
			}
			if _, ok := f.Tag.Lookup("header"); ok {
				return fmt.Errorf("nested struct field %s.%s cannot declare header tag", typeDisplayName(t), f.Name)
			}
		}

		if hasOmitZeroTag(f) {
			return fmt.Errorf("field %s.%s uses unsupported json \"omitzero\" option", typeDisplayName(t), f.Name)
		}

		if isFlattenedAnonymousStructField(f) {
			if hasAnySourceOrValidationOrDefaultTag(f) {
				return fmt.Errorf("anonymous embedded struct %s.%s cannot carry source/default/validation tags", typeDisplayName(t), f.Name)
			}
			if err := rc.populateNestedStructPlan(f.Type, chain, sp, allowRequestTags, memo); err != nil {
				return err
			}
			continue
		}

		if ft := unwrapPtrType(f.Type); ft != nil && ft.PkgPath() == httpResponsePackagePath {
			return fmt.Errorf("nested struct field %s.%s cannot use control response type %s.%s", typeDisplayName(t), f.Name, ft.PkgPath(), ft.Name())
		}

		jsonName, skipJSON, omitempty, isJSONString := parseJSONTag(f)
		if skipJSON {
			if _, hasDef := f.Tag.Lookup("default"); hasDef {
				return fmt.Errorf("ignored field %s.%s (json:\"-\") cannot declare default tag", typeDisplayName(t), f.Name)
			}
			continue
		}
		if jsonName == "" {
			jsonName = f.Name
		}
		if _, dup := sp.byJSONName[jsonName]; dup {
			return fmt.Errorf("duplicate JSON field %q in struct %s", jsonName, typeDisplayName(t))
		}

		validateTag, usedBindingFallback, err := resolveFieldValidationTag(f, t)
		if err != nil {
			return err
		}
		if _, hasVal := f.Tag.Lookup("validate"); hasVal && validateTag != "" {
			rc.hasValidate = true
		} else if usedBindingFallback && validateTag != "" {
			rc.hasBindingFallback = true
		}

		spec, err := rc.buildJSONTypeSpec(f.Type, isJSONString, false, memo)
		if err != nil {
			return fmt.Errorf("field %s.%s: %w", typeDisplayName(t), f.Name, err)
		}

		desc := strings.TrimSpace(f.Tag.Get("dc"))
		if desc == "" {
			desc = strings.TrimSpace(f.Tag.Get("desc"))
		}
		if desc == "" {
			desc = strings.TrimSpace(f.Tag.Get("description"))
		}

		jfp := &jsonFieldPlan{
			indexChain:   chain,
			goName:       f.Name,
			fieldType:    f.Type,
			jsonName:     jsonName,
			errField:     jsonName,
			omitempty:    omitempty,
			isJSONString: isJSONString,
			description:  desc,
			label:        strings.TrimSpace(f.Tag.Get("label")),
			validateTag:  validateTag,
			required:     hasUnconditionalRequired(validateTag),
			typeSpec:     spec,
		}

		if defRaw, hasDef := f.Tag.Lookup("default"); hasDef {
			if hasUnconditionalRequired(validateTag) {
				return fmt.Errorf("field %s.%s cannot combine default with unconditional required validation", typeDisplayName(t), f.Name)
			}
			if spec.ptrDepth > 1 || spec.kind != jsonScalar {
				return fmt.Errorf("field %s.%s default tag only supports scalar types", typeDisplayName(t), f.Name)
			}
			defVal, schemaDef, err := parseScalarDefaultOrExample(defRaw, spec.scalar, isJSONString)
			if err != nil {
				return fmt.Errorf("field %s.%s has invalid default %q: %w", typeDisplayName(t), f.Name, defRaw, err)
			}
			jfp.hasDefault = true
			jfp.defaultVal = defVal
			jfp.schemaDef = schemaDef
		}

		if exRaw, hasEx := f.Tag.Lookup("example"); hasEx {
			var scalar compiledScalarType
			if spec.ptrDepth <= 1 && spec.kind == jsonScalar {
				scalar = spec.scalar
			}
			exVal, err := parseAndValidateExample(exRaw, f.Type, scalar, false, isJSONString)
			if err != nil {
				return fmt.Errorf("field %s.%s has invalid example %q: %w", typeDisplayName(t), f.Name, exRaw, err)
			}
			jfp.hasExample = true
			jfp.schemaEx = exVal
		}

		sp.fields = append(sp.fields, jfp)
		sp.byJSONName[jsonName] = jfp
	}
	return nil
}

func buildTopLevelBodyPlan(reqType reflect.Type, bodyFields []*compiledField) *jsonStructPlan {
	sp := &jsonStructPlan{
		goType:     reqType,
		fields:     make([]*jsonFieldPlan, 0, len(bodyFields)),
		byJSONName: make(map[string]*jsonFieldPlan, len(bodyFields)),
	}
	for _, cf := range bodyFields {
		jfp := &jsonFieldPlan{
			indexChain:   cf.indexChain,
			goName:       cf.goName,
			fieldType:    cf.fieldType,
			jsonName:     cf.externalName,
			errField:     cf.errField,
			omitempty:    cf.omitempty,
			isJSONString: cf.isJSONString,
			description:  cf.description,
			label:        cf.label,
			validateTag:  cf.validateTag,
			required:     cf.required,
			hasDefault:   cf.hasDefault,
			defaultVal:   cf.defaultVal,
			schemaDef:    cf.schemaDefault,
			hasExample:   cf.hasExample,
			schemaEx:     cf.schemaExample,
			typeSpec:     cf.jsonSpec,
		}
		sp.fields = append(sp.fields, jfp)
		sp.byJSONName[cf.externalName] = jfp
	}
	return sp
}

func validateResponseType(t reflect.Type, visited map[reflect.Type]bool, memo map[reflect.Type]*jsonStructPlan) (int, error) {
	if err := checkNoCustomCodec(t); err != nil {
		return 0, fmt.Errorf("response struct %s: %w", typeDisplayName(t), err)
	}

	rc := &reqCollector{
		reqType:    t,
		structMemo: memo,
	}

	sp := &jsonStructPlan{
		goType:     t,
		byJSONName: make(map[string]*jsonFieldPlan),
	}
	memo[t] = sp
	if err := validateResponseStructFields(t, sp, rc, visited); err != nil {
		delete(memo, t)
		return 0, err
	}
	return len(sp.fields), nil
}

func validateResponseStructFields(t reflect.Type, sp *jsonStructPlan, rc *reqCollector, visited map[reflect.Type]bool) error {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		if f.Anonymous && f.Type.Kind() == reflect.Ptr && f.Type.Elem().Kind() == reflect.Struct {
			if isResponsePackageStruct(f.Type.Elem()) || hasAnyContractTags(f) || structHasTaggedOrExportedFields(f.Type.Elem(), make(map[reflect.Type]bool)) {
				return fmt.Errorf("response struct %s cannot embed anonymous pointer struct *%s", typeDisplayName(t), typeDisplayName(f.Type.Elem()))
			}
			continue
		}

		if !f.IsExported() {
			if err := checkUnexportedFieldTags(t, f); err != nil {
				return err
			}
			if isFlattenedAnonymousStructField(f) {
				if err := validateResponseStructFields(f.Type, sp, rc, visited); err != nil {
					return err
				}
			}
			continue
		}

		if _, ok := f.Tag.Lookup("path"); ok {
			return fmt.Errorf("response field %s.%s cannot declare path tag", typeDisplayName(t), f.Name)
		}
		if _, ok := f.Tag.Lookup("query"); ok {
			return fmt.Errorf("response field %s.%s cannot declare query tag", typeDisplayName(t), f.Name)
		}
		if _, ok := f.Tag.Lookup("header"); ok {
			return fmt.Errorf("response field %s.%s cannot declare header tag", typeDisplayName(t), f.Name)
		}
		if _, ok := f.Tag.Lookup("form"); ok {
			return fmt.Errorf("response field %s.%s cannot declare form tag", typeDisplayName(t), f.Name)
		}
		if _, ok := f.Tag.Lookup("file"); ok {
			return fmt.Errorf("response field %s.%s cannot declare file tag; use form:\"name\" on request field", typeDisplayName(t), f.Name)
		}
		if hasOmitZeroTag(f) {
			return fmt.Errorf("response field %s.%s uses unsupported json \"omitzero\" option", typeDisplayName(t), f.Name)
		}
		if _, _, err := resolveFieldValidationTag(f, t); err != nil {
			return err
		}

		if isFlattenedAnonymousStructField(f) {
			if err := validateResponseStructFields(f.Type, sp, rc, visited); err != nil {
				return err
			}
			continue
		}

		if ft := unwrapPtrType(f.Type); ft != nil && ft.PkgPath() == httpResponsePackagePath {
			return fmt.Errorf("response field %s.%s cannot use control response type %s.%s", typeDisplayName(t), f.Name, ft.PkgPath(), ft.Name())
		}

		jsonName, skipJSON, omitempty, isJSONString := parseJSONTag(f)
		if skipJSON {
			continue
		}
		if jsonName == "" {
			jsonName = f.Name
		}
		if _, dup := sp.byJSONName[jsonName]; dup {
			return fmt.Errorf("duplicate JSON field %q in response struct %s", jsonName, typeDisplayName(t))
		}

		spec, err := rc.buildJSONTypeSpec(f.Type, isJSONString, false, rc.structMemo)
		if err != nil {
			return fmt.Errorf("response field %s.%s: %w", typeDisplayName(t), f.Name, err)
		}

		if exRaw, hasEx := f.Tag.Lookup("example"); hasEx {
			var scalar compiledScalarType
			if spec.ptrDepth <= 1 && spec.kind == jsonScalar {
				scalar = spec.scalar
			}
			if _, err := parseAndValidateExample(exRaw, f.Type, scalar, false, isJSONString); err != nil {
				return fmt.Errorf("response field %s.%s has invalid example %q: %w", typeDisplayName(t), f.Name, exRaw, err)
			}
		}

		jfp := &jsonFieldPlan{
			goName:       f.Name,
			fieldType:    f.Type,
			jsonName:     jsonName,
			omitempty:    omitempty,
			isJSONString: isJSONString,
			typeSpec:     spec,
		}
		sp.fields = append(sp.fields, jfp)
		sp.byJSONName[jsonName] = jfp
	}
	return nil
}

func validatePathParamPairing(reqType reflect.Type, pathParams []string, pathFields []*compiledField) error {
	paramSet := make(map[string]struct{}, len(pathParams))
	for _, p := range pathParams {
		paramSet[p] = struct{}{}
	}
	fieldSet := make(map[string]struct{}, len(pathFields))
	for _, cf := range pathFields {
		fieldSet[cf.externalName] = struct{}{}
		if _, ok := paramSet[cf.externalName]; !ok {
			return fmt.Errorf("request struct %s declares path:\"%s\" which does not appear in route path", typeDisplayName(reqType), cf.externalName)
		}
	}
	for _, p := range pathParams {
		if _, ok := fieldSet[p]; !ok {
			return fmt.Errorf("route path placeholder {%s} has no matching path:\"%s\" field on %s", p, p, typeDisplayName(reqType))
		}
	}
	return nil
}

func assignErrFields(fields []*compiledField) {
	counts := make(map[string]int, len(fields))
	for _, cf := range fields {
		counts[cf.externalName]++
	}
	for _, cf := range fields {
		if counts[cf.externalName] > 1 {
			cf.errField = cf.source.prefix() + "." + cf.externalName
		} else {
			cf.errField = cf.externalName
		}
	}
}

func resolveFieldValidationTag(f reflect.StructField, owner reflect.Type) (string, bool, error) {
	valTag, hasVal := f.Tag.Lookup("validate")
	bindTag, hasBind := f.Tag.Lookup("binding")
	if hasVal && hasBind {
		if valTag != bindTag {
			return "", false, fmt.Errorf("field %s.%s has mismatched validate (%q) and binding (%q) tags", typeDisplayName(owner), f.Name, valTag, bindTag)
		}
		return valTag, false, nil
	}
	if hasVal {
		return valTag, false, nil
	}
	if hasBind {
		return bindTag, true, nil
	}
	return "", false, nil
}

func validateParamTagName(tagName, val string, owner reflect.Type, f reflect.StructField) error {
	if val == "" || val == "-" || strings.Contains(val, ",") {
		return fmt.Errorf("field %s.%s has invalid %s tag %q", typeDisplayName(owner), f.Name, tagName, val)
	}
	for _, r := range val {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("field %s.%s %s tag %q cannot contain whitespace or control characters", typeDisplayName(owner), f.Name, tagName, val)
		}
	}
	return nil
}

func validateHeaderTagName(val string, owner reflect.Type, f reflect.StructField) error {
	if err := validateParamTagName("header", val, owner, f); err != nil {
		return err
	}
	if strings.Contains(val, ":") {
		return fmt.Errorf("field %s.%s header tag %q cannot contain ':'", typeDisplayName(owner), f.Name, val)
	}
	if _, forbidden := reservedHeaderNames[strings.ToLower(val)]; forbidden {
		return fmt.Errorf("field %s.%s header %q is reserved; configure security via DocSecurity or media types instead", typeDisplayName(owner), f.Name, val)
	}
	return nil
}

func checkUnexportedFieldTags(owner reflect.Type, f reflect.StructField) error {
	for _, tag := range unexportedForbiddenTags {
		if _, ok := f.Tag.Lookup(tag); ok {
			return fmt.Errorf("unexported field %s.%s cannot carry %q tag", typeDisplayName(owner), f.Name, tag)
		}
	}
	return nil
}

func hasAnyContractTags(f reflect.StructField) bool {
	for _, tag := range unexportedForbiddenTags {
		if _, ok := f.Tag.Lookup(tag); ok {
			return true
		}
	}
	if _, ok := f.Tag.Lookup("label"); ok {
		return true
	}
	if raw, ok := f.Tag.Lookup("json"); ok && raw != "-" {
		return true
	}
	return false
}

func hasAnySourceOrValidationOrDefaultTag(f reflect.StructField) bool {
	for _, tag := range []string{"path", "query", "header", "form", "file", "default", "validate", "binding"} {
		if _, ok := f.Tag.Lookup(tag); ok {
			return true
		}
	}
	return false
}

func structHasTaggedOrExportedFields(t reflect.Type, visited map[reflect.Type]bool) bool {
	if visited[t] {
		return false
	}
	visited[t] = true
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.IsExported() || hasAnyContractTags(f) {
			return true
		}
		if f.Anonymous && f.Type.Kind() == reflect.Struct && !isSpecialStructType(f.Type) {
			if structHasTaggedOrExportedFields(f.Type, visited) {
				return true
			}
		}
	}
	return false
}

func isResponsePackageStruct(t reflect.Type) bool {
	return t != nil && t.PkgPath() == httpResponsePackagePath && t.Kind() == reflect.Struct
}

func isSpecialStructType(t reflect.Type) bool {
	return t == timeType || t == multipartFileHeaderType || isResponsePackageStruct(t) || IsPiMetaType(t)
}

func isFlattenedAnonymousStructField(f reflect.StructField) bool {
	if !f.Anonymous {
		return false
	}
	if f.Type.Kind() != reflect.Struct || isSpecialStructType(f.Type) {
		return false
	}
	name, skip, _, _ := parseJSONTag(f)
	if skip || name != "" {
		return false
	}
	return true
}

func hasOmitZeroTag(f reflect.StructField) bool {
	raw, ok := f.Tag.Lookup("json")
	if !ok || raw == "-" {
		return false
	}
	parts := strings.Split(raw, ",")
	for _, opt := range parts[1:] {
		if opt == "omitzero" {
			return true
		}
	}
	return false
}

func parseJSONTag(f reflect.StructField) (name string, skip bool, omitempty bool, asString bool) {
	raw, ok := f.Tag.Lookup("json")
	if !ok {
		return "", false, false, false
	}
	if raw == "-" {
		return "", true, false, false
	}
	parts := strings.Split(raw, ",")
	name = parts[0]
	for _, opt := range parts[1:] {
		switch opt {
		case "omitempty":
			omitempty = true
		case "string":
			asString = true
		}
	}
	return name, false, omitempty, asString
}

func checkNoCustomCodec(t reflect.Type) error {
	if t == nil {
		return nil
	}
	cur := t
	for cur.Kind() == reflect.Ptr {
		if cur.Name() != "" {
			if err := checkTypeImplementsCustomCodec(cur); err != nil {
				return err
			}
		}
		cur = cur.Elem()
	}
	if cur == timeType || cur == uuidType || cur == jsonRawMessageType {
		return nil
	}
	return checkTypeImplementsCustomCodec(cur)
}

func checkTypeImplementsCustomCodec(t reflect.Type) error {
	ptr := reflect.PtrTo(t)
	switch {
	case t.Implements(jsonMarshalerType) || ptr.Implements(jsonMarshalerType):
		return fmt.Errorf("type %s implements custom json.Marshaler which is not supported in typed contracts", typeDisplayName(t))
	case t.Implements(jsonUnmarshalerType) || ptr.Implements(jsonUnmarshalerType):
		return fmt.Errorf("type %s implements custom json.Unmarshaler which is not supported in typed contracts", typeDisplayName(t))
	case t.Implements(textMarshalerType) || ptr.Implements(textMarshalerType):
		return fmt.Errorf("type %s implements custom encoding.TextMarshaler which is not supported in typed contracts", typeDisplayName(t))
	case t.Implements(textUnmarshalerType) || ptr.Implements(textUnmarshalerType):
		return fmt.Errorf("type %s implements custom encoding.TextUnmarshaler which is not supported in typed contracts", typeDisplayName(t))
	default:
		return nil
	}
}

func classifyScalarType(t reflect.Type, allowSinglePtr bool) (compiledScalarType, bool) {
	isPtr := false
	base := t
	if base.Kind() == reflect.Ptr {
		if !allowSinglePtr {
			return compiledScalarType{}, false
		}
		isPtr = true
		base = base.Elem()
		if base.Kind() == reflect.Ptr {
			return compiledScalarType{}, false
		}
	}

	if base == timeType {
		return compiledScalarType{kind: scalarTime, baseType: base, isPtr: isPtr}, true
	}
	if base == uuidType {
		return compiledScalarType{kind: scalarUUID, baseType: base, isPtr: isPtr}, true
	}
	if err := checkNoCustomCodec(base); err != nil {
		return compiledScalarType{}, false
	}

	switch base.Kind() {
	case reflect.String:
		return compiledScalarType{kind: scalarString, baseType: base, isPtr: isPtr}, true
	case reflect.Bool:
		return compiledScalarType{kind: scalarBool, baseType: base, isPtr: isPtr}, true
	case reflect.Int:
		return compiledScalarType{kind: scalarInt, baseType: base, isPtr: isPtr, bitSize: strconv.IntSize}, true
	case reflect.Int8:
		return compiledScalarType{kind: scalarInt, baseType: base, isPtr: isPtr, bitSize: 8}, true
	case reflect.Int16:
		return compiledScalarType{kind: scalarInt, baseType: base, isPtr: isPtr, bitSize: 16}, true
	case reflect.Int32:
		return compiledScalarType{kind: scalarInt, baseType: base, isPtr: isPtr, bitSize: 32}, true
	case reflect.Int64:
		return compiledScalarType{kind: scalarInt, baseType: base, isPtr: isPtr, bitSize: 64}, true
	case reflect.Uint:
		return compiledScalarType{kind: scalarUint, baseType: base, isPtr: isPtr, bitSize: strconv.IntSize}, true
	case reflect.Uint8:
		return compiledScalarType{kind: scalarUint, baseType: base, isPtr: isPtr, bitSize: 8}, true
	case reflect.Uint16:
		return compiledScalarType{kind: scalarUint, baseType: base, isPtr: isPtr, bitSize: 16}, true
	case reflect.Uint32:
		return compiledScalarType{kind: scalarUint, baseType: base, isPtr: isPtr, bitSize: 32}, true
	case reflect.Uint64:
		return compiledScalarType{kind: scalarUint, baseType: base, isPtr: isPtr, bitSize: 64}, true
	case reflect.Float32:
		return compiledScalarType{kind: scalarFloat, baseType: base, isPtr: isPtr, bitSize: 32}, true
	case reflect.Float64:
		return compiledScalarType{kind: scalarFloat, baseType: base, isPtr: isPtr, bitSize: 64}, true
	default:
		return compiledScalarType{}, false
	}
}

func classifyMapKeyType(t reflect.Type) (compiledScalarType, bool) {
	sc, ok := classifyScalarType(t, false)
	if !ok {
		return compiledScalarType{}, false
	}
	switch sc.kind {
	case scalarString, scalarInt, scalarUint:
		return sc, true
	default:
		return compiledScalarType{}, false
	}
}

func parseScalarReflectValue(raw string, sc compiledScalarType) (reflect.Value, error) {
	rv, _, err := parseScalarReflectValueAndSchema(raw, sc)
	return rv, err
}

func parseScalarReflectValueAndSchema(raw string, sc compiledScalarType) (reflect.Value, any, error) {
	v := reflect.New(sc.baseType).Elem()
	switch sc.kind {
	case scalarString:
		v.SetString(raw)
		return v, raw, nil
	case scalarBool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return reflect.Value{}, nil, err
		}
		v.SetBool(b)
		return v, b, nil
	case scalarInt:
		n, err := strconv.ParseInt(raw, 10, sc.bitSize)
		if err != nil {
			return reflect.Value{}, nil, err
		}
		v.SetInt(n)
		return v, n, nil
	case scalarUint:
		u, err := strconv.ParseUint(raw, 10, sc.bitSize)
		if err != nil {
			return reflect.Value{}, nil, err
		}
		v.SetUint(u)
		return v, u, nil
	case scalarFloat:
		f, err := strconv.ParseFloat(raw, sc.bitSize)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return reflect.Value{}, nil, fmt.Errorf("invalid finite float")
		}
		v.SetFloat(f)
		return v, f, nil
	case scalarTime:
		tm, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return reflect.Value{}, nil, err
		}
		v.Set(reflect.ValueOf(tm))
		return v, raw, nil
	case scalarUUID:
		u, err := uuid.Parse(raw)
		if err != nil {
			return reflect.Value{}, nil, err
		}
		v.Set(reflect.ValueOf(u))
		return v, u.String(), nil
	default:
		return reflect.Value{}, nil, fmt.Errorf("unsupported scalar kind")
	}
}

func parseScalarDefaultOrExample(raw string, sc compiledScalarType, asJSONString bool) (reflect.Value, any, error) {
	rv, schemaVal, err := parseScalarReflectValueAndSchema(raw, sc)
	if err != nil {
		return reflect.Value{}, nil, err
	}
	if asJSONString {
		schemaVal = raw
	}
	return rv, schemaVal, nil
}

func parseAndValidateExample(raw string, fieldType reflect.Type, scalar compiledScalarType, isSlice bool, asJSONString bool) (any, error) {
	if !isSlice && scalar.kind != scalarNone {
		_, schemaVal, err := parseScalarDefaultOrExample(raw, scalar, asJSONString)
		return schemaVal, err
	}

	cur := fieldType
	for cur.Kind() == reflect.Ptr {
		cur = cur.Elem()
	}

	if cur == jsonRawMessageType {
		if !json.Valid([]byte(raw)) {
			return nil, fmt.Errorf("example must be valid JSON")
		}
		var out any
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			return nil, err
		}
		return out, nil
	}

	if cur.Kind() == reflect.Slice && cur.Elem().Kind() == reflect.Uint8 && cur != jsonRawMessageType {
		if _, err := base64.StdEncoding.DecodeString(raw); err != nil {
			return nil, fmt.Errorf("example must be valid base64: %w", err)
		}
		return raw, nil
	}

	target := reflect.New(cur).Interface()
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		return nil, fmt.Errorf("example must be valid JSON for %s: %w", fieldType.String(), err)
	}
	var out any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return out, nil
}
