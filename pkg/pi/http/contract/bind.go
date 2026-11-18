package contract

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	piHTTP "github.com/sllt/pi/pkg/pi/http"
	resTypes "github.com/sllt/pi/pkg/pi/http/response"
)

// BindAndValidate decodes path, query, header, and JSON body inputs into reqPtr
// according to the compiled FieldPlan, runs standard struct validation, and
// invokes customValidator (if non-nil) once afterward.
func (op *CompiledOperation) BindAndValidate(
	httpReq *piHTTP.Request,
	reqPtr any,
	customValidator func(any) error,
) error {
	if op == nil {
		return errors.New("compiled operation is nil")
	}
	if httpReq == nil || httpReq.HTTPRequest() == nil {
		return errors.New("typed HTTP handler requires an HTTP request context")
	}
	rv := reflect.ValueOf(reqPtr)
	if !rv.IsValid() || rv.Kind() != reflect.Ptr || rv.IsNil() || rv.Elem().Type() != op.ReqType {
		return fmt.Errorf("typed handler %s received invalid request target %T", op.HandlerName, reqPtr)
	}

	reqVal := rv.Elem()
	rawReq := httpReq.HTTPRequest()

	rawBody, bodyErr := httpReq.RawBodyBytesWithLimit(op.EffectiveMaxBodyBytes())
	if bodyErr != nil {
		var limit *http.MaxBytesError
		if errors.As(bodyErr, &limit) {
			return piHTTP.ErrorPayloadTooLarge{}
		}
		return newInvalidArgumentError("invalid request body", nil)
	}

	var (
		formValues url.Values
		formFiles  map[string][]*multipart.FileHeader
	)
	switch {
	case op.HasBodyFields:
		if len(rawBody) > 0 {
			if !isJSONContentType(rawReq.Header.Get("Content-Type")) {
				return unsupportedMediaTypeError{}
			}
		}
	case op.HasFormFields:
		var formErr error
		formValues, formFiles, formErr = parseFormInputs(rawReq, rawBody, op.HasFileFields)
		if formErr != nil {
			return formErr
		}
	default:
		if err := op.validateEmptyBodyContract(rawReq, rawBody); err != nil {
			return err
		}
	}

	var violations []fieldViolation

	if len(op.pathFields) > 0 {
		rctx := chi.RouteContext(rawReq.Context())
		for _, cf := range op.pathFields {
			rawVal, found := lookupChiURLParam(rctx, cf.externalName)
			if !found || rawVal == "" {
				violations = append(violations, fieldViolation{
					Field: cf.errField,
					Rule:  "required",
				})
				continue
			}
			parsedRV, err := parseScalarReflectValue(rawVal, cf.scalar)
			if err != nil {
				violations = append(violations, fieldViolation{
					Field: cf.errField,
					Rule:  "type",
				})
				continue
			}
			setScalarField(reqVal.FieldByIndex(cf.indexChain), parsedRV, cf.scalar.isPtr)
		}
	}

	if len(op.queryFields) > 0 {
		var queryValues url.Values
		if rawReq.URL != nil {
			queryValues = rawReq.URL.Query()
		}
		for _, cf := range op.queryFields {
			vals, exists := queryValues[cf.externalName]
			if !exists {
				if cf.hasDefault {
					setScalarField(reqVal.FieldByIndex(cf.indexChain), cf.defaultVal, cf.scalar.isPtr)
				}
				continue
			}
			if cf.isSlice {
				sliceVal := reflect.MakeSlice(cf.fieldType, len(vals), len(vals))
				sliceOK := true
				for idx, item := range vals {
					elemRV, err := parseScalarReflectValue(item, cf.scalar)
					if err != nil {
						violations = append(violations, fieldViolation{
							Field: fmt.Sprintf("%s[%d]", cf.errField, idx),
							Rule:  "type",
						})
						sliceOK = false
						continue
					}
					sliceVal.Index(idx).Set(elemRV)
				}
				if sliceOK {
					reqVal.FieldByIndex(cf.indexChain).Set(sliceVal)
				}
				continue
			}
			if len(vals) > 1 {
				violations = append(violations, fieldViolation{
					Field: cf.errField,
					Rule:  "single_value",
				})
				continue
			}
			if len(vals) == 1 {
				parsedRV, err := parseScalarReflectValue(vals[0], cf.scalar)
				if err != nil {
					violations = append(violations, fieldViolation{
						Field: cf.errField,
						Rule:  "type",
					})
					continue
				}
				setScalarField(reqVal.FieldByIndex(cf.indexChain), parsedRV, cf.scalar.isPtr)
			}
		}
	}

	if len(op.headerFields) > 0 {
		for _, cf := range op.headerFields {
			vals := lookupHeaderValues(rawReq.Header, cf.externalName)
			if len(vals) == 0 {
				if cf.hasDefault {
					setScalarField(reqVal.FieldByIndex(cf.indexChain), cf.defaultVal, cf.scalar.isPtr)
				}
				continue
			}
			if len(vals) > 1 {
				violations = append(violations, fieldViolation{
					Field: cf.errField,
					Rule:  "single_value",
				})
				continue
			}
			parsedRV, err := parseScalarReflectValue(vals[0], cf.scalar)
			if err != nil {
				violations = append(violations, fieldViolation{
					Field: cf.errField,
					Rule:  "type",
				})
				continue
			}
			setScalarField(reqVal.FieldByIndex(cf.indexChain), parsedRV, cf.scalar.isPtr)
		}
	}

	if op.HasBodyFields {
		if len(rawBody) == 0 {
			applyTopLevelBodyDefaults(reqVal, op.bodyPlan)
		} else {
			if err := decodeJSONBody(rawBody, reqVal, op.bodyPlan, op.StrictJSON, &violations); err != nil {
				return err
			}
		}
	} else if op.HasFormFields {
		bindFormFields(op, reqVal, formValues, formFiles, &violations)
	}

	if len(violations) > 0 {
		return newInvalidArgumentError("invalid request parameters", violations)
	}

	return op.runStructValidation(reqPtr, customValidator)
}

// WrapResponse wraps the typed handler output and error into responder-compatible results.
func (op *CompiledOperation) WrapResponse(resPtr any, err error) (any, error) {
	if err != nil {
		return nil, err
	}
	if op == nil {
		return nil, errors.New("compiled operation is nil")
	}
	if op.ResponseKind == ResponseKindFile {
		return op.wrapFileResponse(resPtr)
	}
	if op.ResponseKind == ResponseKindXML {
		return op.wrapXMLResponse(resPtr)
	}
	if op.ResponseKind == ResponseKindStream {
		return op.wrapStreamResponse(resPtr)
	}
	if op.ResponseKind == ResponseKindSSE {
		return op.wrapSSEResponse(resPtr)
	}
	if op.Status == http.StatusNoContent {
		return resTypes.NoContent(), nil
	}
	if op.EmptyResponse {
		return resTypes.Result{StatusCode: op.Status, Data: nil}, nil
	}
	if isNilResponseValue(resPtr) {
		if op.NullableRes {
			return resTypes.Result{StatusCode: op.Status, Data: nil}, nil
		}
		resName := "Res"
		if op.ResType != nil && op.ResType.Name() != "" {
			resName = op.ResType.Name()
		}
		return nil, fmt.Errorf("typed handler %s returned nil *%s without error", op.HandlerName, resName)
	}
	return resTypes.Result{StatusCode: op.Status, Data: resPtr}, nil
}

func (op *CompiledOperation) wrapFileResponse(resPtr any) (any, error) {
	filePtr, ok := resPtr.(*resTypes.File)
	if !ok || filePtr == nil {
		return nil, fmt.Errorf("typed handler %s returned nil *response.File without error", op.HandlerName)
	}
	val := *filePtr
	if val.StatusCode != 0 && val.StatusCode != op.Status {
		return nil, fmt.Errorf("typed handler %s returned *response.File with StatusCode %d, expected 0 or %d", op.HandlerName, val.StatusCode, op.Status)
	}
	val.StatusCode = op.Status

	ct := strings.TrimSpace(val.ContentType)
	if ct == "" {
		var hdrErr error
		ct, hdrErr = lookupHeaderContentType(val.Headers)
		if hdrErr != nil {
			return nil, fmt.Errorf("typed handler %s returned *response.File with %w", op.HandlerName, hdrErr)
		}
	}
	resolvedCT, err := op.validateAndResolveResponseContentType(ct, "application/octet-stream")
	if err != nil {
		return nil, err
	}
	val.ContentType = resolvedCT
	return resTypes.Result{StatusCode: op.Status, Data: val}, nil
}

func (op *CompiledOperation) wrapXMLResponse(resPtr any) (any, error) {
	xmlPtr, ok := resPtr.(*resTypes.XML)
	if !ok || xmlPtr == nil {
		return nil, fmt.Errorf("typed handler %s returned nil *response.XML without error", op.HandlerName)
	}
	val := *xmlPtr
	if val.StatusCode != 0 && val.StatusCode != op.Status {
		return nil, fmt.Errorf("typed handler %s returned *response.XML with StatusCode %d, expected 0 or %d", op.HandlerName, val.StatusCode, op.Status)
	}
	val.StatusCode = op.Status

	ct := strings.TrimSpace(val.ContentType)
	resolvedCT, err := op.validateAndResolveResponseContentType(ct, "application/xml")
	if err != nil {
		return nil, err
	}
	if ct == "" {
		val.ContentType = resolvedCT
	} else {
		val.ContentType = ct
	}
	return resTypes.Result{StatusCode: op.Status, Data: val}, nil
}

func (op *CompiledOperation) wrapStreamResponse(resPtr any) (any, error) {
	streamPtr, ok := resPtr.(*resTypes.Stream)
	if !ok || streamPtr == nil {
		return nil, fmt.Errorf("typed handler %s returned nil *response.Stream without error", op.HandlerName)
	}
	if streamPtr.Run == nil {
		return nil, fmt.Errorf("typed handler %s returned *response.Stream with nil Run callback", op.HandlerName)
	}
	val := *streamPtr
	if val.StatusCode != 0 && val.StatusCode != op.Status {
		return nil, fmt.Errorf("typed handler %s returned *response.Stream with StatusCode %d, expected 0 or %d", op.HandlerName, val.StatusCode, op.Status)
	}
	val.StatusCode = op.Status

	ct := strings.TrimSpace(val.ContentType)
	if ct == "" {
		var hdrErr error
		ct, hdrErr = lookupHeaderContentType(val.Headers)
		if hdrErr != nil {
			return nil, fmt.Errorf("typed handler %s returned *response.Stream with %w", op.HandlerName, hdrErr)
		}
	}
	resolvedCT, err := op.validateAndResolveResponseContentType(ct, "application/octet-stream")
	if err != nil {
		return nil, err
	}
	val.ContentType = resolvedCT
	return resTypes.Result{StatusCode: op.Status, Data: val}, nil
}

func (op *CompiledOperation) wrapSSEResponse(resPtr any) (any, error) {
	ssePtr, ok := resPtr.(*resTypes.SSE)
	if !ok || ssePtr == nil {
		return nil, fmt.Errorf("typed handler %s returned nil *response.SSE without error", op.HandlerName)
	}
	if ssePtr.Run == nil {
		return nil, fmt.Errorf("typed handler %s returned *response.SSE with nil Run callback", op.HandlerName)
	}
	val := *ssePtr
	if len(ssePtr.Headers) > 0 {
		cloned := make(map[string]string, len(ssePtr.Headers))
		for k, v := range ssePtr.Headers {
			cloned[k] = v
		}
		val.Headers = cloned
	}

	ct, hdrErr := lookupHeaderContentType(val.Headers)
	if hdrErr != nil {
		return nil, fmt.Errorf("typed handler %s returned *response.SSE with %w", op.HandlerName, hdrErr)
	}
	if ct != "" {
		_, actualMT, actualParams, err := parseAndValidateMediaRange(ct, false)
		if err != nil {
			return nil, fmt.Errorf("typed handler %s returned invalid Content-Type %q: %w", op.HandlerName, ct, err)
		}
		if !mediaRangeMatches(actualMT, actualParams, "text/event-stream") {
			return nil, fmt.Errorf("typed handler %s returned Content-Type %q which is incompatible with *response.SSE", op.HandlerName, ct)
		}
	}
	resolvedCT, err := op.validateAndResolveResponseContentType(ct, "text/event-stream; charset=utf-8")
	if err != nil {
		return nil, err
	}
	if ct == "" {
		if val.Headers == nil {
			val.Headers = make(map[string]string, 1)
		}
		if !op.hasExplicitProduces {
			val.Headers["Content-Type"] = "text/event-stream; charset=utf-8"
		} else {
			val.Headers["Content-Type"] = resolvedCT
		}
	}
	return resTypes.Result{StatusCode: op.Status, Data: val}, nil
}

func (op *CompiledOperation) validateAndResolveResponseContentType(ct, defaultCT string) (string, error) {
	if ct != "" {
		_, actualMT, actualParams, err := parseAndValidateMediaRange(ct, false)
		if err != nil {
			return "", fmt.Errorf("typed handler %s returned invalid Content-Type %q: %w", op.HandlerName, ct, err)
		}
		if op.hasExplicitProduces {
			matched := false
			for _, rng := range op.Produces {
				if mediaRangeMatches(actualMT, actualParams, rng) {
					matched = true
					break
				}
			}
			if !matched {
				return "", fmt.Errorf("typed handler %s returned Content-Type %q which does not match declared produces %v", op.HandlerName, ct, op.Produces)
			}
		}
		return ct, nil
	}

	if len(op.Produces) > 0 && !strings.Contains(op.Produces[0], "*") {
		return op.Produces[0], nil
	}
	_, defMT, defParams, err := parseAndValidateMediaRange(defaultCT, false)
	if err == nil {
		for _, rng := range op.Produces {
			if mediaRangeMatches(defMT, defParams, rng) {
				return defaultCT, nil
			}
		}
	}
	return "", fmt.Errorf("typed handler %s must specify ContentType matching produces %v", op.HandlerName, op.Produces)
}

func lookupHeaderContentType(headers map[string]string) (string, error) {
	if len(headers) == 0 {
		return "", nil
	}
	var found string
	for k, v := range headers {
		if !strings.EqualFold(k, "Content-Type") {
			continue
		}
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			continue
		}
		if found != "" && found != trimmed {
			return "", fmt.Errorf("conflicting Content-Type header values %q and %q", found, trimmed)
		}
		found = trimmed
	}
	return found, nil
}

func parseFormInputs(rawReq *http.Request, rawBody []byte, hasFileFields bool) (url.Values, map[string][]*multipart.FileHeader, error) {
	ct := rawReq.Header.Get("Content-Type")
	trimmedCT := strings.TrimSpace(ct)
	if len(rawBody) == 0 {
		if trimmedCT == "" {
			return nil, nil, nil
		}
		mediaType, _, ctErr := mime.ParseMediaType(ct)
		if ctErr != nil {
			return nil, nil, unsupportedMediaTypeError{}
		}
		mediaType = strings.ToLower(mediaType)
		switch mediaType {
		case "application/x-www-form-urlencoded":
			if hasFileFields {
				return nil, nil, unsupportedMediaTypeError{}
			}
			return nil, nil, nil
		case "multipart/form-data":
			return nil, nil, newInvalidArgumentError("invalid request body", nil)
		default:
			return nil, nil, unsupportedMediaTypeError{}
		}
	}

	if trimmedCT == "" {
		return nil, nil, unsupportedMediaTypeError{}
	}
	mediaType, params, ctErr := mime.ParseMediaType(ct)
	if ctErr != nil {
		return nil, nil, unsupportedMediaTypeError{}
	}
	mediaType = strings.ToLower(mediaType)
	switch mediaType {
	case "multipart/form-data":
		boundary := params["boundary"]
		if strings.TrimSpace(boundary) == "" {
			return nil, nil, unsupportedMediaTypeError{}
		}
		mr := multipart.NewReader(bytes.NewReader(rawBody), boundary)
		form, readErr := mr.ReadForm(int64(len(rawBody)) + 1024)
		if readErr != nil {
			if form != nil {
				_ = form.RemoveAll()
			}
			if errors.Is(readErr, multipart.ErrMessageTooLarge) {
				return nil, nil, piHTTP.ErrorPayloadTooLarge{}
			}
			return nil, nil, newInvalidArgumentError("invalid request body", nil)
		}
		if rawReq.MultipartForm == nil {
			rawReq.MultipartForm = form
		}
		return url.Values(form.Value), form.File, nil
	case "application/x-www-form-urlencoded":
		if hasFileFields {
			return nil, nil, unsupportedMediaTypeError{}
		}
		vals, parseErr := url.ParseQuery(string(rawBody))
		if parseErr != nil {
			return nil, nil, newInvalidArgumentError("invalid request body", nil)
		}
		return vals, nil, nil
	default:
		return nil, nil, unsupportedMediaTypeError{}
	}
}

func bindFormFields(op *CompiledOperation, reqVal reflect.Value, formValues url.Values, formFiles map[string][]*multipart.FileHeader, violations *[]fieldViolation) {
	for _, cf := range op.formFields {
		files := formFiles[cf.externalName]
		vals, exists := formValues[cf.externalName]
		if cf.isFile {
			hasNonEmptyVal := false
			for _, v := range vals {
				if v != "" {
					hasNonEmptyVal = true
					break
				}
			}
			if hasNonEmptyVal {
				*violations = append(*violations, fieldViolation{
					Field: cf.errField,
					Rule:  "type",
				})
				continue
			}
			if cf.fileKind == fileSingle && len(files) > 0 && len(vals) > 0 {
				*violations = append(*violations, fieldViolation{
					Field: cf.errField,
					Rule:  "single_value",
				})
				continue
			}
			if len(files) == 0 {
				continue
			}
			if cf.fileKind == fileSingle && len(files) > 1 {
				*violations = append(*violations, fieldViolation{
					Field: cf.errField,
					Rule:  "single_value",
				})
				continue
			}
			badFilename := false
			for _, fh := range files {
				if !isSafeUploadFileHeader(fh) {
					*violations = append(*violations, fieldViolation{
						Field: cf.errField,
						Rule:  "format",
					})
					badFilename = true
					break
				}
			}
			if badFilename {
				continue
			}
			if cf.fileKind == fileSingle {
				reqVal.FieldByIndex(cf.indexChain).Set(reflect.ValueOf(files[0]))
			} else {
				sliceVal := reflect.MakeSlice(cf.fieldType, len(files), len(files))
				for idx, fh := range files {
					sliceVal.Index(idx).Set(reflect.ValueOf(fh))
				}
				reqVal.FieldByIndex(cf.indexChain).Set(sliceVal)
			}
			continue
		}

		if len(files) > 0 {
			*violations = append(*violations, fieldViolation{
				Field: cf.errField,
				Rule:  "type",
			})
			continue
		}
		if !exists {
			if cf.hasDefault {
				setScalarField(reqVal.FieldByIndex(cf.indexChain), cf.defaultVal, cf.scalar.isPtr)
			}
			continue
		}
		if cf.isSlice {
			sliceVal := reflect.MakeSlice(cf.fieldType, len(vals), len(vals))
			sliceOK := true
			for idx, item := range vals {
				elemRV, err := parseScalarReflectValue(item, cf.scalar)
				if err != nil {
					*violations = append(*violations, fieldViolation{
						Field: fmt.Sprintf("%s[%d]", cf.errField, idx),
						Rule:  "type",
					})
					sliceOK = false
					continue
				}
				sliceVal.Index(idx).Set(elemRV)
			}
			if sliceOK {
				reqVal.FieldByIndex(cf.indexChain).Set(sliceVal)
			}
			continue
		}
		if len(vals) > 1 {
			*violations = append(*violations, fieldViolation{
				Field: cf.errField,
				Rule:  "single_value",
			})
			continue
		}
		if len(vals) == 1 {
			parsedRV, err := parseScalarReflectValue(vals[0], cf.scalar)
			if err != nil {
				*violations = append(*violations, fieldViolation{
					Field: cf.errField,
					Rule:  "type",
				})
				continue
			}
			setScalarField(reqVal.FieldByIndex(cf.indexChain), parsedRV, cf.scalar.isPtr)
		}
	}

	if op.StrictJSON {
		for key := range formValues {
			if _, known := op.formByName[key]; !known {
				*violations = append(*violations, fieldViolation{
					Field: key,
					Rule:  "unknown_field",
				})
			}
		}
		for key := range formFiles {
			if _, known := op.formByName[key]; !known {
				*violations = append(*violations, fieldViolation{
					Field: key,
					Rule:  "unknown_field",
				})
			}
		}
	}
}

func isSafeUploadFileHeader(fh *multipart.FileHeader) bool {
	if fh == nil || !isSafeUploadFilename(fh.Filename) {
		return false
	}
	if cd := fh.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if rawName, ok := params["filename"]; ok && !isSafeUploadFilename(rawName) {
				return false
			}
		}
	}
	return true
}

func isSafeUploadFilename(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	return !strings.ContainsAny(name, "/\\\x00")
}

func isNilResponseValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}

func isJSONContentType(ct string) bool {
	if strings.TrimSpace(ct) == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	mediaType = strings.ToLower(mediaType)
	if mediaType == "application/json" {
		return true
	}
	return strings.HasPrefix(mediaType, "application/") &&
		strings.HasSuffix(mediaType, "+json") &&
		len(mediaType) > len("application/+json")
}

func (op *CompiledOperation) validateEmptyBodyContract(rawReq *http.Request, rawBody []byte) error {
	trimmed := bytes.TrimSpace(rawBody)
	if len(trimmed) == 0 {
		return nil
	}
	if isBodylessMethod(op.Method) {
		return newInvalidArgumentError("invalid request body", nil)
	}
	if ct := rawReq.Header.Get("Content-Type"); (op.Method == http.MethodPost || op.Method == http.MethodPut || op.Method == http.MethodPatch) &&
		strings.TrimSpace(ct) != "" &&
		!isJSONContentType(ct) {
		return unsupportedMediaTypeError{}
	}
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	tok, err := dec.Token()
	if err != nil {
		return newInvalidArgumentError("invalid request body", nil)
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return newInvalidArgumentError("invalid request body", nil)
	}
	if dec.More() {
		return newInvalidArgumentError("invalid request body", nil)
	}
	endTok, err := dec.Token()
	if err != nil {
		return newInvalidArgumentError("invalid request body", nil)
	}
	if endDelim, ok := endTok.(json.Delim); !ok || endDelim != '}' {
		return newInvalidArgumentError("invalid request body", nil)
	}
	if _, err := dec.Token(); err != io.EOF {
		return newInvalidArgumentError("invalid request body", nil)
	}
	return nil
}

func lookupChiURLParam(rctx *chi.Context, key string) (string, bool) {
	if rctx == nil {
		return "", false
	}
	for i := len(rctx.URLParams.Keys) - 1; i >= 0; i-- {
		if rctx.URLParams.Keys[i] == key {
			if i < len(rctx.URLParams.Values) {
				return rctx.URLParams.Values[i], true
			}
			return "", false
		}
	}
	return "", false
}

func lookupHeaderValues(h http.Header, name string) []string {
	if len(h) == 0 {
		return nil
	}
	canon := http.CanonicalHeaderKey(name)
	var out []string
	for k, vals := range h {
		if k == canon || strings.EqualFold(k, name) {
			out = append(out, vals...)
		}
	}
	return out
}

func setScalarField(dst reflect.Value, val reflect.Value, isPtr bool) {
	if !isPtr {
		dst.Set(val)
		return
	}
	ptr := reflect.New(val.Type())
	ptr.Elem().Set(val)
	dst.Set(ptr)
}

func applyTopLevelBodyDefaults(structVal reflect.Value, plan *jsonStructPlan) {
	if plan == nil {
		return
	}
	for _, fp := range plan.fields {
		if !fp.hasDefault {
			continue
		}
		setScalarField(structVal.FieldByIndex(fp.indexChain), fp.defaultVal, fp.typeSpec.ptrDepth > 0)
	}
}

func decodeJSONBody(rawBody []byte, reqVal reflect.Value, plan *jsonStructPlan, strict bool, violations *[]fieldViolation) error {
	dec := json.NewDecoder(bytes.NewReader(rawBody))
	dec.UseNumber()

	tok, err := dec.Token()
	if err != nil {
		return newInvalidArgumentError("invalid request body", nil)
	}
	delim, ok := tok.(json.Delim)
	if !ok || delim != '{' {
		return newInvalidArgumentError("invalid request body", nil)
	}

	if err := decodeJSONObject(dec, reqVal, plan, "", strict, violations); err != nil {
		return err
	}

	if _, err := dec.Token(); err != io.EOF {
		return newInvalidArgumentError("invalid request body", nil)
	}
	return nil
}

func decodeJSONObject(
	dec *json.Decoder,
	structVal reflect.Value,
	plan *jsonStructPlan,
	errPrefix string,
	strict bool,
	violations *[]fieldViolation,
) error {
	seenKeys := make(map[string]struct{})
	for dec.More() {
		kTok, err := dec.Token()
		if err != nil {
			return newInvalidArgumentError("invalid request body", nil)
		}
		key, ok := kTok.(string)
		if !ok {
			return newInvalidArgumentError("invalid request body", nil)
		}

		fieldPath := joinJSONErrPath(errPrefix, key)
		if _, dup := seenKeys[key]; dup {
			return newInvalidArgumentError("invalid request body", []fieldViolation{{
				Field: fieldPath,
				Rule:  "duplicate_key",
			}})
		}
		seenKeys[key] = struct{}{}

		fp, known := plan.byJSONName[key]
		if !known {
			if err := skipAndValidateJSONValue(dec, fieldPath); err != nil {
				return err
			}
			if strict {
				*violations = append(*violations, fieldViolation{
					Field: fieldPath,
					Rule:  "unknown_field",
				})
			}
			continue
		}

		valTok, err := dec.Token()
		if err != nil {
			return newInvalidArgumentError("invalid request body", nil)
		}
		errPath := fp.errField
		if errPrefix != "" {
			errPath = joinJSONErrPath(errPrefix, fp.jsonName)
		}
		if err := decodeJSONTokenValue(valTok, dec, structVal.FieldByIndex(fp.indexChain), fp.typeSpec, errPath, strict, violations); err != nil {
			return err
		}
	}

	endTok, err := dec.Token()
	if err != nil {
		return newInvalidArgumentError("invalid request body", nil)
	}
	if endDelim, ok := endTok.(json.Delim); !ok || endDelim != '}' {
		return newInvalidArgumentError("invalid request body", nil)
	}

	for _, fp := range plan.fields {
		if !fp.hasDefault {
			continue
		}
		if _, seen := seenKeys[fp.jsonName]; seen {
			continue
		}
		setScalarField(structVal.FieldByIndex(fp.indexChain), fp.defaultVal, fp.typeSpec.ptrDepth > 0)
	}
	return nil
}

func decodeJSONTokenValue(
	tok any,
	dec *json.Decoder,
	dst reflect.Value,
	spec *jsonTypeSpec,
	errPath string,
	strict bool,
	violations *[]fieldViolation,
) error {
	if tok == nil {
		if dst.Kind() == reflect.Ptr {
			return nil
		}
		if spec.kind == jsonRawMessage {
			dst.SetBytes([]byte("null"))
			return nil
		}
		if spec.kind == jsonAny {
			return nil
		}
		*violations = append(*violations, fieldViolation{
			Field: errPath,
			Rule:  "type",
		})
		return nil
	}

	for dst.Kind() == reflect.Ptr {
		ptr := reflect.New(dst.Type().Elem())
		dst.Set(ptr)
		dst = ptr.Elem()
	}

	switch spec.kind {
	case jsonRawMessage:
		raw, err := captureJSONValue(tok, dec, errPath)
		if err != nil {
			return err
		}
		dst.SetBytes(raw)
		return nil

	case jsonAny:
		val, err := decodeAnyJSONValue(tok, dec, errPath)
		if err != nil {
			return err
		}
		if val != nil {
			dst.Set(reflect.ValueOf(val))
		}
		return nil

	case jsonScalar:
		return decodeJSONScalarToken(tok, dec, dst, spec, errPath, violations)

	case jsonBytes:
		s, ok := tok.(string)
		if !ok {
			if err := skipRemainingDelim(tok, dec, errPath); err != nil {
				return err
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		decoded, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "format"})
			return nil
		}
		dst.SetBytes(decoded)
		return nil

	case jsonSlice:
		delim, ok := tok.(json.Delim)
		if !ok || delim != '[' {
			if err := skipRemainingDelim(tok, dec, errPath); err != nil {
				return err
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		sliceVal := reflect.MakeSlice(dst.Type(), 0, 0)
		idx := 0
		for dec.More() {
			elemTok, err := dec.Token()
			if err != nil {
				return newInvalidArgumentError("invalid request body", nil)
			}
			elemVal := reflect.New(dst.Type().Elem()).Elem()
			elemPath := fmt.Sprintf("%s[%d]", errPath, idx)
			if err := decodeJSONTokenValue(elemTok, dec, elemVal, spec.elemSpec, elemPath, strict, violations); err != nil {
				return err
			}
			sliceVal = reflect.Append(sliceVal, elemVal)
			idx++
		}
		endTok, err := dec.Token()
		if err != nil {
			return newInvalidArgumentError("invalid request body", nil)
		}
		if endDelim, ok := endTok.(json.Delim); !ok || endDelim != ']' {
			return newInvalidArgumentError("invalid request body", nil)
		}
		dst.Set(sliceVal)
		return nil

	case jsonArray:
		delim, ok := tok.(json.Delim)
		if !ok || delim != '[' {
			if err := skipRemainingDelim(tok, dec, errPath); err != nil {
				return err
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		idx := 0
		for dec.More() {
			elemTok, err := dec.Token()
			if err != nil {
				return newInvalidArgumentError("invalid request body", nil)
			}
			elemPath := fmt.Sprintf("%s[%d]", errPath, idx)
			if idx < dst.Len() {
				if err := decodeJSONTokenValue(elemTok, dec, dst.Index(idx), spec.elemSpec, elemPath, strict, violations); err != nil {
					return err
				}
			} else {
				if err := skipRemainingDelim(elemTok, dec, elemPath); err != nil {
					return err
				}
			}
			idx++
		}
		endTok, err := dec.Token()
		if err != nil {
			return newInvalidArgumentError("invalid request body", nil)
		}
		if endDelim, ok := endTok.(json.Delim); !ok || endDelim != ']' {
			return newInvalidArgumentError("invalid request body", nil)
		}
		if idx != dst.Len() {
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "len"})
		}
		return nil

	case jsonMap:
		delim, ok := tok.(json.Delim)
		if !ok || delim != '{' {
			if err := skipRemainingDelim(tok, dec, errPath); err != nil {
				return err
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		m := reflect.MakeMap(dst.Type())
		seenKeys := make(map[string]struct{})
		for dec.More() {
			kTok, err := dec.Token()
			if err != nil {
				return newInvalidArgumentError("invalid request body", nil)
			}
			keyStr, ok := kTok.(string)
			if !ok {
				return newInvalidArgumentError("invalid request body", nil)
			}
			entryPath := joinJSONErrPath(errPath, keyStr)
			if _, dup := seenKeys[keyStr]; dup {
				return newInvalidArgumentError("invalid request body", []fieldViolation{{
					Field: entryPath,
					Rule:  "duplicate_key",
				}})
			}
			seenKeys[keyStr] = struct{}{}

			valTok, err := dec.Token()
			if err != nil {
				return newInvalidArgumentError("invalid request body", nil)
			}
			keyVal, err := parseScalarReflectValue(keyStr, spec.mapKeySpec)
			if err != nil {
				if err := skipRemainingDelim(valTok, dec, entryPath); err != nil {
					return err
				}
				*violations = append(*violations, fieldViolation{Field: entryPath, Rule: "type"})
				continue
			}
			elemVal := reflect.New(dst.Type().Elem()).Elem()
			if err := decodeJSONTokenValue(valTok, dec, elemVal, spec.elemSpec, entryPath, strict, violations); err != nil {
				return err
			}
			m.SetMapIndex(keyVal, elemVal)
		}
		endTok, err := dec.Token()
		if err != nil {
			return newInvalidArgumentError("invalid request body", nil)
		}
		if endDelim, ok := endTok.(json.Delim); !ok || endDelim != '}' {
			return newInvalidArgumentError("invalid request body", nil)
		}
		dst.Set(m)
		return nil

	case jsonStruct:
		delim, ok := tok.(json.Delim)
		if !ok || delim != '{' {
			if err := skipRemainingDelim(tok, dec, errPath); err != nil {
				return err
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		return decodeJSONObject(dec, dst, spec.structPlan, errPath, strict, violations)

	default:
		if err := skipRemainingDelim(tok, dec, errPath); err != nil {
			return err
		}
		*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
		return nil
	}
}

func decodeJSONScalarToken(
	tok any,
	dec *json.Decoder,
	dst reflect.Value,
	spec *jsonTypeSpec,
	errPath string,
	violations *[]fieldViolation,
) error {
	if spec.isJSONString {
		rawStr, ok := tok.(string)
		if !ok {
			if err := skipRemainingDelim(tok, dec, errPath); err != nil {
				return err
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		switch spec.scalar.kind {
		case scalarString:
			var inner string
			if err := json.Unmarshal([]byte(rawStr), &inner); err != nil {
				*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
				return nil
			}
			dst.SetString(inner)
			return nil
		case scalarBool:
			if rawStr == "true" {
				dst.SetBool(true)
				return nil
			}
			if rawStr == "false" {
				dst.SetBool(false)
				return nil
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		case scalarInt:
			n, err := strconv.ParseInt(rawStr, 10, spec.scalar.bitSize)
			if err != nil {
				*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
				return nil
			}
			dst.SetInt(n)
			return nil
		case scalarUint:
			u, err := strconv.ParseUint(rawStr, 10, spec.scalar.bitSize)
			if err != nil {
				*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
				return nil
			}
			dst.SetUint(u)
			return nil
		case scalarFloat:
			f, err := strconv.ParseFloat(rawStr, spec.scalar.bitSize)
			if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
				*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
				return nil
			}
			dst.SetFloat(f)
			return nil
		default:
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
	}

	switch spec.scalar.kind {
	case scalarString:
		s, ok := tok.(string)
		if !ok {
			if err := skipRemainingDelim(tok, dec, errPath); err != nil {
				return err
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		dst.SetString(s)
		return nil

	case scalarBool:
		b, ok := tok.(bool)
		if !ok {
			if err := skipRemainingDelim(tok, dec, errPath); err != nil {
				return err
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		dst.SetBool(b)
		return nil

	case scalarInt:
		num, ok := tok.(json.Number)
		if !ok {
			if err := skipRemainingDelim(tok, dec, errPath); err != nil {
				return err
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		n, err := strconv.ParseInt(num.String(), 10, spec.scalar.bitSize)
		if err != nil {
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		dst.SetInt(n)
		return nil

	case scalarUint:
		num, ok := tok.(json.Number)
		if !ok {
			if err := skipRemainingDelim(tok, dec, errPath); err != nil {
				return err
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		u, err := strconv.ParseUint(num.String(), 10, spec.scalar.bitSize)
		if err != nil {
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		dst.SetUint(u)
		return nil

	case scalarFloat:
		num, ok := tok.(json.Number)
		if !ok {
			if err := skipRemainingDelim(tok, dec, errPath); err != nil {
				return err
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		f, err := strconv.ParseFloat(num.String(), spec.scalar.bitSize)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		dst.SetFloat(f)
		return nil

	case scalarTime:
		s, ok := tok.(string)
		if !ok {
			if err := skipRemainingDelim(tok, dec, errPath); err != nil {
				return err
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		tm, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "format"})
			return nil
		}
		dst.Set(reflect.ValueOf(tm))
		return nil

	case scalarUUID:
		s, ok := tok.(string)
		if !ok {
			if err := skipRemainingDelim(tok, dec, errPath); err != nil {
				return err
			}
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
			return nil
		}
		u, err := uuid.Parse(s)
		if err != nil {
			*violations = append(*violations, fieldViolation{Field: errPath, Rule: "format"})
			return nil
		}
		dst.Set(reflect.ValueOf(u))
		return nil

	default:
		if err := skipRemainingDelim(tok, dec, errPath); err != nil {
			return err
		}
		*violations = append(*violations, fieldViolation{Field: errPath, Rule: "type"})
		return nil
	}
}

func skipAndValidateJSONValue(dec *json.Decoder, errPath string) error {
	tok, err := dec.Token()
	if err != nil {
		return newInvalidArgumentError("invalid request body", nil)
	}
	return skipRemainingDelim(tok, dec, errPath)
}

func skipRemainingDelim(tok any, dec *json.Decoder, errPath string) error {
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for dec.More() {
			kTok, err := dec.Token()
			if err != nil {
				return newInvalidArgumentError("invalid request body", nil)
			}
			k, ok := kTok.(string)
			if !ok {
				return newInvalidArgumentError("invalid request body", nil)
			}
			childPath := joinJSONErrPath(errPath, k)
			if _, dup := seen[k]; dup {
				return newInvalidArgumentError("invalid request body", []fieldViolation{{
					Field: childPath,
					Rule:  "duplicate_key",
				}})
			}
			seen[k] = struct{}{}
			if err := skipAndValidateJSONValue(dec, childPath); err != nil {
				return err
			}
		}
		endTok, err := dec.Token()
		if err != nil {
			return newInvalidArgumentError("invalid request body", nil)
		}
		if endDelim, ok := endTok.(json.Delim); !ok || endDelim != '}' {
			return newInvalidArgumentError("invalid request body", nil)
		}
	case '[':
		idx := 0
		for dec.More() {
			elemPath := fmt.Sprintf("%s[%d]", errPath, idx)
			if err := skipAndValidateJSONValue(dec, elemPath); err != nil {
				return err
			}
			idx++
		}
		endTok, err := dec.Token()
		if err != nil {
			return newInvalidArgumentError("invalid request body", nil)
		}
		if endDelim, ok := endTok.(json.Delim); !ok || endDelim != ']' {
			return newInvalidArgumentError("invalid request body", nil)
		}
	}
	return nil
}

func captureJSONValue(tok any, dec *json.Decoder, errPath string) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeCapturedJSONValue(&buf, tok, dec, errPath); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCapturedJSONValue(buf *bytes.Buffer, tok any, dec *json.Decoder, errPath string) error {
	switch v := tok.(type) {
	case nil:
		buf.WriteString("null")
		return nil
	case bool:
		if v {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
		return nil
	case json.Number:
		buf.WriteString(v.String())
		return nil
	case string:
		encoded, err := json.Marshal(v)
		if err != nil {
			return newInvalidArgumentError("invalid request body", nil)
		}
		buf.Write(encoded)
		return nil
	case json.Delim:
		switch v {
		case '{':
			buf.WriteByte('{')
			seen := make(map[string]struct{})
			first := true
			for dec.More() {
				kTok, err := dec.Token()
				if err != nil {
					return newInvalidArgumentError("invalid request body", nil)
				}
				k, ok := kTok.(string)
				if !ok {
					return newInvalidArgumentError("invalid request body", nil)
				}
				childPath := joinJSONErrPath(errPath, k)
				if _, dup := seen[k]; dup {
					return newInvalidArgumentError("invalid request body", []fieldViolation{{
						Field: childPath,
						Rule:  "duplicate_key",
					}})
				}
				seen[k] = struct{}{}

				valTok, err := dec.Token()
				if err != nil {
					return newInvalidArgumentError("invalid request body", nil)
				}
				if !first {
					buf.WriteByte(',')
				}
				first = false
				encodedKey, err := json.Marshal(k)
				if err != nil {
					return newInvalidArgumentError("invalid request body", nil)
				}
				buf.Write(encodedKey)
				buf.WriteByte(':')
				if err := writeCapturedJSONValue(buf, valTok, dec, childPath); err != nil {
					return err
				}
			}
			endTok, err := dec.Token()
			if err != nil {
				return newInvalidArgumentError("invalid request body", nil)
			}
			if endDelim, ok := endTok.(json.Delim); !ok || endDelim != '}' {
				return newInvalidArgumentError("invalid request body", nil)
			}
			buf.WriteByte('}')
			return nil
		case '[':
			buf.WriteByte('[')
			first := true
			idx := 0
			for dec.More() {
				elemTok, err := dec.Token()
				if err != nil {
					return newInvalidArgumentError("invalid request body", nil)
				}
				if !first {
					buf.WriteByte(',')
				}
				first = false
				elemPath := fmt.Sprintf("%s[%d]", errPath, idx)
				if err := writeCapturedJSONValue(buf, elemTok, dec, elemPath); err != nil {
					return err
				}
				idx++
			}
			endTok, err := dec.Token()
			if err != nil {
				return newInvalidArgumentError("invalid request body", nil)
			}
			if endDelim, ok := endTok.(json.Delim); !ok || endDelim != ']' {
				return newInvalidArgumentError("invalid request body", nil)
			}
			buf.WriteByte(']')
			return nil
		}
	}
	return newInvalidArgumentError("invalid request body", nil)
}

func decodeAnyJSONValue(tok any, dec *json.Decoder, errPath string) (any, error) {
	switch v := tok.(type) {
	case nil:
		return nil, nil
	case bool, string:
		return v, nil
	case json.Number:
		f, err := v.Float64()
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return v.String(), nil
		}
		return f, nil
	case json.Delim:
		switch v {
		case '{':
			m := make(map[string]any)
			for dec.More() {
				kTok, err := dec.Token()
				if err != nil {
					return nil, newInvalidArgumentError("invalid request body", nil)
				}
				k, ok := kTok.(string)
				if !ok {
					return nil, newInvalidArgumentError("invalid request body", nil)
				}
				childPath := joinJSONErrPath(errPath, k)
				if _, dup := m[k]; dup {
					return nil, newInvalidArgumentError("invalid request body", []fieldViolation{{
						Field: childPath,
						Rule:  "duplicate_key",
					}})
				}
				valTok, err := dec.Token()
				if err != nil {
					return nil, newInvalidArgumentError("invalid request body", nil)
				}
				val, err := decodeAnyJSONValue(valTok, dec, childPath)
				if err != nil {
					return nil, err
				}
				m[k] = val
			}
			endTok, err := dec.Token()
			if err != nil {
				return nil, newInvalidArgumentError("invalid request body", nil)
			}
			if endDelim, ok := endTok.(json.Delim); !ok || endDelim != '}' {
				return nil, newInvalidArgumentError("invalid request body", nil)
			}
			return m, nil
		case '[':
			arr := make([]any, 0)
			idx := 0
			for dec.More() {
				elemTok, err := dec.Token()
				if err != nil {
					return nil, newInvalidArgumentError("invalid request body", nil)
				}
				val, err := decodeAnyJSONValue(elemTok, dec, fmt.Sprintf("%s[%d]", errPath, idx))
				if err != nil {
					return nil, err
				}
				arr = append(arr, val)
				idx++
			}
			endTok, err := dec.Token()
			if err != nil {
				return nil, newInvalidArgumentError("invalid request body", nil)
			}
			if endDelim, ok := endTok.(json.Delim); !ok || endDelim != ']' {
				return nil, newInvalidArgumentError("invalid request body", nil)
			}
			return arr, nil
		}
	}
	return nil, newInvalidArgumentError("invalid request body", nil)
}

func joinJSONErrPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}
