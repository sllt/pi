package http

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	resTypes "github.com/sllt/pi/pkg/pi/http/response"
)

var errTest = fmt.Errorf("internal server error")

func TestResponder(t *testing.T) {
	tests := []struct {
		desc         string
		data         any
		contentType  string
		expectedBody []byte
	}{
		{
			desc: "xml response type default content type",
			data: resTypes.XML{
				Content: []byte(`<Response status="ok"><Message>Hello</Message></Response>`),
			},
			contentType:  "application/xml",
			expectedBody: []byte(`<Response status="ok"><Message>Hello</Message></Response>`),
		},
		{
			desc: "xml response type custom content type",
			data: resTypes.XML{
				Content:     []byte(`<soapenv:Envelope></soapenv:Envelope>`),
				ContentType: "application/soap+xml",
			},
			contentType:  "application/soap+xml",
			expectedBody: []byte(`<soapenv:Envelope></soapenv:Envelope>`),
		},
		{
			desc:         "raw response type",
			data:         resTypes.Raw{Data: []byte("raw data")},
			contentType:  "application/json",
			expectedBody: []byte(`"cmF3IGRhdGE="`),
		},
		{
			desc: "file response type",
			data: resTypes.File{
				ContentType: "image/png",
			},
			contentType:  "image/png",
			expectedBody: nil,
		},
		{
			desc:         "map response type",
			data:         map[string]string{"key": "value"},
			contentType:  "application/json",
			expectedBody: []byte(`{"code":0,"data":{"key":"value"},"message":"ok"}`),
		},
		{
			desc: "pi response type with meta",
			data: resTypes.Response{
				Data: "Hello World from new Server",
				Meta: map[string]any{
					"environment": "stage",
				},
			},
			contentType:  "application/json",
			expectedBody: []byte(`{"code":0,"data":"Hello World from new Server","message":"ok","meta":{"environment":"stage"}}`),
		},
		{
			desc: "pi response type without meta",
			data: resTypes.Response{
				Data: "Hello World from new Server",
			},
			contentType:  "application/json",
			expectedBody: []byte(`{"code":0,"data":"Hello World from new Server","message":"ok"}`),
		},
	}

	for i, tc := range tests {
		recorder := httptest.NewRecorder()
		recorder.Body.Reset()
		r := NewResponder(recorder, http.MethodGet)

		r.Respond(tc.data, nil)

		contentType := recorder.Header().Get("Content-Type")
		assert.Equal(t, tc.contentType, contentType, "TEST[%d] Failed: %s", i, tc.desc)

		responseBody := recorder.Body.Bytes()

		expected := bytes.TrimSpace(tc.expectedBody)

		actual := bytes.TrimSpace(responseBody)

		assert.Equal(t, expected, actual, "TEST[%d] Failed: %s", i, tc.desc)
	}
}

func TestResponder_buildResponse(t *testing.T) {
	tests := []struct {
		desc     string
		data     any
		meta     map[string]any
		err      error
		expected response
	}{
		{
			desc: "success case with data and meta",
			data: map[string]string{"key": "value"},
			meta: map[string]any{"page": 1},
			err:  nil,
			expected: response{
				Code:    0,
				Data:    map[string]string{"key": "value"},
				Message: "ok",
				Meta:    map[string]any{"page": 1},
			},
		},
		{
			desc: "success case with data no meta",
			data: "success response",
			meta: nil,
			err:  nil,
			expected: response{
				Code:    0,
				Data:    "success response",
				Message: "ok",
				Meta:    nil,
			},
		},
		{
			desc: "error case with StatusCodeResponder",
			data: nil,
			meta: nil,
			err:  ErrorInvalidRoute{},
			expected: response{
				Code:    http.StatusNotFound,
				Data:    nil,
				Message: "route not registered",
			},
		},
		{
			desc: "error case with unknown error",
			data: nil,
			meta: nil,
			err:  errTest,
			expected: response{
				Code:    -1,
				Data:    nil,
				Message: "internal server error",
			},
		},
		{
			desc: "error with empty struct as data",
			data: struct{}{},
			meta: nil,
			err:  errTest,
			expected: response{
				Code:    -1,
				Data:    nil,
				Message: "internal server error",
			},
		},
	}

	for i, tc := range tests {
		recorder := httptest.NewRecorder()
		r := Responder{w: recorder, method: http.MethodGet}

		result := r.buildResponse(tc.data, tc.meta, tc.err)

		assert.Equal(t, tc.expected.Code, result.Code, "TEST[%d], Failed.\n%s", i, tc.desc)
		assert.Equal(t, tc.expected.Message, result.Message, "TEST[%d], Failed.\n%s", i, tc.desc)
		assert.Equal(t, tc.expected.Data, result.Data, "TEST[%d], Failed.\n%s", i, tc.desc)
		assert.Equal(t, tc.expected.Meta, result.Meta, "TEST[%d], Failed.\n%s", i, tc.desc)
	}
}

func TestGetErrorCode(t *testing.T) {
	tests := []struct {
		desc     string
		err      error
		expected int
	}{
		{"StatusCodeResponder error", ErrorInvalidRoute{}, http.StatusNotFound},
		{"StatusCodeResponder - 499", ErrorClientClosedRequest{}, StatusClientClosedRequest},
		{"StatusCodeResponder - timeout", ErrorRequestTimeout{}, http.StatusRequestTimeout},
		{"unknown error", errTest, -1},
		{"standard error", fmt.Errorf("some error"), -1},
	}

	for i, tc := range tests {
		code := getErrorCode(tc.err)
		assert.Equal(t, tc.expected, code, "TEST[%d], Failed.\n%s", i, tc.desc)
	}
}

func TestResponder_getHTTPStatusCode(t *testing.T) {
	tests := []struct {
		desc       string
		method     string
		data       any
		err        error
		statusCode int
	}{
		{"success GET", http.MethodGet, "success response", nil, http.StatusOK},
		{"success POST with body", http.MethodPost, "entity created", nil, http.StatusCreated},
		{"success POST nil body", http.MethodPost, nil, nil, http.StatusAccepted},
		{"success DELETE", http.MethodDelete, nil, nil, http.StatusNoContent},
		{"error invalid route", http.MethodGet, nil, ErrorInvalidRoute{}, http.StatusNotFound},
		{"error unknown", http.MethodGet, nil, errTest, http.StatusInternalServerError},
		{"error timeout", http.MethodGet, nil, ErrorRequestTimeout{}, http.StatusRequestTimeout},
		{"error client closed", http.MethodGet, nil, ErrorClientClosedRequest{}, StatusClientClosedRequest},
	}

	for i, tc := range tests {
		recorder := httptest.NewRecorder()
		r := Responder{w: recorder, method: tc.method}

		statusCode := r.getHTTPStatusCode(tc.data, tc.err)

		assert.Equal(t, tc.statusCode, statusCode, "TEST[%d], Failed.\n%s", i, tc.desc)
	}
}

type temp struct {
	ID string `json:"id,omitempty"`
}

// newNilTemp returns a nil pointer of type *temp for testing purposes.
func newNilTemp() *temp {
	return nil
}

// CustomBusinessError implements both StatusCodeResponder and CodeResponder.
// This demonstrates the priority: CodeResponder.Code() is used for the business error code.
type CustomBusinessError struct {
	BusinessCode int
	HTTPStatus   int
	Msg          string
}

func (e CustomBusinessError) Error() string   { return e.Msg }
func (e CustomBusinessError) StatusCode() int { return e.HTTPStatus }
func (e CustomBusinessError) Code() int       { return e.BusinessCode }

func TestRespondWithApplicationJSON(t *testing.T) {
	sampleData := map[string]string{"message": "Hello World"}
	sampleError := ErrorInvalidRoute{}

	tests := []struct {
		desc         string
		data         any
		err          error
		expectedCode int
		expectedBody string
	}{
		{"sample data response", sampleData, nil,
			http.StatusOK, `{"code":0,"data":{"message":"Hello World"},"message":"ok"}`},
		{"error response", nil, sampleError,
			http.StatusNotFound, `{"code":404,"data":null,"message":"route not registered"}`},
		{"error response contains a nullable type with a nil value", newNilTemp(), sampleError,
			http.StatusNotFound, `{"code":404,"data":null,"message":"route not registered"}`},
		{"client closed request", nil, ErrorClientClosedRequest{},
			StatusClientClosedRequest, `{"code":499,"data":null,"message":"client closed request"}`},
		{"server timeout error", nil, ErrorRequestTimeout{},
			http.StatusRequestTimeout, `{"code":408,"data":null,"message":"request timed out"}`},
	}

	for i, tc := range tests {
		recorder := httptest.NewRecorder()
		responder := Responder{w: recorder, method: http.MethodGet}

		responder.Respond(tc.data, tc.err)

		result := recorder.Result()

		assert.Equal(t, tc.expectedCode, result.StatusCode, "TEST[%d], Failed.\n%s", i, tc.desc)

		body := new(bytes.Buffer)
		_, err := body.ReadFrom(result.Body)

		result.Body.Close()

		require.NoError(t, err, "TEST[%d], Failed.\n%s", i, tc.desc)

		// json Encoder by default terminate each value with a newline
		tc.expectedBody += "\n"

		assert.Equal(t, tc.expectedBody, body.String(), "TEST[%d], Failed.\n%s", i, tc.desc)
	}
}

func TestIsNil(t *testing.T) {
	tests := []struct {
		desc     string
		value    any
		expected bool
	}{
		{"nil value", nil, true},
		{"nullable type with a nil value", newNilTemp(), true},
		{"not nil value", temp{ID: "test"}, false},
		{"chan type", make(chan int), false},
	}

	for i, tc := range tests {
		resp := isNil(tc.value)

		assert.Equal(t, tc.expected, resp, "TEST[%d], Failed.\n%s", i, tc.desc)
	}
}

func TestResponder_TemplateResponse(t *testing.T) {
	templatePath := "./templates/example.html"
	templateContent := `<html><head><title>{{.Title}}</title></head><body>{{.Body}}</body></html>`

	createTemplateFile(t, templatePath, templateContent)
	defer removeTemplateDir(t)

	recorder := httptest.NewRecorder()
	r := NewResponder(recorder, http.MethodGet)

	templateData := map[string]string{"Title": "Test Title", "Body": "Test Body"}
	expectedBody := "<html><head><title>Test Title</title></head><body>Test Body</body></html>"

	r.Respond(resTypes.Template{Name: "example.html", Data: templateData}, nil)

	contentType := recorder.Header().Get("Content-Type")
	responseBody := recorder.Body.String()

	assert.Equal(t, "text/html", contentType)
	assert.Equal(t, expectedBody, responseBody)
}

func TestIsEmptyStruct(t *testing.T) {
	tests := []struct {
		desc     string
		data     any
		expected bool
	}{
		{"nil value", nil, false},
		{"empty struct", struct{}{}, true},
		{"non-empty struct", struct{ ID int }{ID: 1}, false},
		{"nil pointer to struct", (*struct{})(nil), false},
		{"pointer to non-empty struct", &struct{ ID int }{ID: 1}, false},
		{"non-struct type", 42, false},
	}

	for i, tc := range tests {
		result := isEmptyStruct(tc.data)

		assert.Equal(t, tc.expected, result, "TEST[%d] Failed: %s", i, tc.desc)
	}
}

func TestCodeResponder(t *testing.T) {
	tests := []struct {
		desc         string
		err          error
		expectedCode int
		expectedHTTP int
		expectedBody string
	}{
		{
			desc: "CodeResponder with custom business code",
			err: CustomBusinessError{
				BusinessCode: 1001,
				HTTPStatus:   http.StatusBadRequest,
				Msg:          "invalid input",
			},
			expectedCode: 1001,
			expectedHTTP: http.StatusBadRequest,
			expectedBody: `{"code":1001,"data":null,"message":"invalid input"}`,
		},
		{
			desc: "CodeResponder priority over StatusCodeResponder",
			err: CustomBusinessError{
				BusinessCode: 5000,
				HTTPStatus:   http.StatusInternalServerError,
				Msg:          "custom error",
			},
			expectedCode: 5000,
			expectedHTTP: http.StatusInternalServerError,
			expectedBody: `{"code":5000,"data":null,"message":"internal server error"}`,
		},
	}

	for i, tc := range tests {
		// Test getErrorCode function
		code := getErrorCode(tc.err)
		assert.Equal(t, tc.expectedCode, code, "TEST[%d] Failed: %s", i, tc.desc)

		// Test full response
		recorder := httptest.NewRecorder()
		responder := NewResponder(recorder, http.MethodGet)

		responder.Respond(nil, tc.err)

		result := recorder.Result()
		defer result.Body.Close()

		assert.Equal(t, tc.expectedHTTP, result.StatusCode, "TEST[%d] Failed: %s", i, tc.desc)

		body := new(bytes.Buffer)
		_, err := body.ReadFrom(result.Body)
		require.NoError(t, err, "TEST[%d] Failed: %s", i, tc.desc)

		tc.expectedBody += "\n"
		assert.Equal(t, tc.expectedBody, body.String(), "TEST[%d] Failed: %s", i, tc.desc)
	}
}

func createTemplateFile(t *testing.T, path, content string) {
	t.Helper()

	err := os.MkdirAll("./templates", os.ModePerm)
	require.NoError(t, err)

	err = os.WriteFile(path, []byte(content), 0600)
	require.NoError(t, err)
}

func removeTemplateDir(t *testing.T) {
	t.Helper()

	err := os.RemoveAll("./templates")

	require.NoError(t, err)
}

func TestResponder_RedirectResponse_Post(t *testing.T) {
	recorder := httptest.NewRecorder()
	r := NewResponder(recorder, http.MethodPost)

	// Set up redirect with specific URL and status code
	redirectURL := "/new-location?from=start"
	statusCode := http.StatusSeeOther // 303

	redirect := resTypes.Redirect{URL: redirectURL}

	r.Respond(redirect, nil)

	assert.Equal(t, statusCode, recorder.Code, "Redirect should set the correct status code")
	assert.Equal(t, redirectURL, recorder.Header().Get("Location"),
		"Redirect should set the Location header")
	assert.Empty(t, recorder.Body.String(), "Redirect response should not have a body")
}

func TestResponder_RedirectResponse_Head(t *testing.T) {
	recorder := httptest.NewRecorder()
	r := NewResponder(recorder, http.MethodHead)

	// Set up redirect with specific URL and status code
	redirectURL := "/new-location?from=start"
	statusCode := http.StatusFound // 302

	redirect := resTypes.Redirect{URL: redirectURL}

	r.Respond(redirect, nil)

	assert.Equal(t, statusCode, recorder.Code, "Redirect should set the correct status code")
	assert.Equal(t, redirectURL, recorder.Header().Get("Location"),
		"Redirect should set the Location header")
	assert.Empty(t, recorder.Body.String(), "Redirect response should not have a body")
}

func TestResponder_ClientClosedRequestHandling(t *testing.T) {
	recorder := httptest.NewRecorder()
	responder := NewResponder(recorder, http.MethodGet)

	// ErrorClientClosedRequest should return 499 status code with proper response format
	responder.Respond(nil, ErrorClientClosedRequest{})

	assert.Equal(t, 499, recorder.Code)
	assert.JSONEq(t, `{"code":499,"data":null,"message":"client closed request"}`, recorder.Body.String())
}

func TestResponder_ContentTypePreservation(t *testing.T) {
	tests := []struct {
		desc              string
		presetContentType string
		expectedType      string
	}{
		{
			desc:              "preset content type should be preserved",
			presetContentType: "text/event-stream",
			expectedType:      "text/event-stream",
		},
		{
			desc:              "no preset content type - defaults to application/json",
			presetContentType: "",
			expectedType:      "application/json",
		},
	}

	for i, tc := range tests {
		recorder := httptest.NewRecorder()

		// Simulate SetCustomHeaders by manually setting Content-Type header before calling Respond
		if tc.presetContentType != "" {
			recorder.Header().Set("Content-Type", tc.presetContentType)
		}

		responder := NewResponder(recorder, http.MethodGet)
		responder.Respond("Test data", nil)

		contentType := recorder.Header().Get("Content-Type")

		assert.Equal(t, tc.expectedType, contentType, "TEST[%d] Failed: %s", i, tc.desc)
	}
}

// TestResponder_XMLFileTemplate_ErrorStatusCodes verifies that XML, File, and Template responses
// return appropriate error status codes when errors occur, not always 200 OK.
func TestResponder_XMLFileTemplate_ErrorStatusCodes(t *testing.T) {
	tests := []struct {
		desc         string
		data         any
		err          error
		expectedCode int
	}{
		{
			desc: "XML response with 404 error should return 404",
			data: resTypes.XML{
				Content: []byte(`<Response><Error>Not Found</Error></Response>`),
			},
			err:          ErrorEntityNotFound{Name: "id", Value: "123"},
			expectedCode: http.StatusNotFound,
		},
		{
			desc: "XML response with 500 error should return 500",
			data: resTypes.XML{
				Content: []byte(`<Response><Error>Internal Error</Error></Response>`),
			},
			err:          errTest,
			expectedCode: http.StatusInternalServerError,
		},
		{
			desc: "File response with 404 error should return 404",
			data: resTypes.File{
				ContentType: "image/png",
				Content:     []byte("fake image data"),
			},
			err:          ErrorEntityNotFound{Name: "file", Value: "test.png"},
			expectedCode: http.StatusNotFound,
		},
		{
			desc: "File response with 500 error should return 500",
			data: resTypes.File{
				ContentType: "application/pdf",
				Content:     []byte("fake pdf data"),
			},
			err:          errTest,
			expectedCode: http.StatusInternalServerError,
		},
		{
			desc: "XML response with no error should return 200",
			data: resTypes.XML{
				Content: []byte(`<Response><Status>OK</Status></Response>`),
			},
			err:          nil,
			expectedCode: http.StatusOK,
		},
		{
			desc: "File response with no error should return 200",
			data: resTypes.File{
				ContentType: "text/plain",
				Content:     []byte("file content"),
			},
			err:          nil,
			expectedCode: http.StatusOK,
		},
	}

	for i, tc := range tests {
		recorder := httptest.NewRecorder()
		r := NewResponder(recorder, http.MethodGet)

		r.Respond(tc.data, tc.err)

		assert.Equal(t, tc.expectedCode, recorder.Code, "TEST[%d] Failed: %s", i, tc.desc)
	}
}

func TestResponder_CustomStatusCode_Success(t *testing.T) {
	tests := []struct {
		desc         string
		data         any
		expectedCode int
	}{
		{
			desc: "raw response should use custom status code",
			data: resTypes.Raw{
				Data:       map[string]string{"status": "ok"},
				StatusCode: http.StatusOK,
			},
			expectedCode: http.StatusOK,
		},
		{
			desc: "xml response should use custom status code",
			data: resTypes.XML{
				Content:    []byte(`<Response><Status>OK</Status></Response>`),
				StatusCode: http.StatusOK,
			},
			expectedCode: http.StatusOK,
		},
		{
			desc: "file response should use custom status code",
			data: resTypes.File{
				ContentType: "text/plain",
				Content:     []byte("ok"),
				StatusCode:  http.StatusOK,
			},
			expectedCode: http.StatusOK,
		},
	}

	for i, tc := range tests {
		recorder := httptest.NewRecorder()
		r := NewResponder(recorder, http.MethodPost)

		r.Respond(tc.data, nil)

		assert.Equal(t, tc.expectedCode, recorder.Code, "TEST[%d] Failed: %s", i, tc.desc)
	}
}

func TestResponder_CustomStatusCode_InvalidFallback(t *testing.T) {
	tests := []struct {
		desc         string
		data         any
		expectedCode int
	}{
		{
			desc: "raw response should fall back to default status when custom code is invalid",
			data: resTypes.Raw{
				Data:       map[string]string{"status": "ok"},
				StatusCode: 99,
			},
			expectedCode: http.StatusCreated,
		},
		{
			desc: "xml response should fall back to default status when custom code is invalid",
			data: resTypes.XML{
				Content:    []byte(`<Response><Status>OK</Status></Response>`),
				StatusCode: 99,
			},
			expectedCode: http.StatusCreated,
		},
		{
			desc: "file response should fall back to default status when custom code is invalid",
			data: resTypes.File{
				ContentType: "text/plain",
				Content:     []byte("ok"),
				StatusCode:  99,
			},
			expectedCode: http.StatusCreated,
		},
	}

	for i, tc := range tests {
		recorder := httptest.NewRecorder()
		r := NewResponder(recorder, http.MethodPost)

		r.Respond(tc.data, nil)

		assert.Equal(t, tc.expectedCode, recorder.Code, "TEST[%d] Failed: %s", i, tc.desc)
	}
}

func TestResponder_CustomStatusCode_IgnoredWhenError(t *testing.T) {
	tests := []struct {
		desc         string
		data         any
		err          error
		expectedCode int
	}{
		{
			desc: "raw response should still use error status code",
			data: resTypes.Raw{
				Data:       map[string]string{"status": "error"},
				StatusCode: http.StatusOK,
			},
			err:          ErrorEntityNotFound{Name: "id", Value: "123"},
			expectedCode: http.StatusNotFound,
		},
		{
			desc: "xml response should still use error status code",
			data: resTypes.XML{
				Content:    []byte(`<Response><Error>Not Found</Error></Response>`),
				StatusCode: http.StatusOK,
			},
			err:          ErrorEntityNotFound{Name: "id", Value: "123"},
			expectedCode: http.StatusNotFound,
		},
		{
			desc: "file response should still use error status code",
			data: resTypes.File{
				ContentType: "text/plain",
				Content:     []byte("error"),
				StatusCode:  http.StatusOK,
			},
			err:          ErrorEntityNotFound{Name: "id", Value: "123"},
			expectedCode: http.StatusNotFound,
		},
	}

	for i, tc := range tests {
		recorder := httptest.NewRecorder()
		r := NewResponder(recorder, http.MethodPost)

		r.Respond(tc.data, tc.err)

		assert.Equal(t, tc.expectedCode, recorder.Code, "TEST[%d] Failed: %s", i, tc.desc)
	}
}

func TestResponder_JSONEncodingFailure(t *testing.T) {
	tests := []struct {
		desc string
		data any
	}{
		{"NaN value", math.NaN()},
		{"positive infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
		{"channel type", make(chan int)},
		{"function type", func() {}},
	}

	for i, tc := range tests {
		recorder := httptest.NewRecorder()
		responder := NewResponder(recorder, http.MethodGet)

		responder.Respond(tc.data, nil)

		result := recorder.Result()

		assert.Equal(t, http.StatusInternalServerError, result.StatusCode, "TEST[%d] Failed: %s", i, tc.desc)
		assert.Equal(t, "application/json", result.Header.Get("Content-Type"), "TEST[%d] Failed: %s", i, tc.desc)

		body := new(bytes.Buffer)
		_, err := body.ReadFrom(result.Body)

		require.NoError(t, err, "TEST[%d] Failed: %s", i, tc.desc)

		expectedBody := `{"code":-1,"data":null,"message":"failed to encode response as JSON"}` + "\n"
		assert.Equal(t, expectedBody, body.String(), "TEST[%d] Failed: %s", i, tc.desc)

		require.NoError(t, result.Body.Close())
	}
}

func TestResponder_ValidEncodableData(t *testing.T) {
	tests := []struct {
		desc         string
		data         any
		expectedCode int
	}{
		{"normal float", 42.5, http.StatusOK},
		{"zero float", 0.0, http.StatusOK},
		{"struct with floats", struct{ Temp float64 }{Temp: 98.6}, http.StatusOK},
		{"map with numbers", map[string]float64{"value": 123.45}, http.StatusOK},
	}

	for i, tc := range tests {
		recorder := httptest.NewRecorder()
		responder := NewResponder(recorder, http.MethodGet)

		responder.Respond(tc.data, nil)

		result := recorder.Result()

		t.Cleanup(func() {
			require.NoError(t, result.Body.Close())
		})

		assert.Equal(t, tc.expectedCode, result.StatusCode, "TEST[%d] Failed: %s", i, tc.desc)

		body := new(bytes.Buffer)
		_, err := body.ReadFrom(result.Body)

		require.NoError(t, err, "TEST[%d] Failed: %s", i, tc.desc)

		assert.NotEmpty(t, body.String(), "TEST[%d] Failed: %s", i, tc.desc)
	}
}
