package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

func TestResponder_FileWithHeaders(t *testing.T) {
	recorder := httptest.NewRecorder()
	r := NewResponder(recorder, http.MethodGet)

	payload := []byte("compressed-js-bytes")
	r.Respond(resTypes.File{
		Content:     payload,
		ContentType: "text/javascript; charset=utf-8",
		StatusCode:  http.StatusOK,
		Headers: map[string]string{
			"Content-Encoding": "gzip",
			"Vary":             "Accept-Encoding",
			"":                 "ignored-empty-key",
			"X-Empty-Value":    "",
		},
	}, nil)

	result := recorder.Result()
	t.Cleanup(func() {
		require.NoError(t, result.Body.Close())
	})

	assert.Equal(t, http.StatusOK, result.StatusCode)
	assert.Equal(t, "text/javascript; charset=utf-8", result.Header.Get("Content-Type"))
	assert.Equal(t, "gzip", result.Header.Get("Content-Encoding"))
	assert.Equal(t, "Accept-Encoding", result.Header.Get("Vary"))
	assert.Empty(t, result.Header.Get("X-Empty-Value"))
	assert.Equal(t, payload, recorder.Body.Bytes())
}

func TestResponder_HeadSuppressesAllBodies(t *testing.T) {
	t.Run("json_success", func(t *testing.T) {
		rec := httptest.NewRecorder()
		NewResponder(rec, http.MethodHead).Respond(map[string]string{"hello": "world"}, nil)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		assert.Empty(t, rec.Body.Bytes())
	})

	t.Run("json_error", func(t *testing.T) {
		rec := httptest.NewRecorder()
		NewResponder(rec, http.MethodHead).Respond(nil, ErrorInvalidRoute{})
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		assert.Empty(t, rec.Body.Bytes())
	})

	t.Run("json_encode_error", func(t *testing.T) {
		rec := httptest.NewRecorder()
		NewResponder(rec, http.MethodHead).Respond(math.NaN(), nil)
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		assert.Empty(t, rec.Body.Bytes())
	})

	t.Run("file_and_xml", func(t *testing.T) {
		recFile := httptest.NewRecorder()
		NewResponder(recFile, http.MethodHead).Respond(resTypes.File{
			Content:     []byte("binary"),
			ContentType: "image/png",
			Headers:     map[string]string{"Cache-Control": "max-age=60"},
		}, nil)
		assert.Equal(t, http.StatusOK, recFile.Code)
		assert.Equal(t, "image/png", recFile.Header().Get("Content-Type"))
		assert.Equal(t, "max-age=60", recFile.Header().Get("Cache-Control"))
		assert.Empty(t, recFile.Body.Bytes())

		recXML := httptest.NewRecorder()
		NewResponder(recXML, http.MethodHead).Respond(&resTypes.XML{
			Content:     []byte("<rss/>"),
			ContentType: "application/rss+xml",
			StatusCode:  http.StatusCreated,
		}, nil)
		assert.Equal(t, http.StatusCreated, recXML.Code)
		assert.Equal(t, "application/rss+xml", recXML.Header().Get("Content-Type"))
		assert.Empty(t, recXML.Body.Bytes())
	})

	t.Run("stream_and_template", func(t *testing.T) {
		streamCalled := false
		recStream := httptest.NewRecorder()
		NewResponder(recStream, http.MethodHead).Respond(resTypes.Stream{
			StatusCode:  http.StatusAccepted,
			ContentType: "text/event-stream",
			Run: func(ctx context.Context, w io.Writer) error {
				streamCalled = true
				_, err := w.Write([]byte("data: hi\n\n"))
				return err
			},
		}, nil)
		assert.Equal(t, http.StatusAccepted, recStream.Code)
		assert.Equal(t, "text/event-stream", recStream.Header().Get("Content-Type"))
		assert.False(t, streamCalled)
		assert.Empty(t, recStream.Body.Bytes())

		recTmpl := httptest.NewRecorder()
		NewResponder(recTmpl, http.MethodHead).Respond(resTypes.Template{Name: "nonexistent.html"}, nil)
		assert.Equal(t, http.StatusOK, recTmpl.Code)
		assert.Equal(t, "text/html", recTmpl.Header().Get("Content-Type"))
		assert.Empty(t, recTmpl.Body.Bytes())
	})

	t.Run("explicit_result_201", func(t *testing.T) {
		rec := httptest.NewRecorder()
		NewResponder(rec, http.MethodHead).Respond(resTypes.Result{
			StatusCode: http.StatusCreated,
			Data:       map[string]int{"id": 1},
			Headers:    map[string]string{"X-Test": "ok"},
		}, nil)
		assert.Equal(t, http.StatusCreated, rec.Code)
		assert.Equal(t, "ok", rec.Header().Get("X-Test"))
		assert.Empty(t, rec.Body.Bytes())
	})

	t.Run("no_content_stream_nil_raw_and_response", func(t *testing.T) {
		rec204 := httptest.NewRecorder()
		NewResponder(rec204, http.MethodHead).Respond(resTypes.NoContent(), nil)
		assert.Equal(t, http.StatusNoContent, rec204.Code)
		assert.Empty(t, rec204.Body.Bytes())

		recNilStream := httptest.NewRecorder()
		NewResponder(recNilStream, http.MethodHead).Respond(resTypes.Stream{Run: nil}, nil)
		assert.Equal(t, http.StatusInternalServerError, recNilStream.Code)
		assert.Equal(t, "application/json", recNilStream.Header().Get("Content-Type"))
		assert.Empty(t, recNilStream.Body.Bytes())

		recRaw := httptest.NewRecorder()
		NewResponder(recRaw, http.MethodHead).Respond(resTypes.Raw{Data: map[string]string{"a": "b"}}, nil)
		assert.Equal(t, http.StatusOK, recRaw.Code)
		assert.Equal(t, "application/json", recRaw.Header().Get("Content-Type"))
		assert.Empty(t, recRaw.Body.Bytes())

		recResp := httptest.NewRecorder()
		NewResponder(recResp, http.MethodHead).Respond(resTypes.Response{
			Data:    "ok",
			Headers: map[string]string{"X-Custom": "yes"},
		}, nil)
		assert.Equal(t, http.StatusOK, recResp.Code)
		assert.Equal(t, "yes", recResp.Header().Get("X-Custom"))
		assert.Equal(t, "application/json", recResp.Header().Get("Content-Type"))
		assert.Empty(t, recResp.Body.Bytes())
	})
}

func TestResponder_FileContentTypeAndPointerDefense(t *testing.T) {
	t.Run("default_octet_stream", func(t *testing.T) {
		rec := httptest.NewRecorder()
		NewResponder(rec, http.MethodGet).Respond(resTypes.File{Content: []byte("abc")}, nil)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
		assert.Equal(t, []byte("abc"), rec.Body.Bytes())
	})

	t.Run("whitespace_content_type_falls_back_cleanly", func(t *testing.T) {
		recFile := httptest.NewRecorder()
		NewResponder(recFile, http.MethodGet).Respond(resTypes.File{
			Content:     []byte("abc"),
			ContentType: "   ",
		}, nil)
		assert.Equal(t, http.StatusOK, recFile.Code)
		assert.Equal(t, "application/octet-stream", recFile.Header().Get("Content-Type"))
		assert.Equal(t, []byte("abc"), recFile.Body.Bytes())

		recFileHdr := httptest.NewRecorder()
		NewResponder(recFileHdr, http.MethodGet).Respond(resTypes.File{
			Content:     []byte("webp"),
			ContentType: "   ",
			Headers:     map[string]string{"content-type": "image/webp"},
		}, nil)
		assert.Equal(t, http.StatusOK, recFileHdr.Code)
		assert.Equal(t, "image/webp", recFileHdr.Header().Get("Content-Type"))

		recXML := httptest.NewRecorder()
		NewResponder(recXML, http.MethodGet).Respond(resTypes.XML{
			Content:     []byte("<ok/>"),
			ContentType: "   ",
		}, nil)
		assert.Equal(t, http.StatusOK, recXML.Code)
		assert.Equal(t, "application/xml", recXML.Header().Get("Content-Type"))
		assert.Equal(t, []byte("<ok/>"), recXML.Body.Bytes())
	})

	t.Run("preserves_headers_content_type_when_field_empty", func(t *testing.T) {
		rec := httptest.NewRecorder()
		NewResponder(rec, http.MethodGet).Respond(resTypes.File{
			Content: []byte("webp"),
			Headers: map[string]string{"Content-Type": "image/webp"},
		}, nil)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "image/webp", rec.Header().Get("Content-Type"))
		assert.Equal(t, []byte("webp"), rec.Body.Bytes())
	})

	t.Run("non_nil_pointer_file_and_xml", func(t *testing.T) {
		recFile := httptest.NewRecorder()
		NewResponder(recFile, http.MethodPost).Respond(&resTypes.File{
			Content:     []byte("ok"),
			ContentType: "text/plain",
			StatusCode:  http.StatusOK,
		}, nil)
		assert.Equal(t, http.StatusOK, recFile.Code)
		assert.Equal(t, "text/plain", recFile.Header().Get("Content-Type"))
		assert.Equal(t, []byte("ok"), recFile.Body.Bytes())

		recXML := httptest.NewRecorder()
		NewResponder(recXML, http.MethodPost).Respond(&resTypes.XML{
			Content:    []byte("<ok/>"),
			StatusCode: http.StatusAccepted,
		}, nil)
		assert.Equal(t, http.StatusAccepted, recXML.Code)
		assert.Equal(t, "application/xml", recXML.Header().Get("Content-Type"))
		assert.Equal(t, []byte("<ok/>"), recXML.Body.Bytes())
	})

	t.Run("nil_pointer_file_and_xml_fall_through_safely", func(t *testing.T) {
		var nilFile *resTypes.File
		recFile := httptest.NewRecorder()
		NewResponder(recFile, http.MethodGet).Respond(nilFile, nil)
		assert.Equal(t, http.StatusOK, recFile.Code)
		assert.Equal(t, "application/json", recFile.Header().Get("Content-Type"))
		assert.JSONEq(t, `{"code":0,"data":null,"message":"ok"}`, recFile.Body.String())

		var nilXML *resTypes.XML
		recXML := httptest.NewRecorder()
		NewResponder(recXML, http.MethodGet).Respond(nilXML, nil)
		assert.Equal(t, http.StatusOK, recXML.Code)
		assert.Equal(t, "application/json", recXML.Header().Get("Content-Type"))
		assert.JSONEq(t, `{"code":0,"data":null,"message":"ok"}`, recXML.Body.String())
	})
}

func TestRequestRawBodyLimitAndRelease(t *testing.T) {
	var nilReq *Request
	nilReq.ReleaseRawBody()
	got, err := nilReq.RawBodyBytesWithLimit(64)
	require.NoError(t, err)
	assert.Nil(t, got)

	// RawBodyBytesWithLimit uses passed limit when no snapshot or maxBodyBytes is set.
	payload := strings.Repeat("a", 200)
	reqOver := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(payload))
	piReqOver := NewRequest(reqOver)
	_, err = piReqOver.RawBodyBytesWithLimit(100)
	var maxErr *http.MaxBytesError
	require.ErrorAs(t, err, &maxErr)

	reqUnder := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(payload))
	piReqUnder := NewRequest(reqUnder)
	gotUnder, err := piReqUnder.RawBodyBytesWithLimit(256)
	require.NoError(t, err)
	assert.Equal(t, []byte(payload), gotUnder)

	// Explicit maxBodyBytes on NewRequestWithSnapshotAndLimit takes precedence over limit param.
	reqExplicit := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader(payload))
	piReqExplicit := NewRequestWithSnapshotAndLimit(reqExplicit, nil, nil, false, 50)
	_, err = piReqExplicit.RawBodyBytesWithLimit(500)
	require.ErrorAs(t, err, &maxErr)

	// ReleaseRawBody clears rawBody, resets Body to http.NoBody, and preserves MultipartForm.
	mf := &multipart.Form{Value: map[string][]string{"title": {"demo"}}}
	reqRel := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader("raw"))
	reqRel.MultipartForm = mf
	piReqRel := NewRequestWithSnapshotAndLimit(reqRel, nil, []byte("raw"), true, 1024)
	piReqRel.ReleaseRawBody()

	released, err := piReqRel.RawBodyBytes()
	require.NoError(t, err)
	assert.Nil(t, released)
	assert.Equal(t, http.NoBody, reqRel.Body)
	assert.Same(t, mf, reqRel.MultipartForm)
}

type unwrappedTestResponseWriter struct {
	inner     *httptest.ResponseRecorder
	flushes   int
	writeHook func()
}

func (u *unwrappedTestResponseWriter) Header() http.Header {
	return u.inner.Header()
}

func (u *unwrappedTestResponseWriter) Write(p []byte) (int, error) {
	if u.writeHook != nil {
		u.writeHook()
	}
	return u.inner.Write(p)
}

func (u *unwrappedTestResponseWriter) WriteHeader(code int) {
	u.inner.WriteHeader(code)
}

func (u *unwrappedTestResponseWriter) Flush() {
	u.flushes++
	u.inner.Flush()
}

func (u *unwrappedTestResponseWriter) Unwrap() http.ResponseWriter {
	return u.inner
}

type wrappingOnlyResponseWriter struct {
	inner http.ResponseWriter
}

func (w *wrappingOnlyResponseWriter) Header() http.Header {
	return w.inner.Header()
}

func (w *wrappingOnlyResponseWriter) Write(p []byte) (int, error) {
	return w.inner.Write(p)
}

func (w *wrappingOnlyResponseWriter) WriteHeader(code int) {
	w.inner.WriteHeader(code)
}

func (w *wrappingOnlyResponseWriter) Unwrap() http.ResponseWriter {
	return w.inner
}

func TestResponder_StreamPointerHeadersFlushAndSSE(t *testing.T) {
	t.Run("non_nil_pointer_stream_with_headers_and_immediate_flush", func(t *testing.T) {
		rec := httptest.NewRecorder()
		var flushedBeforeRun bool
		var sawFlusher, sawRC bool

		inner := &unwrappedTestResponseWriter{inner: rec}
		wrapped := &wrappingOnlyResponseWriter{inner: inner}
		NewResponder(wrapped, http.MethodPost).Respond(&resTypes.Stream{
			StatusCode: http.StatusAccepted,
			Headers: map[string]string{
				"X-Stream":     "yes",
				"content-type": "text/plain; charset=utf-8",
				"":             "ignored",
				"X-Empty":      "",
			},
			Run: func(ctx context.Context, w io.Writer) error {
				flushedBeforeRun = rec.Flushed
				if f, ok := w.(http.Flusher); ok {
					sawFlusher = true
					f.Flush()
				}
				if rw, ok := w.(http.ResponseWriter); ok {
					if err := http.NewResponseController(rw).Flush(); err == nil {
						sawRC = true
					}
				}
				_, err := io.WriteString(w, "chunk-1")
				return err
			},
		}, nil)

		assert.True(t, flushedBeforeRun, "headers must be flushed before Stream.Run executes")
		assert.True(t, sawFlusher, "stream writer must implement http.Flusher")
		assert.True(t, sawRC, "stream writer must support http.NewResponseController")
		assert.Equal(t, 4, inner.flushes, "expected initial header flush + explicit flushes + auto-flush on Write")
		assert.Equal(t, http.StatusAccepted, rec.Code)
		assert.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))
		assert.Equal(t, "yes", rec.Header().Get("X-Stream"))
		assert.Empty(t, rec.Header().Get("X-Empty"))
		assert.Equal(t, "chunk-1", rec.Body.String())
	})

	t.Run("nil_pointer_stream_falls_through_safely", func(t *testing.T) {
		var nilStream *resTypes.Stream
		rec := httptest.NewRecorder()
		r := NewResponder(rec, http.MethodGet)
		assert.Equal(t, http.StatusOK, r.getStatusCodeForSpecialResponse(nilStream, nil))
		r.Respond(nilStream, nil)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		assert.JSONEq(t, `{"code":0,"data":null,"message":"ok"}`, rec.Body.String())
	})

	t.Run("non_nil_pointer_stream_nil_run_returns_500", func(t *testing.T) {
		rec := httptest.NewRecorder()
		NewResponder(rec, http.MethodGet).Respond(&resTypes.Stream{StatusCode: http.StatusCreated, Run: nil}, nil)
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	})

	t.Run("new_sse_get_and_head", func(t *testing.T) {
		recGet := httptest.NewRecorder()
		sseStream := resTypes.NewSSE(
			func(ctx context.Context, sse *resTypes.SSEWriter) error {
				return sse.Send(resTypes.SSEEvent{ID: "1", Event: "msg", Data: "hello"})
			},
			resTypes.WithSSEKeepAlive(0),
			resTypes.WithSSEHeaders(map[string]string{"X-Custom": "sse"}),
		)
		NewResponder(recGet, http.MethodGet).Respond(sseStream, nil)
		assert.Equal(t, http.StatusOK, recGet.Code)
		assert.Equal(t, "text/event-stream; charset=utf-8", recGet.Header().Get("Content-Type"))
		assert.Equal(t, "no-cache, no-transform", recGet.Header().Get("Cache-Control"))
		assert.Empty(t, recGet.Header().Get("Connection"))
		assert.Equal(t, "no", recGet.Header().Get("X-Accel-Buffering"))
		assert.Equal(t, "sse", recGet.Header().Get("X-Custom"))
		assert.True(t, recGet.Flushed)
		assert.Equal(t, "id: 1\nevent: msg\ndata: hello\n\n", recGet.Body.String())

		called := false
		recHead := httptest.NewRecorder()
		NewResponder(recHead, http.MethodHead).Respond(resTypes.NewSSE(func(ctx context.Context, sse *resTypes.SSEWriter) error {
			called = true
			return sse.SendData("ignored")
		}), nil)
		assert.False(t, called)
		assert.Equal(t, http.StatusOK, recHead.Code)
		assert.Equal(t, "text/event-stream; charset=utf-8", recHead.Header().Get("Content-Type"))
		assert.Empty(t, recHead.Body.Bytes())
	})

	t.Run("raw_sse_value_and_pointer_defaults_and_nil_guards", func(t *testing.T) {
		recPost := httptest.NewRecorder()
		var gotFlusher bool
		NewResponder(recPost, http.MethodPost).Respond(resTypes.SSE{
			Run: func(ctx context.Context, w io.Writer, flusher http.Flusher) error {
				gotFlusher = flusher != nil
				_, err := io.WriteString(w, "data: post-sse\n\n")
				if flusher != nil {
					flusher.Flush()
				}
				return err
			},
		}, nil)
		assert.True(t, gotFlusher)
		assert.Equal(t, http.StatusOK, recPost.Code)
		assert.Equal(t, "text/event-stream; charset=utf-8", recPost.Header().Get("Content-Type"))
		assert.Equal(t, "data: post-sse\n\n", recPost.Body.String())

		var nilSSE *resTypes.SSE
		recNil := httptest.NewRecorder()
		NewResponder(recNil, http.MethodGet).Respond(nilSSE, nil)
		assert.Equal(t, http.StatusOK, recNil.Code)
		assert.Equal(t, "application/json", recNil.Header().Get("Content-Type"))
		assert.JSONEq(t, `{"code":0,"data":null,"message":"ok"}`, recNil.Body.String())

		recNilRun := httptest.NewRecorder()
		NewResponder(recNilRun, http.MethodGet).Respond(&resTypes.SSE{Run: nil}, nil)
		assert.Equal(t, http.StatusInternalServerError, recNilRun.Code)
		assert.Equal(t, "application/json", recNilRun.Header().Get("Content-Type"))
	})

	t.Run("set_stream_base_context_reaches_stream_and_sse", func(t *testing.T) {
		type ctxKey string
		const k ctxKey = "mw-key"

		req := httptest.NewRequest(http.MethodGet, "/events", http.NoBody)
		rec := httptest.NewRecorder()
		resp := NewResponderForRequest(rec, req, LegacyStatus)
		resp.SetStreamBaseContext(context.WithValue(context.Background(), k, "mw-val"))

		var gotVal any
		resp.Respond(resTypes.NewSSE(func(ctx context.Context, sse *resTypes.SSEWriter) error {
			gotVal = ctx.Value(k)
			return sse.SendData("ok")
		}, resTypes.WithSSEKeepAlive(0)), nil)

		assert.Equal(t, "mw-val", gotVal)
	})
}

func TestResponder_RawReplayAndSetHeader(t *testing.T) {
	t.Run("preserves_outer_vary_and_request_id_and_uses_canonical_headers", func(t *testing.T) {
		rec := httptest.NewRecorder()
		rec.Header().Set("X-Request-ID", "fresh-req-id")
		rec.Header().Set("Vary", "Origin")

		NewResponder(rec, http.MethodPost).Respond(RawReplay{
			StatusCode: http.StatusCreated,
			Headers: http.Header{
				"content-type": {"application/json"},
				"Vary":         {"Origin, Accept-Encoding"},
				"X-Custom":     {"v1", "v2"},
			},
			Body: []byte(`{"id":"ord-1"}` + "\n"),
		}, nil)

		assert.Equal(t, http.StatusCreated, rec.Code)
		assert.Equal(t, "fresh-req-id", rec.Header().Get("X-Request-ID"))
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		assert.Equal(t, []string{"Origin", "Accept-Encoding"}, rec.Header().Values("Vary"))
		assert.Equal(t, []string{"v1", "v2"}, rec.Header().Values("X-Custom"))
		assert.Equal(t, `{"id":"ord-1"}`+"\n", rec.Body.String())
	})

	t.Run("pointer_raw_replay_and_nil_pointer", func(t *testing.T) {
		rec := httptest.NewRecorder()
		NewResponder(rec, http.MethodPost).Respond(&RawReplay{
			StatusCode: http.StatusAccepted,
			Headers:    map[string][]string{"Content-Type": {"text/plain"}},
			Body:       []byte("ptr-ok"),
		}, nil)
		assert.Equal(t, http.StatusAccepted, rec.Code)
		assert.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
		assert.Equal(t, "ptr-ok", rec.Body.String())

		var nilReplay *RawReplay
		recNil := httptest.NewRecorder()
		NewResponder(recNil, http.MethodGet).Respond(nilReplay, nil)
		assert.Equal(t, http.StatusOK, recNil.Code)
		assert.JSONEq(t, `{"code":0,"data":null,"message":"ok"}`, recNil.Body.String())
	})

	t.Run("suppresses_body_on_204_304_and_head_while_preserving_headers", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			method string
			status int
		}{
			{name: "204_no_content", method: http.MethodDelete, status: http.StatusNoContent},
			{name: "304_not_modified", method: http.MethodGet, status: http.StatusNotModified},
			{name: "head_200", method: http.MethodHead, status: http.StatusOK},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				NewResponder(rec, tc.method).Respond(RawReplay{
					StatusCode: tc.status,
					Headers: http.Header{
						"X-Idempotent-Replay": {"true"},
						"Content-Type":        {"application/json"},
					},
					Body: []byte(`{"should":"not-be-written"}`),
				}, nil)

				assert.Equal(t, tc.status, rec.Code)
				assert.Equal(t, "true", rec.Header().Get("X-Idempotent-Replay"))
				assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
				assert.Empty(t, rec.Body.Bytes())
			})
		}
	})

	t.Run("zero_status_falls_back_to_policy_status", func(t *testing.T) {
		rec := httptest.NewRecorder()
		r := NewResponder(rec, http.MethodPost)
		assert.Equal(t, http.StatusCreated, r.getStatusCodeForSpecialResponse(RawReplay{Body: []byte("ok")}, nil))
		r.Respond(RawReplay{
			Headers: http.Header{"Content-Type": {"text/plain"}},
			Body:    []byte("ok"),
		}, nil)
		assert.Equal(t, http.StatusCreated, rec.Code)
		assert.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
		assert.Equal(t, "ok", rec.Body.String())
	})

	t.Run("set_header_guards_nil_and_empty_key", func(t *testing.T) {
		var nilResp *Responder
		require.NotPanics(t, func() {
			nilResp.SetHeader("X-Test", "1")
			(&Responder{}).SetHeader("X-Test", "1")
		})

		rec := httptest.NewRecorder()
		resp := NewResponder(rec, http.MethodGet)
		resp.SetHeader("", "ignored")
		resp.SetHeader("X-Idempotent-Replayed", "false")
		resp.Respond("ok", nil)
		assert.Equal(t, "false", rec.Header().Get("X-Idempotent-Replayed"))
		assert.Empty(t, rec.Header().Get(""))
	})
}
