package http

import (
	"bytes"
	"context"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/file"
)

func TestParam(t *testing.T) {
	req := NewRequest(httptest.NewRequest(http.MethodGet, "/abc?a=b", http.NoBody))
	if req.Param("a") != "b" {
		t.Error("Can not parse the request params")
	}
}

func TestBind(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/abc", strings.NewReader(`{"a": "b", "b": 5}`))
	r.Header.Set("Content-Type", "application/json")
	req := NewRequest(r)

	x := struct {
		A string `json:"a"`
		B int    `json:"b"`
	}{}

	_ = req.Bind(&x)

	if x.A != "b" || x.B != 5 {
		t.Errorf("Bind error. Got: %v", x)
	}
}

func TestBind_FileSuccess(t *testing.T) {
	r := NewRequest(generateMultipartRequestZip(t))
	x := struct {
		// Zip file bind for zip struct
		Zip file.Zip `file:"zip"`

		// Zip file bind for zip pointer
		ZipPtr *file.Zip `file:"zip"`

		// FileHeader multipart.FileHeader bind(value)
		FileHeader multipart.FileHeader `file:"hello"`

		// FileHeaderPtr multipart.FileHeader bind for pointer
		FileHeaderPtr *multipart.FileHeader `file:"hello"`

		// Skip bind
		Skip *file.Zip `file:"-"`

		// Incompatible type cannot be bound
		Incompatible string `file:"hello"`

		// File not in multipart form
		FileNotPresent *multipart.FileHeader `file:"text"`

		// Additional form fields
		StringField string  `form:"stringField"`
		IntField    int     `form:"intField"`
		FloatField  float64 `form:"floatField"`
		BoolField   bool    `form:"boolField"`
	}{}

	err := r.Bind(&x)
	require.NoError(t, err)

	// Assert zip file bind
	assert.Len(t, x.Zip.Files, 2)
	assert.Equal(t, "Hello! This is file A.\n", string(x.Zip.Files["a.txt"].Bytes()))
	assert.Equal(t, "Hello! This is file B.\n\n", string(x.Zip.Files["b.txt"].Bytes()))

	// Assert zip file bind for pointer
	assert.NotNil(t, x.ZipPtr)
	assert.Len(t, x.ZipPtr.Files, 2)
	assert.Equal(t, "Hello! This is file A.\n", string(x.ZipPtr.Files["a.txt"].Bytes()))
	assert.Equal(t, "Hello! This is file B.\n\n", string(x.ZipPtr.Files["b.txt"].Bytes()))

	// Assert FileHeader struct type
	assert.Equal(t, "hello.txt", x.FileHeader.Filename)

	f, err := x.FileHeader.Open()
	require.NoError(t, err)
	assert.NotNil(t, f)

	content, err := io.ReadAll(f)
	require.NoError(t, err)
	assert.Equal(t, "Test hello!", string(content))

	// Assert FileHeader pointer type
	assert.NotNil(t, x.FileHeader)
	assert.Equal(t, "hello.txt", x.FileHeader.Filename)

	f, err = x.FileHeader.Open()
	require.NoError(t, err)
	assert.NotNil(t, f)

	content, err = io.ReadAll(f)
	require.NoError(t, err)
	assert.Equal(t, "Test hello!", string(content))

	// Assert skipped field
	assert.Nil(t, x.Skip)

	// Assert incompatible
	assert.Empty(t, x.Incompatible)

	// Assert file not present
	assert.Nil(t, x.FileNotPresent)

	// Assert additional form fields
	assert.Equal(t, "testString", x.StringField)
	assert.Equal(t, 123, x.IntField)
	assert.InEpsilon(t, 123.456, x.FloatField, 0.01)

	assert.True(t, x.BoolField)
}

func TestBind_NoContentType(t *testing.T) {
	req := NewRequest(httptest.NewRequest(http.MethodPost, "/abc", strings.NewReader(`{"a": "b", "b": 5}`)))
	x := struct {
		A string `json:"a"`
		B int    `json:"b"`
	}{}

	_ = req.Bind(&x)

	// The data won't bind so zero values are expected
	if x.A != "" || x.B != 0 {
		t.Errorf("Bind error. Got: %v", x)
	}
}

func Test_GetContext(t *testing.T) {
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "test/hello", http.NoBody)
	r := Request{req: req}

	assert.Equal(t, t.Context(), r.Context())
	assert.Equal(t, "http://", r.HostName())
}

func generateMultipartRequestZip(t *testing.T) *http.Request {
	t.Helper()

	var buf bytes.Buffer

	writer := multipart.NewWriter(&buf)

	f, err := os.Open("../testutil/test.zip")
	if err != nil {
		t.Fatalf("Failed to open test.zip: %v", err)
	}
	defer f.Close()

	zipPart, err := writer.CreateFormFile("zip", "test.zip")
	if err != nil {
		t.Fatalf("Failed to create form file: %v", err)
	}

	_, err = io.Copy(zipPart, f)
	if err != nil {
		t.Fatalf("Failed to write file to form: %v", err)
	}

	fileHeader, err := writer.CreateFormFile("hello", "hello.txt")
	if err != nil {
		t.Fatalf("Failed to create form file: %v", err)
	}

	_, err = io.Copy(fileHeader, bytes.NewReader([]byte(`Test hello!`)))
	if err != nil {
		t.Fatalf("Failed to write file to form: %v", err)
	}

	// Add non-file fields
	err = writer.WriteField("stringField", "testString")
	require.NoError(t, err)

	err = writer.WriteField("intField", "123")
	require.NoError(t, err)

	err = writer.WriteField("floatField", "123.456")
	require.NoError(t, err)

	err = writer.WriteField("boolField", "true")
	require.NoError(t, err)

	// Close the multipart writer
	writer.Close()

	// Create a new HTTP request with the multipart data
	req := httptest.NewRequest(http.MethodPost, "/upload", &buf)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	return req
}

func Test_bindMultipart_Fails(t *testing.T) {
	// Non-pointer bind
	r := NewRequest(generateMultipartRequestZip(t))
	input := struct {
		file *file.Zip
	}{}

	err := r.bindMultipart(input)
	require.Error(t, err)
	assert.Equal(t, errNonPointerBind, err)

	// unexported field cannot be binded
	err = r.bindMultipart(&input)
	require.ErrorIs(t, err, errNoFileFound)
}

func Test_bindMultipart_Fail_ParseMultiPart(t *testing.T) {
	r := NewRequest(generateMultipartRequestZip(t))
	input2 := struct {
		File *file.Zip `file:"zip"`
	}{}

	// Call the multipart reader to handle form from a multipart reader
	// This is called to invoke error while parsing Multipart form in bind
	_, _ = r.req.MultipartReader()

	err := r.bindMultipart(&input2)
	require.ErrorContains(t, err, "http: multipart handled by MultipartReader")
}

func Test_Params(t *testing.T) {
	req := &http.Request{
		URL: &url.URL{
			RawQuery: "category=books&category=electronics&tag=tech,science",
		},
	}
	r := NewRequest(req)

	expectedCategories := []string{"books", "electronics"}
	expectedTags := []string{"tech", "science"}

	assert.ElementsMatch(t, expectedCategories, r.Params("category"), "expected all values of 'category' to match")
	assert.ElementsMatch(t, expectedTags, r.Params("tag"), "expected all values of 'tag' to match")
	assert.Empty(t, r.Params("nonexistent"), "expected empty slice for non-existent query param")
}

func TestBind_FormURLEncoded(t *testing.T) {
	// Create a new HTTP request with form-encoded data
	req := NewRequest(httptest.NewRequest(http.MethodPost, "/abc", strings.NewReader("Name=John&Age=30")))
	req.req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	x := struct {
		Name string `form:"Name"`
		Age  int    `form:"Age"`
	}{}

	err := req.Bind(&x)
	if err != nil {
		t.Errorf("Bind error: %v", err)
	}

	// Check the results
	if x.Name != "John" || x.Age != 30 {
		t.Errorf("Bind error. Got: %v", x)
	}
}

func TestBind_BinaryOctetStream(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"Raw Binary Data", []byte{0x42, 0x65, 0x6c, 0x6c, 0x61}},
		{"Text-Based Binary Data", []byte("This is some binary data")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := NewRequest(httptest.NewRequest(http.MethodPost, "/binary", bytes.NewReader(tc.data)))
			req.req.Header.Set("Content-Type", "binary/octet-stream")

			var result []byte

			err := req.Bind(&result)
			if err != nil {
				t.Errorf("Bind error: %v", err)
			}

			if !bytes.Equal(result, tc.data) {
				t.Errorf("Bind error. Expected: %v, Got: %v", tc.data, result)
			}
		})
	}
}
func TestBind_BinaryOctetStream_NotPointerToByteSlice(t *testing.T) {
	req := &Request{
		req: httptest.NewRequest(http.MethodPost, "/binary", http.NoBody),
	}
	req.req.Header.Set("Content-Type", "binary/octet-stream")

	err := req.Bind("invalid input")

	if !errors.Is(err, errNonSliceBind) {
		t.Fatalf("Expected error: %v, got: %v", errNonSliceBind, err)
	}

	if !strings.Contains(err.Error(), "input is not a pointer to a byte slice: invalid input") {
		t.Errorf("Expected error to contain: input is not a pointer to a byte slice: invalid input, got: %v", err)
	}
}

func TestBind_ValidationRequired(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/abc", strings.NewReader(`{"email": ""}`))
	r.Header.Set("Content-Type", "application/json")
	req := NewRequest(r)

	x := struct {
		Email string `json:"email" binding:"required"`
	}{}

	err := req.Bind(&x)

	assert.Error(t, err, "Expected validation error for empty required field")
	assert.Contains(t, err.Error(), "email")
}

func TestBind_ValidationEmail(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/abc", strings.NewReader(`{"email": "invalid-email"}`))
	r.Header.Set("Content-Type", "application/json")
	req := NewRequest(r)

	x := struct {
		Email string `json:"email" binding:"required,email"`
	}{}

	err := req.Bind(&x)

	assert.Error(t, err, "Expected validation error for invalid email")
	assert.Contains(t, err.Error(), "email")
}

func TestBind_ValidationSuccess(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/abc", strings.NewReader(`{"email": "test@example.com"}`))
	r.Header.Set("Content-Type", "application/json")
	req := NewRequest(r)

	x := struct {
		Email string `json:"email" binding:"required,email"`
	}{}

	err := req.Bind(&x)

	assert.NoError(t, err, "Expected no validation error for valid email")
	assert.Equal(t, "test@example.com", x.Email)
}

func TestBind_ValidationWithLabel(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/abc", strings.NewReader(`{"email": ""}`))
	r.Header.Set("Content-Type", "application/json")
	req := NewRequest(r)

	x := struct {
		Email string `json:"email" binding:"required" label:"邮箱"`
	}{}

	err := req.Bind(&x)

	assert.Error(t, err)
	// label tag should be used as the field name in the error message
	assert.Contains(t, err.Error(), "邮箱")
}

func TestBind_ValidationWithMsgWildcard(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/abc", strings.NewReader(`{"code": ""}`))
	r.Header.Set("Content-Type", "application/json")
	req := NewRequest(r)

	x := struct {
		Code string `json:"code" binding:"required,len=6" msg:"验证码必须是6位数字"`
	}{}

	err := req.Bind(&x)

	assert.Error(t, err)
	assert.Equal(t, "验证码必须是6位数字", err.Error())
}

func TestBind_ValidationWithMsgPerRule(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/abc", strings.NewReader(`{"password": "12"}`))
	r.Header.Set("Content-Type", "application/json")
	req := NewRequest(r)

	x := struct {
		Password string `json:"password" binding:"required,min=6" label:"密码" msg:"required:{label}不能为空;min:{label}至少{param}位"`
	}{}

	err := req.Bind(&x)

	assert.Error(t, err)
	assert.Equal(t, "密码至少6位", err.Error())
}

func TestBind_ValidationWithMsgVars(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/abc", strings.NewReader(`{"age": 10}`))
	r.Header.Set("Content-Type", "application/json")
	req := NewRequest(r)

	x := struct {
		Age int `json:"age" binding:"required,gte=18" label:"年龄" msg:"gte:必须年满{param}岁"`
	}{}

	err := req.Bind(&x)

	assert.Error(t, err)
	assert.Equal(t, "必须年满18岁", err.Error())
}

func TestBind_ValidationStatusCode(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/abc", strings.NewReader(`{"email": ""}`))
	r.Header.Set("Content-Type", "application/json")
	req := NewRequest(r)

	x := struct {
		Email string `json:"email" binding:"required"`
	}{}

	err := req.Bind(&x)

	assert.Error(t, err)
	ve, ok := err.(*ValidationError)
	assert.True(t, ok)
	assert.Equal(t, 400, ve.StatusCode())
}

func TestParseMsgTag(t *testing.T) {
	// Wildcard message
	m := parseMsgTag("请输入正确的邮箱")
	assert.Equal(t, "请输入正确的邮箱", m["*"])

	// Per-rule messages
	m = parseMsgTag("required:不能为空;email:格式不正确")
	assert.Equal(t, "不能为空", m["required"])
	assert.Equal(t, "格式不正确", m["email"])

	// Empty
	m = parseMsgTag("")
	assert.Empty(t, m)
}

type timeoutReadCloser struct {
	err    error
	closed bool
}

func (b *timeoutReadCloser) Read([]byte) (int, error) {
	return 0, b.err
}

func (b *timeoutReadCloser) Close() error {
	b.closed = true
	return nil
}

type netTimeoutErr struct{}

func (netTimeoutErr) Error() string   { return "i/o timeout" }
func (netTimeoutErr) Timeout() bool   { return true }
func (netTimeoutErr) Temporary() bool { return false }

type closerSpyBody struct {
	reader io.Reader
	closed bool
}

func (b *closerSpyBody) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (b *closerSpyBody) Close() error {
	b.closed = true
	return nil
}

func TestRequest_BindBodyErrorNormalizationAndEOFClose(t *testing.T) {
	t.Run("truncated_json_body_io_error_returns_400_not_500", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/json", &timeoutReadCloser{err: io.ErrUnexpectedEOF})
		r.Header.Set("Content-Type", "application/json")
		req := NewRequest(r)

		var dst struct {
			Name string `json:"name"`
		}
		err := req.Bind(&dst)
		require.Error(t, err)
		status, _, _ := ErrorResponse(err)
		assert.Equal(t, http.StatusBadRequest, status)
	})

	t.Run("slow_body_net_timeout_returns_408", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/json", &timeoutReadCloser{err: netTimeoutErr{}})
		r.Header.Set("Content-Type", "application/json")
		req := NewRequest(r)

		var dst struct {
			Name string `json:"name"`
		}
		err := req.Bind(&dst)
		require.Error(t, err)
		status, _, _ := ErrorResponse(err)
		assert.Equal(t, http.StatusRequestTimeout, status)
	})

	t.Run("body_closes_origBody_on_clean_eof", func(t *testing.T) {
		spy := &closerSpyBody{reader: strings.NewReader(`{"ok":true}`)}
		r := httptest.NewRequest(http.MethodPost, "/json", spy)
		r.Header.Set("Content-Type", "application/json")
		req := NewRequest(r)

		raw, err := req.RawBodyBytes()
		require.NoError(t, err)
		assert.Equal(t, `{"ok":true}`, string(raw))
		assert.True(t, spy.closed)
	})
}

type deadlineSpyResponseWriter struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

func (w *deadlineSpyResponseWriter) SetReadDeadline(d time.Time) error {
	w.deadlines = append(w.deadlines, d)
	return nil
}

func TestRequest_BodyReadDeadlineGuards(t *testing.T) {
	t.Run("raw_body_arms_and_clears_deadline_immediately_and_multipart_bind_does_not_rearm", func(t *testing.T) {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		part, err := writer.CreateFormFile("upload", "note.txt")
		require.NoError(t, err)
		_, err = io.WriteString(part, "hello")
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		r := httptest.NewRequest(http.MethodPost, "/upload", &closerSpyBody{reader: bytes.NewReader(body.Bytes())})
		r.ContentLength = int64(body.Len())
		r.Header.Set("Content-Type", writer.FormDataContentType())

		spyW := &deadlineSpyResponseWriter{ResponseRecorder: httptest.NewRecorder()}
		req := NewRequest(r)
		req.ConfigureBodyRead(spyW, time.Now().Add(time.Second), false)

		raw, err := req.RawBodyBytes()
		require.NoError(t, err)
		assert.NotEmpty(t, raw)
		require.Len(t, spyW.deadlines, 2)
		assert.False(t, spyW.deadlines[0].IsZero())
		assert.True(t, spyW.deadlines[1].IsZero())

		// Subsequent bindMultipart sees r.hasRawBody == true and must not touch socket SetReadDeadline.
		var form struct {
			Upload *multipart.FileHeader `form:"upload"`
		}
		require.NoError(t, req.Bind(&form))
		require.NotNil(t, form.Upload)
		assert.Len(t, spyW.deadlines, 2)
	})

	t.Run("preparsed_post_form_skips_socket_deadline_but_enforces_expired_deadline_and_no_closed_body_leak", func(t *testing.T) {
		spyBody := &closerSpyBody{reader: strings.NewReader("")}
		r := httptest.NewRequest(http.MethodPost, "/form", spyBody)
		r.ContentLength = 10
		r.PostForm = url.Values{"k": []string{strings.Repeat("a", 32)}}

		spyW := &deadlineSpyResponseWriter{ResponseRecorder: httptest.NewRecorder()}
		req := NewRequestWithLimit(r, 8, nil)
		req.ConfigureBodyRead(spyW, time.Now().Add(time.Second), false)

		_, err := req.RawBodyBytes()
		var maxErr *http.MaxBytesError
		require.ErrorAs(t, err, &maxErr)
		assert.Empty(t, spyW.deadlines)
		assert.True(t, spyBody.closed)
		assert.Equal(t, http.NoBody, r.Body)

		// Expired readDeadline still fails fast with ErrorRequestTimeout even without socket deadline.
		reqExpired := NewRequest(httptest.NewRequest(http.MethodPost, "/form", strings.NewReader("a=b")))
		reqExpired.req.PostForm = url.Values{"a": []string{"b"}}
		reqExpired.ConfigureBodyRead(spyW, time.Now().Add(-time.Second), false)
		_, err = reqExpired.RawBodyBytes()
		assert.ErrorAs(t, err, new(ErrorRequestTimeout))
		assert.Empty(t, spyW.deadlines)
	})

	t.Run("bind_multipart_with_hasRawBody_still_enforces_canceled_context", func(t *testing.T) {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		part, err := writer.CreateFormFile("upload", "note.txt")
		require.NoError(t, err)
		_, err = io.WriteString(part, "hello")
		require.NoError(t, err)
		require.NoError(t, writer.Close())

		ctx, cancel := context.WithCancel(context.Background())
		r := httptest.NewRequest(http.MethodPost, "/upload", bytes.NewReader(body.Bytes())).WithContext(ctx)
		r.Header.Set("Content-Type", writer.FormDataContentType())
		spyW := &deadlineSpyResponseWriter{ResponseRecorder: httptest.NewRecorder()}
		req := NewRequest(r)
		req.ConfigureBodyRead(spyW, time.Now().Add(time.Second), false)

		_, err = req.RawBodyBytes()
		require.NoError(t, err)
		deadlinesAfterRaw := len(spyW.deadlines)

		cancel()
		var form struct {
			Upload *multipart.FileHeader `form:"upload"`
		}
		err = req.Bind(&form)
		assert.ErrorAs(t, err, new(ErrorClientClosedRequest))
		assert.Len(t, spyW.deadlines, deadlinesAfterRaw)
	})
}
