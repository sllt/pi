package http

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type spyBody struct {
	reader    io.Reader
	readCalls int
}

func (s *spyBody) Read(p []byte) (int, error) {
	s.readCalls++
	return s.reader.Read(p)
}

func (s *spyBody) Close() error {
	return nil
}

type errBody struct {
	err error
}

func (e *errBody) Read([]byte) (int, error) {
	return 0, e.err
}

func (e *errBody) Close() error {
	return nil
}

func TestRequestWithSnapshotNilSafety(t *testing.T) {
	var nilReq *Request
	assert.Nil(t, nilReq.HTTPRequest())
	body, err := nilReq.RawBodyBytes()
	assert.NoError(t, err)
	assert.Nil(t, body)

	noHTTP := NewRequestWithSnapshot(nil, nil, []byte(`{"a":1}`), true)
	assert.Nil(t, noHTTP.HTTPRequest())
	body, err = noHTTP.RawBodyBytes()
	assert.NoError(t, err)
	assert.Nil(t, body)
}

func TestRequestWithSnapshotZeroCopyFastPath(t *testing.T) {
	spy := &spyBody{reader: strings.NewReader(`{"from":"stream"}`)}
	httpReq := httptest.NewRequest(http.MethodPost, "/users", spy)
	httpReq.Header.Set("Content-Type", "application/json")

	snapshot := []byte(`{"name":"alice"}`)
	piReq := NewRequestWithSnapshot(httpReq, nil, snapshot, true)

	assert.Same(t, httpReq, piReq.HTTPRequest())

	got, err := piReq.RawBodyBytes()
	require.NoError(t, err)
	assert.Equal(t, snapshot, got)
	// Must return exact backing slice with zero extra copy and zero reads of req.Body.
	assert.True(t, &got[0] == &snapshot[0])
	assert.Equal(t, 0, spy.readCalls)

	var payload struct {
		Name string `json:"name"`
	}
	require.NoError(t, piReq.Bind(&payload))
	assert.Equal(t, "alice", payload.Name)
	assert.Equal(t, 0, spy.readCalls)

	// Underlying http.Request.Body must remain readable after body()/Bind().
	streamBytes, err := io.ReadAll(httpReq.Body)
	require.NoError(t, err)
	assert.Equal(t, `{"from":"stream"}`, string(streamBytes))
	assert.Greater(t, spy.readCalls, 0)
}

func TestRequestRawBodyBytesFallbackWithoutSnapshot(t *testing.T) {
	raw := []byte(`{"id":42}`)
	httpReq := httptest.NewRequest(http.MethodPost, "/items", bytes.NewReader(raw))
	httpReq.Header.Set("Content-Type", "application/json")

	piReq := NewRequestWithSnapshot(httpReq, nil, nil, false)

	got1, err := piReq.RawBodyBytes()
	require.NoError(t, err)
	assert.Equal(t, raw, got1)

	// body() resets r.req.Body so subsequent reads and Bind() still succeed.
	got2, err := piReq.RawBodyBytes()
	require.NoError(t, err)
	assert.Equal(t, raw, got2)

	var payload struct {
		ID int `json:"id"`
	}
	require.NoError(t, piReq.Bind(&payload))
	assert.Equal(t, 42, payload.ID)

	// Empty snapshot fast-path.
	emptyReq := NewRequestWithSnapshot(httptest.NewRequest(http.MethodPost, "/empty", nil), nil, []byte{}, true)
	emptyGot, err := emptyReq.RawBodyBytes()
	require.NoError(t, err)
	assert.Empty(t, emptyGot)

	// Nil Body without snapshot should return nil, nil rather than panicking.
	nilBodyHTTP, err := http.NewRequest(http.MethodGet, "/none", nil)
	require.NoError(t, err)
	nilBodyReq := NewRequestWithSnapshot(nilBodyHTTP, nil, nil, false)
	nilGot, err := nilBodyReq.RawBodyBytes()
	require.NoError(t, err)
	assert.Nil(t, nilGot)

	// Read error propagation when snapshot is absent.
	sentinel := errors.New("boom")
	errHTTP := httptest.NewRequest(http.MethodPost, "/err", &errBody{err: sentinel})
	_, err = NewRequestWithSnapshot(errHTTP, nil, nil, false).RawBodyBytes()
	require.ErrorIs(t, err, sentinel)
}
