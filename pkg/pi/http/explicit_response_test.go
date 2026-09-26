package http

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/sllt/pi/pkg/pi/apperror"
	r "github.com/sllt/pi/pkg/pi/http/response"
	"github.com/stretchr/testify/require"
)

type mustNotMarshal struct{}

func (mustNotMarshal) MarshalJSON() ([]byte, error) { panic("204 tried to encode body") }

func TestExplicitResponseOwnership(t *testing.T) {
	for _, tc := range []struct {
		data any
		err  error
		code int
		body string
	}{
		{r.Created(map[string]int{"id": 1}), nil, 201, "\"id\":1"},
		{r.Accepted(nil), nil, 202, "\"code\":0"},
		{nil, nil, 200, "\"code\":0"},
		{r.Result{Data: mustNotMarshal{}, StatusCode: 204}, nil, 204, ""},
		{r.File{Content: []byte("must disappear"), StatusCode: 204}, nil, 204, ""},
		{r.Created("private success"), apperror.New(apperror.Forbidden, 403, "Forbidden"), 403, "Forbidden"},
	} {
		w := httptest.NewRecorder()
		NewResponderForRequest(w, httptest.NewRequest("POST", "/", nil), ExplicitStatus).Respond(tc.data, tc.err)
		require.Equal(t, tc.code, w.Code)
		if tc.code == 204 {
			require.Empty(t, w.Body.String())
		} else {
			require.Contains(t, w.Body.String(), tc.body)
		}
		require.NotContains(t, w.Body.String(), "private success")
	}
	called := false
	stream := r.Stream{Run: func(context.Context, io.Writer) error { called = true; return nil }}
	w := httptest.NewRecorder()
	NewResponder(w, "GET").Respond(stream, errors.New("private failure"))
	require.False(t, called)
	require.Equal(t, 500, w.Code)
	w = httptest.NewRecorder()
	NewResponder(w, "GET").Respond(r.Stream{Run: func(_ context.Context, w io.Writer) error {
		_, _ = io.WriteString(w, "partial")
		return errors.New("private stream failure")
	}}, nil)
	require.Equal(t, "partial", w.Body.String())
}
