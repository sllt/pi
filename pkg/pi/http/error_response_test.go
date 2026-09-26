package http

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"

	resTypes "github.com/sllt/pi/pkg/pi/http/response"
	"github.com/stretchr/testify/require"
)

func TestPublicErrors(t *testing.T) {
	bad := CustomBusinessError{BusinessCode: 42, HTTPStatus: 409, Msg: "conflict"}
	for _, tc := range []struct {
		name         string
		err          error
		status, code int
		message      string
	}{
		{"wrapped", fmt.Errorf("password=secret: %w", bad), 409, 42, "conflict"},
		{"joined", errors.Join(bad, ErrorInvalidParam{}), 409, 42, "conflict"},
		{"joined reversed", errors.Join(ErrorInvalidParam{}, bad), 400, 400, "'0' invalid parameter(s): "},
		{"private", errors.New("postgres://secret"), 500, -1, "internal server error"},
		{"cancel wins", errors.Join(bad, context.Canceled), 499, 499, "request canceled"},
		{"deadline", fmt.Errorf("wrapped: %w", context.DeadlineExceeded), 408, 408, "request timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, message := ErrorResponse(tc.err)
			require.Equal(t, tc.status, status)
			require.Equal(t, tc.code, code)
			require.Equal(t, tc.message, message)
			for _, data := range []any{resTypes.Raw{Data: "private content"}, resTypes.File{Content: []byte("private content")}, resTypes.Redirect{URL: "https://private"}} {
				w := httptest.NewRecorder()
				NewResponder(w, "GET").Respond(data, tc.err)
				require.Equal(t, tc.status, w.Code)
				require.NotContains(t, w.Body.String(), "private content")
				require.NotContains(t, w.Body.String(), "secret")
				require.Empty(t, w.Header().Get("Location"))
			}
		})
	}
}
