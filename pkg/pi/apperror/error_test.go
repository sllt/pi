package apperror_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/sllt/pi/pkg/pi/apperror"
	"github.com/sllt/pi/pkg/pi/auth"
	"github.com/sllt/pi/pkg/pi/cmd"
	piGRPC "github.com/sllt/pi/pkg/pi/grpc"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type legacyMissing struct{}

func (legacyMissing) Error() string   { return "missing" }
func (legacyMissing) StatusCode() int { return 404 }

func TestErrorTransportParityAndPrivateCauses(t *testing.T) {
	cause := errors.New("private database password")
	base := apperror.New(apperror.Conflict, 7001, "already exists")
	e := base.WithCause(cause).WithDetails(apperror.Detail{Field: "email", Rule: "unique"})
	err := fmt.Errorf("private wrapper: %w", e)
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, base)
	httpStatus, code, message := piHTTP.ErrorResponse(err)
	require.Equal(t, 409, httpStatus)
	require.Equal(t, 7001, code)
	require.Equal(t, "already exists", message)
	rpc := piGRPC.MapError(err)
	require.Equal(t, codes.AlreadyExists, status.Code(rpc))
	require.Equal(t, "already exists", status.Convert(rpc).Message())
	require.Equal(t, rpc.Error(), piGRPC.MapError(rpc).Error())
	exit, result := cmd.MapError(err)
	require.Equal(t, 1, exit)
	data, marshalErr := json.Marshal(result)
	require.NoError(t, marshalErr)
	require.NotContains(t, string(data), "private")
	require.Contains(t, string(data), "conflict")
	details := e.Details()
	details[0].Rule = "changed"
	require.Equal(t, "unique", e.Details()[0].Rule)
	s, _, m := piHTTP.ErrorResponse(errors.Join(legacyMissing{}, e))
	require.Equal(t, 404, s)
	require.Equal(t, "missing", m)
	s, _, m = piHTTP.ErrorResponse(errors.Join(e, legacyMissing{}))
	require.Equal(t, 409, s)
	require.Equal(t, "already exists", m)
	s, _, _ = piHTTP.ErrorResponse(errors.Join(e, context.Canceled))
	require.Equal(t, 499, s)
	require.Equal(t, codes.DeadlineExceeded, status.Code(piGRPC.MapError(errors.Join(e, context.DeadlineExceeded))))
	_, _, m = piHTTP.ErrorResponse(cause)
	require.Equal(t, "internal server error", m)
}

func TestPrincipalCopiesPermissions(t *testing.T) {
	p := auth.Principal{Subject: "task", Permissions: []string{"users:read:any"}}
	ctx := auth.WithPrincipal(t.Context(), p)
	p.Permissions[0] = "users:write:any"
	id, err := auth.AuthorizeSubject(ctx, "someone", "users:read:any")
	require.NoError(t, err)
	require.Equal(t, "someone", id)
	_, err = auth.AuthorizeSubject(ctx, "someone", "users:write:any")
	require.Equal(t, apperror.Forbidden, apperror.Resolve(err).Kind())
	copy, _ := auth.FromContext(ctx)
	copy.Permissions[0] = "users:write:any"
	again, _ := auth.FromContext(ctx)
	require.True(t, again.Can("users:read:any"))
}
