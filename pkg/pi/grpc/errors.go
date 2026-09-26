package grpc

import (
	"context"
	"errors"
	"strconv"

	"github.com/sllt/pi/pkg/pi/apperror"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MapError exposes only catalogued public messages; causes remain in server logs.
func MapError(err error) error {
	if err == nil {
		return nil
	}
	var typed *apperror.Error
	if !errors.As(err, &typed) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		var mapped interface{ GRPCStatus() *status.Status }
		if errors.As(err, &mapped) {
			for _, detail := range mapped.GRPCStatus().Details() {
				if info, ok := detail.(*errdetails.ErrorInfo); ok && info.Domain == "pi" {
					return mapped.GRPCStatus().Err()
				}
			}
		}
		if s, ok := status.FromError(err); ok {
			switch s.Code() {
			case codes.Unauthenticated:
				return status.Error(codes.Unauthenticated, "Unauthorized")
			case codes.PermissionDenied:
				return status.Error(codes.PermissionDenied, "Forbidden")
			case codes.InvalidArgument:
				return status.Error(codes.InvalidArgument, "invalid argument")
			case codes.NotFound:
				return status.Error(codes.NotFound, "not found")
			case codes.AlreadyExists:
				return status.Error(codes.AlreadyExists, "already exists")
			case codes.Unavailable:
				return status.Error(codes.Unavailable, "service unavailable")
			case codes.ResourceExhausted:
				return status.Error(codes.ResourceExhausted, "resource exhausted")
			case codes.Canceled:
				return status.Error(codes.Canceled, "request canceled")
			case codes.DeadlineExceeded:
				return status.Error(codes.DeadlineExceeded, "request timeout")
			case codes.Unimplemented:
				return status.Error(codes.Unimplemented, "unimplemented")
			}
		}
	}
	e := apperror.Resolve(err)
	code := codes.Internal
	switch e.Kind() {
	case apperror.InvalidArgument:
		code = codes.InvalidArgument
	case apperror.Unauthenticated:
		code = codes.Unauthenticated
	case apperror.Forbidden:
		code = codes.PermissionDenied
	case apperror.NotFound:
		code = codes.NotFound
	case apperror.Conflict:
		code = codes.AlreadyExists
	case apperror.Unavailable:
		code = codes.Unavailable
	case apperror.Canceled:
		code = codes.Canceled
	case apperror.DeadlineExceeded:
		code = codes.DeadlineExceeded
	}
	s := status.New(code, e.PublicMessage())
	with, detailErr := s.WithDetails(&errdetails.ErrorInfo{Reason: string(e.Kind()), Domain: "pi", Metadata: map[string]string{"code": strconv.Itoa(e.Code())}})
	if detailErr == nil {
		s = with
	}
	if len(e.Details()) > 0 {
		bad := &errdetails.BadRequest{}
		for _, d := range e.Details() {
			bad.FieldViolations = append(bad.FieldViolations, &errdetails.BadRequest_FieldViolation{Field: d.Field, Description: d.Rule})
		}
		if with, err := s.WithDetails(bad); err == nil {
			s = with
		}
	}
	return s.Err()
}

func ErrorUnaryInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	res, err := next(ctx, req)
	if err != nil {
		return nil, MapError(err)
	}
	return res, nil
}
func ErrorStreamInterceptor(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	return MapError(next(srv, stream))
}
