package grpc

import (
	"context"
	"strings"

	"github.com/sllt/pi/pkg/pi/apperror"
	"github.com/sllt/pi/pkg/pi/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type TokenVerifier func(context.Context, string) (auth.Principal, error)

func authenticate(ctx context.Context, verify TokenVerifier) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 {
		return nil, apperror.New(apperror.Unauthenticated, 401, "Unauthorized")
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || verify == nil {
		return nil, apperror.New(apperror.Unauthenticated, 401, "Unauthorized")
	}
	p, err := verify(ctx, parts[1])
	if err != nil || p.Subject == "" {
		return nil, apperror.New(apperror.Unauthenticated, 401, "Unauthorized").WithCause(err)
	}
	return auth.WithPrincipal(ctx, p), nil
}
func NewPrincipalUnaryInterceptor(verify TokenVerifier, publicMethods ...string) grpc.UnaryServerInterceptor {
	public := map[string]bool{}
	for _, method := range publicMethods {
		public[method] = true
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if public[info.FullMethod] {
			return next(ctx, req)
		}
		authenticated, err := authenticate(ctx, verify)
		if err != nil {
			return nil, MapError(err)
		}
		return next(authenticated, req)
	}
}

type principalStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *principalStream) Context() context.Context { return s.ctx }
func NewPrincipalStreamInterceptor(verify TokenVerifier) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		ctx, err := authenticate(stream.Context(), verify)
		if err != nil {
			return MapError(err)
		}
		return next(srv, &principalStream{ServerStream: stream, ctx: ctx})
	}
}
