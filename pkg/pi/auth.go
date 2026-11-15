package pi

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"

	grpcMiddleware "github.com/sllt/pi/pkg/pi/grpc/middleware"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/middleware"
)

// EnableBasicAuth enables basic authentication for the application.
//
// It takes a variable number of credentials as alternating username and password strings.
// An error is logged if an odd number of arguments is provided.
func (a *App) EnableBasicAuth(credentials ...string) {
	if a == nil {
		return
	}
	if len(credentials) == 0 {
		a.Logger().Error("No credentials provided for EnableBasicAuth. Proceeding without Authentication")
		return
	}

	if len(credentials)%2 != 0 {
		a.Logger().Error("Invalid number of arguments for EnableBasicAuth. Proceeding without Authentication")

		return
	}

	users := make(map[string]string)
	for i := 0; i < len(credentials); i += 2 {
		users[credentials[i]] = credentials[i+1]
	}

	provider := grpcMiddleware.BasicAuthProvider{Users: users}

	a.addAuthMiddleware(middleware.BasicAuthMiddleware(middleware.BasicAuthProvider{Users: users}),
		grpcMiddleware.BasicAuthUnaryInterceptor(provider), grpcMiddleware.BasicAuthStreamInterceptor(provider))
}

// EnableBasicAuthWithValidator enables basic authentication for HTTP and gRPC servers with a custom validator.
//
// The provided `validateFunc` is invoked for each authentication attempt with a request-scoped `*pi.Context`
// exposing application datasources, adapters, and headers/metadata.
func (a *App) EnableBasicAuthWithValidator(validateFunc func(ctx *Context, username, password string) bool) {
	if a == nil {
		return
	}
	if validateFunc == nil {
		a.Logger().Error("Nil validator provided for EnableBasicAuthWithValidator. Proceeding without Authentication")
		return
	}

	provider := grpcMiddleware.BasicAuthProvider{
		ValidateFuncWithContext: func(rpcCtx context.Context, username, password string) bool {
			return validateFunc(a.NewContext(rpcCtx), username, password)
		},
	}

	httpProvider := middleware.BasicAuthProvider{
		ValidateFuncWithRequest: func(r *http.Request, username, password string) bool {
			return validateFunc(a.newAuthHTTPContext(r), username, password)
		},
	}

	a.addAuthMiddleware(middleware.BasicAuthMiddleware(httpProvider),
		grpcMiddleware.BasicAuthUnaryInterceptor(provider), grpcMiddleware.BasicAuthStreamInterceptor(provider))
}

// EnableAPIKeyAuth enables API key authentication for the application.
//
// It requires at least one API key to be provided. The provided API keys will be used to authenticate requests.
func (a *App) EnableAPIKeyAuth(apiKeys ...string) {
	if a == nil {
		return
	}
	provider := grpcMiddleware.APIKeyAuthProvider{APIKeys: apiKeys}

	a.addAuthMiddleware(middleware.APIKeyAuthMiddleware(middleware.APIKeyAuthProvider{}, apiKeys...),
		grpcMiddleware.APIKeyAuthUnaryInterceptor(provider), grpcMiddleware.APIKeyAuthStreamInterceptor(provider))
}

// EnableAPIKeyAuthWithValidator enables API key authentication for HTTP and gRPC servers with a custom validator.
//
// The provided `validateFunc` receives a request-scoped `*pi.Context` exposing application datasources,
// adapters, and headers/metadata along with the API key.
func (a *App) EnableAPIKeyAuthWithValidator(validateFunc func(ctx *Context, apiKey string) bool) {
	if a == nil {
		return
	}
	if validateFunc == nil {
		a.Logger().Error("Nil validator provided for EnableAPIKeyAuthWithValidator. Proceeding without Authentication")
		return
	}

	provider := grpcMiddleware.APIKeyAuthProvider{
		ValidateFuncWithContext: func(rpcCtx context.Context, apiKey string) bool {
			return validateFunc(a.NewContext(rpcCtx), apiKey)
		},
	}

	httpProvider := middleware.APIKeyAuthProvider{
		ValidateFuncWithRequest: func(r *http.Request, apiKey string) bool {
			return validateFunc(a.newAuthHTTPContext(r), apiKey)
		},
	}

	a.addAuthMiddleware(middleware.APIKeyAuthMiddleware(httpProvider),
		grpcMiddleware.APIKeyAuthUnaryInterceptor(provider), grpcMiddleware.APIKeyAuthStreamInterceptor(provider))
}

func (a *App) newAuthHTTPContext(r *http.Request) *Context {
	var (
		rt       *runtimeKernel
		maxBytes int64
		validate func(any) error
	)
	if a != nil && a.kernel != nil {
		rt = a.kernel
		maxBytes = rt.maxBodyBytes
		validate = rt.validate
	}
	if r == nil {
		return newContext(nil, nil, rt)
	}
	return newContext(nil, piHTTP.NewRequestWithLimit(r, maxBytes, validate), rt)
}

// EnableOAuth configures OAuth middleware for the application.
//
// It registers a new HTTP service for fetching JWKS and sets up OAuth middleware
// with the given JWKS endpoint and refresh interval.
//
// The JWKS endpoint is used to retrieve JSON Web Key Sets for verifying tokens.
// The refresh interval specifies how often to refresh the token cache.
// We can define optional JWT claim validation settings, including issuer, audience, and expiration checks.
// Accepts jwt.ParserOption for additional parsing options:
// https://pkg.go.dev/github.com/golang-jwt/jwt/v4#ParserOption
func (a *App) EnableOAuth(jwksEndpoint string,
	refreshInterval int,
	options ...jwt.ParserOption,
) {
	if a == nil {
		return
	}
	a.AddHTTPService("pi_oauth", jwksEndpoint)

	oauthOption := middleware.OauthConfigs{
		Provider:        a.HTTPService("pi_oauth"),
		RefreshInterval: time.Second * time.Duration(refreshInterval),
	}

	var publicKeyProvider middleware.PublicKeyProvider
	if a.pureBuild {
		deferred := middleware.NewDeferredOAuth(oauthOption)
		a.OnStart(func(*Context) error {
			deferred.Start(a.intakeContext())
			return nil
		})
		a.registerMiddlewareCloser(deferred.Close)
		publicKeyProvider = deferred
	} else {
		publicKeyProvider = middleware.NewOAuth(oauthOption)
		if closer, ok := publicKeyProvider.(io.Closer); ok {
			a.registerMiddlewareCloser(closer.Close)
		}
	}

	a.addAuthMiddleware(middleware.OAuth(publicKeyProvider, options...),
		grpcMiddleware.OAuthUnaryInterceptor(publicKeyProvider, options...),
		grpcMiddleware.OAuthStreamInterceptor(publicKeyProvider, options...))
}

func (a *App) addAuthMiddleware(httpMW func(http.Handler) http.Handler,
	grpcUnary grpc.UnaryServerInterceptor, grpcStream grpc.StreamServerInterceptor) {
	if a.httpServer != nil {
		a.Use(httpMW)
	}

	if a.grpcServer != nil {
		if a.grpcServer.serverCreated {
			a.Logger().Error("cannot add auth interceptors after gRPC server has been created - call authentication methods before RegisterService or Run")
			return
		}
		a.grpcServer.addUnaryInterceptors(grpcUnary)
		a.grpcServer.addStreamInterceptors(grpcStream)
	}
}
