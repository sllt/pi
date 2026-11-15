package middleware

import (
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
)

// BasicAuthProvider represents a basic authentication provider.
type BasicAuthProvider struct {
	Users                   map[string]string
	ValidateFunc            func(username, password string) bool
	ValidateFuncWithRequest func(r *http.Request, username, password string) bool
}

var (
	errUserListEmpty = errors.New("user list is empty")
)

// NewBasicAuthProvider returns an instance of type AuthProvider interface.
func NewBasicAuthProvider(users map[string]string) (AuthProvider, error) {
	if len(users) == 0 {
		return nil, errUserListEmpty
	}

	return &BasicAuthProvider{Users: users}, nil
}

// NewBasicAuthProviderWithValidateFunc returns an instance of type AuthProvider interface.
func NewBasicAuthProviderWithValidateFunc(
	validateFunc func(r *http.Request, username, password string) bool,
) (AuthProvider, error) {
	if validateFunc == nil {
		return nil, errValidateFuncEmpty
	}

	return &BasicAuthProvider{ValidateFuncWithRequest: validateFunc}, nil
}

// ExtractAuthHeader retrieves & returns validated value from auth header.
func (a *BasicAuthProvider) ExtractAuthHeader(r *http.Request) (any, ErrorHTTP) {
	header, err := getAuthHeaderFromRequest(r, headerAuthorization, "Basic")
	if err != nil {
		return nil, err
	}

	userName, password, ok := parseBasicAuth(header)
	if !ok {
		return "", NewInvalidAuthorizationHeaderFormatError(headerAuthorization,
			"credentials should be in the format base64(username:password)")
	}

	if !a.validateCredentials(r, userName, password) {
		return nil, NewInvalidAuthorizationHeaderError(headerAuthorization)
	}

	return userName, nil
}

// GetAuthMethod returns authMethod Username.
func (*BasicAuthProvider) GetAuthMethod() AuthMethod {
	return Username
}

// parseBasicAuth extracts and decodes the username and password from the Authorization header.
func parseBasicAuth(authHeader string) (username, password string, ok bool) {
	if authHeader == "" {
		return "", "", false
	}

	payload, err := base64.StdEncoding.DecodeString(authHeader)
	if err != nil {
		return "", "", false
	}

	username, password, found := strings.Cut(string(payload), ":")
	if !found {
		return "", "", false
	}

	return username, password, true
}

// validateCredentials checks the provided username and password against the BasicAuthProvider.
func (a *BasicAuthProvider) validateCredentials(r *http.Request, username, password string) bool {
	switch {
	case a.ValidateFuncWithRequest != nil:
		return a.ValidateFuncWithRequest(r, username, password)
	case a.ValidateFunc != nil:
		return a.ValidateFunc(username, password)
	default:
		storedPass, ok := a.Users[username]
		if !ok {
			subtle.ConstantTimeCompare([]byte(password), []byte(dummyValue))

			return false
		}

		return subtle.ConstantTimeCompare([]byte(password), []byte(storedPass)) == 1
	}
}

// BasicAuthMiddleware creates a middleware function that enforces basic authentication using the provided BasicAuthProvider.
func BasicAuthMiddleware(basicAuthProvider BasicAuthProvider) func(handler http.Handler) http.Handler {
	return AuthMiddleware(&basicAuthProvider)
}
