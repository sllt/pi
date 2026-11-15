package middleware

import (
	"crypto/subtle"
	"errors"
	"net/http"
)

var (
	errAPIKeyEmpty = errors.New("api keys list is empty")
)

// APIKeyAuthProvider represents an API key authentication provider.
type APIKeyAuthProvider struct {
	ValidateFunc            func(apiKey string) bool
	ValidateFuncWithRequest func(r *http.Request, apiKey string) bool
	APIKeys                 []string
}

// NewAPIKeyAuthProvider instantiates an instance of type AuthProvider interface.
func NewAPIKeyAuthProvider(apiKeys []string) (AuthProvider, error) {
	if len(apiKeys) == 0 {
		return nil, errAPIKeyEmpty
	}

	return &APIKeyAuthProvider{APIKeys: apiKeys}, nil
}

// NewAPIKeyAuthProviderWithValidateFunc instantiates an instance of type AuthProvider interface.
func NewAPIKeyAuthProviderWithValidateFunc(
	validateFunc func(r *http.Request, apiKey string) bool,
) (AuthProvider, error) {
	if validateFunc == nil {
		return nil, errValidateFuncEmpty
	}

	return &APIKeyAuthProvider{ValidateFuncWithRequest: validateFunc}, nil
}

func (a *APIKeyAuthProvider) ExtractAuthHeader(r *http.Request) (any, ErrorHTTP) {
	header, err := getAuthHeaderFromRequest(r, headerXAPIKey, "")
	if err != nil {
		return nil, err
	}

	if !a.validateAPIKey(r, header) {
		return nil, NewInvalidAuthorizationHeaderError(headerXAPIKey)
	}

	return header, nil
}

func (*APIKeyAuthProvider) GetAuthMethod() AuthMethod {
	return APIKey
}

// validateAPIKey verifies the given apiKey as per the configured APIKeyAuthProvider.
func (a *APIKeyAuthProvider) validateAPIKey(r *http.Request, apiKey string) bool {
	switch {
	case a.ValidateFuncWithRequest != nil:
		return a.ValidateFuncWithRequest(r, apiKey)
	case a.ValidateFunc != nil:
		return a.ValidateFunc(apiKey)
	default:
		for _, key := range a.APIKeys {
			if subtle.ConstantTimeCompare([]byte(apiKey), []byte(key)) == 1 {
				return true
			}
		}

		subtle.ConstantTimeCompare([]byte(apiKey), []byte(dummyValue))

		return false
	}
}

// APIKeyAuthMiddleware creates a middleware function that enforces API key authentication based on the provided API
// keys or a validation function.
func APIKeyAuthMiddleware(a APIKeyAuthProvider, apiKeys ...string) func(handler http.Handler) http.Handler {
	a.APIKeys = apiKeys
	return AuthMiddleware(&a)
}
