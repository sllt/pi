package middleware

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	validKey1 string = "valid-key-1"
	validKey2 string = "valid-key-2"
)

func TestMain(m *testing.M) {
	os.Setenv("PI_TELEMETRY", "false")
	m.Run()
}

func Test_NewAPIKeyAuthProvider(t *testing.T) {
	testCases := []struct {
		apiKeys  []string
		provider AuthProvider
		err      error
	}{
		{err: errAPIKeyEmpty},
		{apiKeys: make([]string, 0), err: errAPIKeyEmpty},
		{apiKeys: []string{validKey1}, provider: &APIKeyAuthProvider{APIKeys: []string{validKey1}}, err: nil},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			provider, err := NewAPIKeyAuthProvider(tc.apiKeys)
			assert.Equal(t, tc.provider, provider)
			assert.Equal(t, tc.err, err)
		})
	}
}

func Test_NewAPIKeyAuthProviderWithValidateFunc(t *testing.T) {
	validateFunc := func(_ *http.Request, _ string) bool {
		return true
	}
	testCases := []struct {
		validateFunc func(*http.Request, string) bool
		err          error
	}{
		{err: errValidateFuncEmpty},
		{validateFunc: validateFunc},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			authProvider, err := NewAPIKeyAuthProviderWithValidateFunc(tc.validateFunc)
			assert.Equal(t, tc.err, err)

			if err != nil {
				return
			}

			apiAuthProvider, ok := authProvider.(*APIKeyAuthProvider)
			require.True(t, ok)
			assert.NotNil(t, apiAuthProvider.ValidateFuncWithRequest)
			assert.Nil(t, apiAuthProvider.ValidateFunc)
			assert.Empty(t, apiAuthProvider.APIKeys)
		})
	}
}

func Test_extractAuthHeader(t *testing.T) {
	provider := APIKeyAuthProvider{APIKeys: []string{validKey1, validKey2}}
	testCases := []struct {
		header   string
		err      ErrorHTTP
		response any
	}{
		{err: ErrorMissingAuthHeader{key: headerXAPIKey}},
		{header: "some-value", err: ErrorInvalidAuthorizationHeader{key: headerXAPIKey}},
		{header: validKey1, response: validKey1},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			req.Header.Set(headerXAPIKey, tc.header)
			response, err := provider.ExtractAuthHeader(req)
			assert.Equal(t, tc.response, response)
			assert.Equal(t, tc.err, err)
		})
	}
}

func Test_authMethod(t *testing.T) {
	authProvider := APIKeyAuthProvider{}
	assert.Equal(t, APIKey, authProvider.GetAuthMethod())
}

func Test_validateAPIKey(t *testing.T) {
	validateFuncSuccess := func(_ string) bool {
		return true
	}
	validateFuncFail := func(_ string) bool {
		return false
	}

	validateFuncReqPass := func(r *http.Request, _ string) bool {
		return r != nil && r.Header.Get("X-Tenant") == "acme"
	}
	validateFuncReqFail := func(_ *http.Request, _ string) bool {
		return false
	}

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.Header.Set("X-Tenant", "acme")

	apiKeys := []string{validKey1, validKey2}

	testCases := []struct {
		validateFuncReq func(*http.Request, string) bool
		validateFunc    func(string) bool
		apiKeys         []string
		apiKey          string
		result          bool
	}{
		{validateFuncReq: validateFuncReqPass, validateFunc: validateFuncFail, apiKeys: apiKeys, apiKey: validKey1, result: true},
		{validateFuncReq: validateFuncReqPass, validateFunc: validateFuncFail, apiKey: validKey1, result: true},
		{validateFuncReq: validateFuncReqPass, validateFunc: validateFuncSuccess, apiKeys: apiKeys, apiKey: validKey1, result: true},
		{validateFuncReq: validateFuncReqPass, validateFunc: validateFuncSuccess, apiKey: validKey1, result: true},
		{validateFuncReq: validateFuncReqPass, apiKeys: apiKeys, apiKey: validKey1, result: true},
		{validateFuncReq: validateFuncReqPass, apiKey: validKey1, result: true},

		{validateFuncReq: validateFuncReqFail, validateFunc: validateFuncFail, apiKeys: apiKeys, apiKey: validKey1, result: false},
		{validateFuncReq: validateFuncReqFail, validateFunc: validateFuncFail, apiKey: validKey1, result: false},
		{validateFuncReq: validateFuncReqFail, validateFunc: validateFuncSuccess, apiKeys: apiKeys, apiKey: validKey1, result: false},
		{validateFuncReq: validateFuncReqFail, validateFunc: validateFuncSuccess, apiKey: validKey1, result: false},
		{validateFuncReq: validateFuncReqFail, apiKeys: apiKeys, apiKey: validKey1, result: false},
		{validateFuncReq: validateFuncReqFail, apiKey: validKey1, result: false},

		{validateFunc: validateFuncSuccess, apiKeys: apiKeys, apiKey: validKey1, result: true},
		{validateFunc: validateFuncFail, apiKeys: apiKeys, apiKey: validKey1, result: false},
		{validateFunc: validateFuncSuccess, apiKey: validKey1, result: true},
		{validateFunc: validateFuncFail, apiKey: validKey1, result: false},

		{apiKeys: apiKeys, apiKey: validKey1, result: true},
		{apiKeys: apiKeys, apiKey: "wrong-key", result: false},
		{apiKey: validKey1, result: false},
	}

	for i, tc := range testCases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			authProvider := APIKeyAuthProvider{
				ValidateFunc:            tc.validateFunc,
				ValidateFuncWithRequest: tc.validateFuncReq,
				APIKeys:                 tc.apiKeys,
			}
			result := authProvider.validateAPIKey(req, tc.apiKey)
			assert.Equal(t, tc.result, result)
		})
	}
}

func Test_APIKeyAuthMiddleware(t *testing.T) {
	t.Logf("Test_APIKeyAuthMiddleware")
}
