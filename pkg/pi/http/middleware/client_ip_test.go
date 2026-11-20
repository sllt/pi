package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTrustedProxies(t *testing.T) {
	prefixes, err := ParseTrustedProxies([]string{"", "  ", "10.0.0.0/8", " 127.0.0.1 ", "::1", "::ffff:192.168.1.5", "::ffff:172.16.0.0/108"})
	require.NoError(t, err)
	require.Len(t, prefixes, 5)
	assert.Equal(t, "10.0.0.0/8", prefixes[0].String())
	assert.Equal(t, "127.0.0.1/32", prefixes[1].String())
	assert.Equal(t, "::1/128", prefixes[2].String())
	assert.Equal(t, "192.168.1.5/32", prefixes[3].String())
	assert.Equal(t, "172.16.0.0/12", prefixes[4].String())

	empty, err := ParseTrustedProxies([]string{"", "   "})
	require.NoError(t, err)
	assert.Nil(t, empty)

	_, err = ParseTrustedProxies([]string{"10.0.0.0/8", "not-an-ip"})
	require.Error(t, err)

	_, err = ParseTrustedProxies([]string{"10.0.0.0/99"})
	require.Error(t, err)
}

func TestExtractClientIP_SpoofingWithoutTrustedProxies(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.RemoteAddr = "198.51.100.7:44321"
	req.Header.Set("X-Forwarded-For", "203.0.113.1")
	req.Header.Set("X-Real-IP", "203.0.113.2")

	// Empty trusted proxies: never trust headers.
	assert.Equal(t, "198.51.100.7", ExtractClientIP(req, nil))

	// Trusted proxies configured, but peer IP is untrusted: still never trust headers.
	trusted, err := ParseTrustedProxies([]string{"10.0.0.0/8", "127.0.0.1"})
	require.NoError(t, err)
	assert.Equal(t, "198.51.100.7", ExtractClientIP(req, trusted))
}

func TestExtractClientIP_RightToLeftMultiHopAndMultipleHeaders(t *testing.T) {
	trusted, err := ParseTrustedProxies([]string{"10.0.0.0/8", "127.0.0.1"})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.RemoteAddr = "127.0.0.1:8080"
	// Spoofed 1.1.1.1 on the left; real client 203.0.113.45 appended by edge proxy 10.0.0.5,
	// then internal hop 10.0.0.9 in a second X-Forwarded-For header.
	req.Header.Add("X-Forwarded-For", "1.1.1.1, 203.0.113.45, 10.0.0.5")
	req.Header.Add("X-Forwarded-For", "10.0.0.9")

	assert.Equal(t, "203.0.113.45", ExtractClientIP(req, trusted))
}

func TestExtractClientIP_IPv4MappedIPv6Normalized(t *testing.T) {
	trusted, err := ParseTrustedProxies([]string{"10.0.0.0/8"})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.RemoteAddr = "[::ffff:10.0.0.1]:54321"
	req.Header.Set("X-Forwarded-For", "::ffff:203.0.113.8, ::ffff:10.0.0.2")

	assert.Equal(t, "203.0.113.8", ExtractClientIP(req, trusted))
}

func TestExtractClientIP_AllTrustedFallbackToXRealIPAndLeftmost(t *testing.T) {
	trusted, err := ParseTrustedProxies([]string{"10.0.0.0/8", "127.0.0.1"})
	require.NoError(t, err)

	t.Run("all XFF trusted falls back to valid X-Real-IP", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.RemoteAddr = "127.0.0.1:1234"
		req.Header.Set("X-Forwarded-For", "10.0.0.2, 10.0.0.3")
		req.Header.Set("X-Real-IP", "::ffff:203.0.113.55")
		assert.Equal(t, "203.0.113.55", ExtractClientIP(req, trusted))
	})

	t.Run("all XFF trusted without valid X-Real-IP falls back to leftmost valid XFF", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.RemoteAddr = "127.0.0.1:1234"
		req.Header.Set("X-Forwarded-For", "invalid, 10.0.0.2, 10.0.0.3")
		req.Header.Set("X-Real-IP", "not-an-ip")
		assert.Equal(t, "10.0.0.2", ExtractClientIP(req, trusted))
	})

	t.Run("invalid token in XFF breaks right-to-left walk and fails closed against spoofed left IP", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.RemoteAddr = "127.0.0.1:1234"
		req.Header.Set("X-Forwarded-For", "1.1.1.1, unknown, 10.0.0.2")
		assert.Equal(t, "10.0.0.2", ExtractClientIP(req, trusted))

		req2 := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req2.RemoteAddr = "10.0.0.2:1234"
		req2.Header.Set("X-Forwarded-For", "1.1.1.1, unknown")
		assert.Equal(t, "10.0.0.2", ExtractClientIP(req2, trusted))
	})

	t.Run("no valid headers falls back to peer IP", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.RemoteAddr = "127.0.0.1"
		assert.Equal(t, "127.0.0.1", ExtractClientIP(req, trusted))
	})

	t.Run("nil request and empty RemoteAddr", func(t *testing.T) {
		assert.Equal(t, "", ExtractClientIP(nil, trusted))
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.RemoteAddr = ""
		assert.Equal(t, "", ExtractClientIP(req, trusted))
	})
}
