package solr

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLifecycle_FromEnv_ConnectContext_Close(t *testing.T) {
	_, err := FromEnv(nil)
	require.ErrorIs(t, err, errInvalidConfig)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"responseHeader":{"status":0}}`))
	}))
	defer ts.Close()

	parts := strings.Split(ts.Listener.Addr().String(), ":")
	client, err := FromEnv(func(k string) string {
		switch k {
		case "SOLR_HOST":
			return parts[0]
		case "SOLR_PORT":
			return parts[1]
		default:
			return ""
		}
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	_, err = client.HealthCheck(context.Background())
	require.ErrorIs(t, err, errSolrNotConnected)

	require.NoError(t, client.ConnectContext(context.Background()))
	require.NoError(t, client.Close())
	require.NoError(t, client.Close())

	_, err = client.HealthCheck(context.Background())
	require.ErrorIs(t, err, errSolrNotConnected)
}
