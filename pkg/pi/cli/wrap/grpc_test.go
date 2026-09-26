package wrap

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWrapperRegenerationProtectsImplementation(t *testing.T) {
	dir := t.TempDir()
	protoPath := filepath.Join(dir, "sample.proto")
	out := filepath.Join(dir, "sample")
	require.NoError(t, os.WriteFile(protoPath, []byte(`syntax="proto3"; package example.v1; option go_package="example.com/app/sample"; message Request{} message Response{} service Sample { rpc Get(Request) returns (Response); }`), 0644))
	_, err := BuildGRPCPiServer(protoPath, out)
	require.NoError(t, err)
	impl := filepath.Join(out, "sample_server.go")
	require.NoError(t, os.WriteFile(impl, []byte("package sample\n// handwritten\n"), 0644))
	_, err = BuildGRPCPiServer(protoPath, out)
	require.NoError(t, err)
	data, err := os.ReadFile(impl)
	require.NoError(t, err)
	require.Contains(t, string(data), "handwritten")
	data, err = os.ReadFile(filepath.Join(out, "sample_pi.go"))
	require.NoError(t, err)
	require.Contains(t, string(data), "example.v1.Sample")
	other := filepath.Join(dir, "other.proto")
	require.NoError(t, os.WriteFile(other, []byte(`syntax="proto3"; package example.v1; option go_package="example.com/app/sample"; message OtherRequest{} message OtherResponse{} service Other { rpc Get(OtherRequest) returns (OtherResponse); }`), 0644))
	_, err = BuildGRPCPiServer(other, out)
	require.ErrorContains(t, err, "another proto")
	_, err = os.Stat(filepath.Join(out, "other_pi.go"))
	require.True(t, os.IsNotExist(err))
	protected := filepath.Join(out, "request_pi.go")
	require.NoError(t, os.WriteFile(protected, []byte("package sample\n// user owned\n"), 0644))
	_, err = BuildGRPCPiServer(protoPath, out)
	require.Error(t, err)
	data, err = os.ReadFile(protected)
	require.NoError(t, err)
	require.Contains(t, string(data), "user owned")
}
