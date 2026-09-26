package grpc

import (
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"testing"
)

func TestBindRequestChecksTypeAndCopiesMessages(t *testing.T) {
	source, err := structpb.NewStruct(map[string]any{"key": "original"})
	require.NoError(t, err)
	dest := new(structpb.Struct)
	require.NoError(t, BindRequest(dest, source))
	source.Fields["key"] = structpb.NewStringValue("changed")
	require.Equal(t, "original", dest.Fields["key"].GetStringValue())
	for _, bad := range []any{nil, (*structpb.Struct)(nil), new(wrapperspb.StringValue), &struct{ Key string }{}} {
		require.Error(t, BindRequest(bad, source))
	}
	require.Error(t, BindRequest(dest, (*structpb.Struct)(nil)))
}
