package grpc

import (
	"fmt"
	"reflect"

	"github.com/sllt/pi/pkg/pi/apperror"
	"google.golang.org/protobuf/proto"
)

// BindRequest clones into the same protobuf message type. Adapters translate
// protobuf messages to business types explicitly; struct field offsets are never used.
func BindRequest(destination any, source proto.Message) error {
	dest, ok := destination.(proto.Message)
	if !ok || dest == nil || source == nil || reflect.ValueOf(dest).Kind() != reflect.Ptr || reflect.ValueOf(source).Kind() != reflect.Ptr || reflect.ValueOf(dest).IsNil() || reflect.ValueOf(source).IsNil() || reflect.TypeOf(dest) != reflect.TypeOf(source) {
		return apperror.New(apperror.Internal, 500, "internal server error").WithCause(fmt.Errorf("protobuf Bind requires non-nil matching message pointers"))
	}
	if dest == source {
		return nil
	}
	proto.Reset(dest)
	proto.Merge(dest, source)
	return nil
}
