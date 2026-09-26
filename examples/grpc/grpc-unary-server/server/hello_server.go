// versions:
// 	pi-cli v0.6.0
// 	github.com/sllt/pi v1.37.0
// 	source: hello.proto

package server

import (
	"fmt"

	"github.com/sllt/pi/pkg/pi"
)

// Register the gRPC service in your app using the following code in your main.go:
//
// server.RegisterHelloServerWithPi(app, &server.NewHelloPiServer())
//
// HelloPiServer defines the gRPC server implementation.
// Customize the struct with required dependencies and fields as needed.

type HelloPiServer struct {
	health *healthServer
}

func (s *HelloPiServer) SayHello(ctx *pi.Context) (any, error) {
	request := HelloRequest{}

	err := ctx.Bind(&request)
	if err != nil {
		return nil, err
	}

	name := request.Name
	if name == "" {
		name = "World"
	}

	//Performing HealthCheck
	//res, err := s.health.Check(ctx, &grpc_health_v1.HealthCheckRequest{
	//	Service: "Hello",
	//})
	//ctx.Log(res.String())

	// Setting the serving status
	//s.health.SetServingStatus(ctx, "Hello", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	return &HelloResponse{
		Message: fmt.Sprintf("Hello %s!", name),
	}, nil
}
