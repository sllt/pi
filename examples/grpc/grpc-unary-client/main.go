package main

import (
	"github.com/sllt/pi/examples/grpc/grpc-unary-client/client"
	"github.com/sllt/pi/pkg/pi"
)

func main() {
	app := pi.New()

	// Create a gRPC client for the Hello service
	helloGRPCClient, err := client.NewHelloPiClient(app.Config.Get("GRPC_SERVER_HOST"), app.Metrics())
	if err != nil {
		app.Logger().Errorf("Failed to create Hello gRPC client: %v", err)
		return
	}

	greet := NewGreetHandler(helloGRPCClient)

	app.GET("/hello", greet.Hello)

	app.Run()
}

type GreetHandler struct {
	helloGRPCClient client.HelloPiClient
}

func NewGreetHandler(helloClient client.HelloPiClient) *GreetHandler {
	return &GreetHandler{
		helloGRPCClient: helloClient,
	}
}

func (g GreetHandler) Hello(ctx *pi.Context) (any, error) {
	userName := ctx.Param("name")

	if userName == "" {
		ctx.Log("Name parameter is empty, defaulting to 'World'")
		userName = "World"
	}

	// HealthCheck to SayHello Service.
	// res, err := g.helloGRPCClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: "Hello"})
	// if err != nil {
	//	return nil, err
	// } else if res.Status == grpc_health_v1.HealthCheckResponse_NOT_SERVING {
	//	 ctx.Error("Hello Service is down")
	//	 return nil, fmt.Errorf("Hello Service is down")
	// }

	// Make a gRPC call to the Hello service
	helloResponse, err := g.helloGRPCClient.SayHello(ctx, &client.HelloRequest{Name: userName})
	if err != nil {
		return nil, err
	}

	return helloResponse, nil
}
