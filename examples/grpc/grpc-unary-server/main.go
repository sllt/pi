package main

import (
	"github.com/sllt/pi/examples/grpc/grpc-unary-server/server"
	"github.com/sllt/pi/pkg/pi"
)

func main() {
	app := pi.New()

	server.RegisterHelloServerWithPi(app, server.NewHelloPiServer())

	app.Run()
}
