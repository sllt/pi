package main

import (
	"github.com/sllt/pi/examples/grpc/grpc-streaming-server/server"
	"github.com/sllt/pi/pkg/pi"
)

func main() {
	app := pi.New()

	server.RegisterChatServiceServerWithPi(app, server.NewChatServicePiServer())

	app.Run()
}
