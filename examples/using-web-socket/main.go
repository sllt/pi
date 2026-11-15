package main

import (
	"github.com/sllt/pi/pkg/pi"
)

func main() {
	app := pi.New()

	app.WebSocket("/ws", WSHandler)

	app.Run()
}

func WSHandler(ctx *pi.Context) (any, error) {
	var message string

	err := ctx.Bind(&message)
	if err != nil {
		ctx.Errorf("Error binding message: %v", err)
		return nil, err
	}

	ctx.Infof("Received message: %s", message)

	err = ctx.WriteMessageToSocket("Hello! Pi")
	if err != nil {
		return nil, err
	}

	return message, nil
}
