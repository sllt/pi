# Websockets

WebSockets provide a full-duplex communication channel over a single, long-lived connection, making them ideal for 
real-time applications like chat, notifications, and live updates. Pi provides a convenient way to integrate websockets
into your application. By leveraging Pi's WebSocket support and customizable upgrader options,
users can efficiently manage real-time communication in your applications.

## Usage in Pi

Here is a simple example to set up a WebSocket server in Pi:

```go
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
		ctx.Logger.Errorf("Error binding message: %v", err)
		return nil, err
	}

	ctx.Logger.Infof("Received message: %s", message)

	return message, nil
}
```

## Configuration Options
Pi allows us to customize the WebSocket upgrader with several options. We can set these options using the
`websocket.NewWSUpgrader` function. Here is the list of options we can apply to your websocket upgrader using Pi.

- `HandshakeTimeout (WithHandshakeTimeout)`: Sets the handshake timeout.
- `ReadBufferSize (WithReadBufferSize)`: Sets the size of the read buffer.
- `WriteBufferSize (WithWriteBufferSize)`: Sets the size of the write buffer.
- `Subprotocols (WithSubprotocols)`: Sets the supported sub-protocols.
- `Error (WithError)`:  Sets a custom error handler.
- `CheckOrigin (WithCheckOrigin)`: Sets a custom origin check function.
- `Compression (WithCompression)`:  Enables compression.

## Writing Messages

Pi provides the `WriteMessageToSocket` method to send messages to the underlying websocket connection in a thread-safe way. The data parameter can be a string, []byte, or any struct that can be marshaled to JSON.

## Example:
We can configure the Upgrader by creating a chain of option functions provided by Pi.

```go
package main

import (
	"time"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/websocket"
)

func main() {
	app := pi.New()

	wsUpgrader := websocket.NewWSUpgrader(
		websocket.WithHandshakeTimeout(5*time.Second), // Set handshake timeout
		websocket.WithReadBufferSize(2048),            // Set read buffer size
		websocket.WithWriteBufferSize(2048),           // Set write buffer size
		websocket.WithSubprotocols("chat", "binary"),  // Specify subprotocols
		websocket.WithCompression(),                   // Enable compression
	)

	app.OverrideWebsocketUpgrader(wsUpgrader)

	app.WebSocket("/ws", WSHandler)

	app.Run()
}

func WSHandler(ctx *pi.Context) (any, error) {
	var message string

	err := ctx.Bind(&message)
	if err != nil {
		ctx.Logger.Errorf("Error binding message: %v", err)
		return nil, err
	}

	ctx.Logger.Infof("Received message: %s", message)

	err = ctx.WriteMessageToSocket("Hello! Pi")
	if err != nil {
		return nil, err
	}

	return message, nil
}
```
> #### Check out the example on how to read/write through a WebSocket in Pi: [Visit GitHub](https://github.com/kite-dev/pi/blob/main/examples/using-web-socket/main.go)

## Inter-Service WebSocket Communication

Pi also supports Inter-Service WebSocket Communication, enabling seamless communication between services using WebSocket connections.
This feature is particularly useful for microservices architectures where services need to exchange real-time data.

## Key Methods: 

1. **AddWSService**
This method registers a WebSocket service and establishes a persistent connection to the specified service. It also supports automatic reconnection in case of connection failures.

**Parameters:**


- `serviceName (string)`: A unique name for the WebSocket service.
- `url (string)`: The WebSocket URL of the target service.
- `headers ( map[string][]string)`: HTTP headers to include in the WebSocket handshake.
-  `enableReconnection (bool)`: A boolean to enable automatic reconnection.
- `retryInterval (time.Duration)`: The interval between reconnection attempts.

2. **WriteMessageToService**
This method sends a message to a WebSocket connection associated with a specific service.

**Parameters:**

- `serviceName (string)`: The name of the WebSocket service.
- `data (any)`: The message to send. It can be a string, []byte, or any struct that can be marshaled to JSON.

## Usage in Pi

```go
package main

import (
	"time"
	"github.com/sllt/pi/pkg/pi"
)

func main() {
	app := pi.New()

	// Add a WebSocket service
	err := app.AddWSService("notification-service", "ws://notifications.example.com/ws", nil, true, 5*time.Second)
	if err != nil {
		app.Logger.Errorf("Failed to add WebSocket service: %v", err)
		return
	}

	// Example route to send a message to the notification service
	app.POST("/send-notification", func(ctx *pi.Context) (any, error) {
		message := map[string]string{
			"title":   "New Message",
			"content": "You have a new notification!",
		}

		err := ctx.WriteMessageToService("notification-service", message)
		if err != nil {
			ctx.Logger.Errorf("Failed to send message: %v", err)
			return nil, err
		}

		return "Notification sent successfully!", nil
	})

	app.Run()
}
```
