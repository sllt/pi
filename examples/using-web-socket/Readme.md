# Using WebSocket in Pi

This example demonstrates how to create and handle a WebSocket connection using the Pi framework.
It covers establishing the connection, receiving messages from the client, and sending responses back to the client in real time.

---

## Overview

The `/ws` endpoint in this example:

* Accepts incoming WebSocket connections.
* Reads and logs messages sent by the client.
* Sends a fixed greeting message back to the client (`"Hello! Pi"`).
* Returns the received message as part of the response.

This is useful for building **real-time applications** such as chat systems, dashboards, and live notifications.

---

## How to Run

1. **Clone the repository** and navigate to the example folder:

   ```bash
   git clone https://github.com/kite-dev/pi.git
   cd pi/examples/using-web-socket
   ```

2. **Start the application**:

   ```bash
   go run main.go
   ```

   This will start the server on:

   ```
   ws://localhost:8001/ws
   ```

---

## How It Works

### main.go

```go
app := pi.New()
app.WebSocket("/ws", WSHandler)
app.Run()
```

* Creates a new Pi app.
* Registers the `/ws` route for WebSocket connections.
* Starts the server.

#### WSHandler

* Binds the incoming WebSocket message to a string.
* Logs the received message.
* Sends `"Hello! Pi"` back to the client.
* Returns the received message.

---

## Testing

The example includes a test file `main_test.go` which:

* Starts the WebSocket server.
* Connects using a `gorilla/websocket` client.
* Sends a test message.
* Reads the server’s response.
* Asserts that the response matches the expected message.

Run the test:

```bash
go test -v
```

Expected output:

```
=== RUN   Test_WebSocket_Success
--- PASS: Test_WebSocket_Success (0.10s)
PASS
```

---

## Example Usage

Using [wscat](https://github.com/websockets/wscat):

```bash
npm install -g wscat
wscat -c ws://localhost:8001/ws
> Hello from Client
< Hello! Pi
```

