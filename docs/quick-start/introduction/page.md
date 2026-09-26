# Prerequisite

- Go 1.24 or above.
  To check the Go version, use the following command `go version`.

- Prior familiarity with Golang syntax is essential. {% new-tab-link title="Golang Tour" href="https://tour.golang.org/" /%} is highly recommended as it has an excellent guided tour.

## Write your first Pi API

Let's start by initializing the {% new-tab-link title="go module" href="https://go.dev/ref/mod" /%} by using the following command.

```bash
go mod init github.com/example
```

Add {% new-tab-link title="pi" href="https://github.com/kite-dev/pi" /%} package to the project using the following command.

```bash
go get github.com/sllt/pi
```

This code snippet showcases the creation of a simple Pi application that defines a route and serves a response.
You can add this code to your main.go file.

```go
package main

import "github.com/sllt/pi/pkg/pi"

func main() {
	// initialize pi object
	app := pi.New()

	// register route greet
	app.GET("/greet", func(ctx *pi.Context) (any, error) {
		return "Hello World!", nil
	})

	// Runs the server, it will listen on the default port 8000.
	// it can be over-ridden through configs
	app.Run()
}
```

Before starting the server, run the following command in your terminal to ensure you have downloaded and synchronized all required dependencies for your project.

`go mod tidy`

Once the dependencies are synchronized, start the Pi server using the following command:

`go run main.go`

This would start the server at 8000 port, `/greet` endpoint can be accessed from your browser at {% new-tab-link title="http://localhost:8000/greet" href="http://localhost:8000/greet" /%}, you would be able to see the output as following with _Status Code 200_ as per REST Standard.

```json
{ "data": "Hello World!" }
```

## Understanding the example

The `hello-world` server involves three essential steps:

1. **Creating Pi Server:**

   When `pi.New()` is called, it initializes the framework and handles various setup tasks like initializing logger, metrics, datasources, etc., based on the configs.

   _This single line is a standard part of all Pi servers._

2. **Attaching a Handler to a Path:**

   In this step, the server is instructed to associate an HTTP request with a specific handler function. This is achieved through `app.GET("/greet", HandlerFunction)`, where _GET /greet_ maps to HandlerFunction. Likewise, `app.POST("/todo", ToDoCreationHandler)` links a _POST_ request to the `/todo` endpoint with _ToDoCreationHandler_.

   **Good To Know**

> In Go, functions are first-class citizens, allowing easy handler definition and reference.
> HTTP Handler functions should follow the `func(ctx *pi.Context) (any, error)` signature.
> They take a context as input, returning two values: the response data and an error (set to `nil` when there is no error).

Pi {% new-tab-link  newtab=false title="context" href="/docs/references/context" /%} `ctx *pi.Context` serves as a wrapper for requests, responses, and dependencies, providing various functionalities.

3. **Starting the server**

   When `app.Run()` is called, it configures, initiates, and runs the HTTP server, middlewares. It manages essential features such as routes for health check endpoints, metrics server, favicon etc. It starts the server on the default port 8000.
