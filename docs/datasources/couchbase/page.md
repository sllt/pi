# Couchbase

## Configuration

To connect to `Couchbase`, you need to provide the following environment variables and use it:
- `HOST`: The hostname or IP address of your Couchbase server.
- `USER`: The username for connecting to the database.
- `PASSWORD`: The password for the specified user.
- `BUCKET`: Top level container

## Setup

Pi supports injecting `Couchbase` that implements the following interface. Any driver that implements the interface can be
added using the `app.AddCouchbase()` method, and users can use Couchbase across the application with `pi.Context`.

```go
type Couchbase interface {
    Get(ctx context.Context, key string, result any) error

    Insert(ctx context.Context, key string, document, result any) error

    Upsert(ctx context.Context, key string, document any, result any) error

    Remove(ctx context.Context, key string) error

    Query(ctx context.Context, statement string, params map[string]any, result any) error

    AnalyticsQuery(ctx context.Context, statement string, params map[string]any, result any) error
}
```

Users can easily inject a driver that supports this interface, providing usability without compromising the extensibility to use multiple databases.
Don't forget to serup the Couchbase cluster in Couchbase Web Console first. [Follow for more details](https://docs.couchbase.com/server/current/install/getting-started-docker.html#section_jvt_zvj_42b).
To begin using Couchbase in your Pi application, you need to import the Couchbase datasource package:

```shell
go get github.com/sllt/pi/pkg/pi/datasource/couchbase@latest
```

### Example

Here is an example of how to use the Couchbase datasource in a Pi application:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "github.com/sllt/pi/pkg/pi"
    "github.com/sllt/pi/pkg/pi/datasource/couchbase"
)

type User struct {
    ID   string `json:"id"`
    Name string `json:"name"`
    Age  int    `json:"age"`
}

func main() {
    // Create a new Pi application
    a := pi.New()

    // Add the Couchbase datasource to the application
    a.AddCouchbase(couchbase.New(&couchbase.Config{
        Host:     app.Config.Get("HOST"),
        User:     app.Config.Get("USER"),
        Password: app.Config.Get("PASSWORD"),
        Bucket:   app.Config.Get("BUCKET"),
    }))

    // Add the routes
    a.GET("/users/{id}", getUser)
    a.POST("/users", createUser)
	a.DELETE("/users/{id}", deleteUser)

    // Run the application
    a.Run()
}

func getUser(c *pi.Context) (any, error) {
    // Get the user ID from the URL path
    id := c.PathParam("id")

    // Get the user from Couchbase
    var user User
    if err := c.Couchbase.Get(c, id, &user); err != nil {
        return nil, err
    }

    return user, nil
}

func createUser(c *pi.Context) (any, error) {
    // Get the user from the request body
    var user User
    if err := c.Bind(&user); err != nil {
        return nil, err
    }

    // Insert the user into Couchbase
    if err := c.Couchbase.Insert(c, user.ID, user, nil); err != nil {
        return nil, err
    }

    return "user created successfully", nil
}

func deleteUser(c *pi.Context) (any, error) {
	// Get the user ID from the URL path
	id := c.PathParam("id")

	// Remove the user from Couchbase
	if err := c.Couchbase.Remove(c, id); err != nil {
		return nil, err
	}

	return "user deleted successfully", nil
}
```
