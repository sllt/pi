# HTTP Auth Middleware

This Pi example demonstrates the usage of auth middlewares in Pi:
- API Key Auth
- Basic Auth
- OAuth

## Setup

### Basic Auth Middleware Setup

```go
a := pi.New()

a.EnableBasicAuthWithValidator(func(ctx *pi.Context, username, password string) bool {
    return username == "username" && password == "password"
})
```

### API Key Auth Middleware Setup

```go
a := pi.New()

a.EnableAPIKeyAuthWithValidator(func(ctx *pi.Context, apiKey string) bool {
    return apiKey == "valid-api-key"
})
```

### OAuth Middleware Setup

```go
a := pi.New()
a.EnableOAuth("<JWKS-Endpoint>", 10)
```

## Execution

```console
go run main.go
```
