# Testing REST APIs with Pi

Testing REST APIs ensures that your endpoints function correctly under various conditions. In Pi `v0.5.1+`, applications and tests wire dependencies explicitly via `pi.Build(...)` (`WithSQL`, `WithRedis`, `WithPubSub`, `WithFile`, `WithHTTPService`, `WithAdapter`) together with `pkg/pi/testkit` — without global state or monolithic mock containers.

## Example of Unit Testing a REST API Using Pi

Here is an `Add` handler that binds a JSON request body and inserts a book using `ctx.SQL()`:

```go
// main.go
package main

import (
	"github.com/sllt/pi/pkg/pi"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
)

type Book struct {
	ID    int    `json:"id"`
	ISBN  int    `json:"isbn"`
	Title string `json:"title"`
}

func Add(ctx *pi.Context) (any, error) {
	var book Book

	if err := ctx.Bind(&book); err != nil {
		ctx.Errorf("error in binding: %v", err)
		return nil, piHTTP.ErrorInvalidParam{Params: []string{"body"}}
	}

	res, err := ctx.SQL().ExecContext(ctx, `INSERT INTO books (title, isbn) VALUES (?, ?)`, book.Title, book.ISBN)
	if err != nil {
		return nil, err
	}

	return res.LastInsertId()
}
```

Mount a mock `sql.Client` (or any custom adapter) via `pi.Build` with `pi.Borrowed` ownership and exercise the router directly via `app.HTTPHandler()` and `testkit`:

```go
// main_test.go
package main

import (
	"database/sql"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/sllt/pi/pkg/pi"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/testkit"
)

func TestAdd(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockDB := piSQL.NewMockClient(ctrl)

	app, err := pi.Build(
		pi.WithConfig(testkit.MinimalConfig()),
		pi.WithSQL(mockDB, pi.Borrowed),
	)
	require.NoError(t, err)
	app.POST("/book", Add)

	t.Run("Invalid JSON Body", func(t *testing.T) {
		rec := testkit.DoJSON(t, app.HTTPHandler(), http.MethodPost, "/book", []byte(`title":"Book Title"}`))
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("Database Error", func(t *testing.T) {
		mockDB.EXPECT().
			ExecContext(gomock.Any(), `INSERT INTO books (title, isbn) VALUES (?, ?)`, "Book Title", 12345).
			Return(nil, sql.ErrConnDone)

		rec := testkit.DoJSON(t, app.HTTPHandler(), http.MethodPost, "/book", map[string]any{
			"title": "Book Title",
			"isbn":  12345,
		})
		require.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}
```

## Testing HTTP Handlers with Outbound `service.HTTP` Mocks

Outbound HTTP services implement the public `service.HTTP` interface and can be injected per service name using `pi.WithHTTPService(name, svc, pi.Borrowed)` (or tested against `httptest.NewServer` with `service.NewHTTPService`):

```go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/service"
	"github.com/sllt/pi/pkg/pi/testkit"
)

func OrderDetailsHandler(ctx *pi.Context) (any, error) {
	orderID := ctx.PathParam("id")
	if orderID == "" {
		return nil, errors.New("order ID is required")
	}

	paymentService := ctx.GetHTTPService("paymentService")
	paymentResp, err := paymentService.Get(ctx, "/payments/"+orderID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch payment details: %w", err)
	}
	defer paymentResp.Body.Close()

	var paymentData struct {
		Status string `json:"status"`
		Amount int    `json:"amount"`
	}
	paymentBody, err := io.ReadAll(paymentResp.Body)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(paymentBody, &paymentData); err != nil {
		return nil, err
	}

	shippingService := ctx.GetHTTPService("shippingService")
	shippingResp, err := shippingService.Get(ctx, "/shipping/"+orderID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch shipping details: %w", err)
	}
	defer shippingResp.Body.Close()

	var shippingData struct {
		Status   string `json:"status"`
		Tracking string `json:"tracking"`
	}
	shippingBody, err := io.ReadAll(shippingResp.Body)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(shippingBody, &shippingData); err != nil {
		return nil, err
	}

	return map[string]any{
		"order_id":        orderID,
		"payment_status":  paymentData.Status,
		"payment_amount":  paymentData.Amount,
		"shipping_status": shippingData.Status,
		"tracking_number": shippingData.Tracking,
	}, nil
}

func TestOrderDetailsHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	paymentMock := service.NewMockHTTP(ctrl)
	shippingMock := service.NewMockHTTP(ctrl)

	app, err := pi.Build(
		pi.WithConfig(testkit.MinimalConfig()),
		pi.WithHTTPService("paymentService", paymentMock, pi.Borrowed),
		pi.WithHTTPService("shippingService", shippingMock, pi.Borrowed),
	)
	require.NoError(t, err)
	app.GET("/orders/{id}", OrderDetailsHandler)

	paymentMock.EXPECT().
		Get(gomock.Any(), "/payments/12345", map[string]any(nil)).
		Return(&http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"status":"completed","amount":1500}`)),
		}, nil)

	shippingMock.EXPECT().
		Get(gomock.Any(), "/shipping/12345", map[string]any(nil)).
		Return(&http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"status":"in_transit","tracking":"TRACK123"}`)),
		}, nil)

	rec := testkit.Do(t, app.HTTPHandler(), http.MethodGet, "/orders/12345", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"payment_status":"completed"`)
	assert.Contains(t, rec.Body.String(), `"tracking_number":"TRACK123"`)
}
```

### Summary

- **Explicit Dependency Wiring**: Build isolated test instances with `pi.Build(pi.WithConfig(testkit.MinimalConfig()), ...)` and `pi.Borrowed` mocks or fakes.
- **Custom Adapters**: Mount mocks or in-memory adapters via `pi.WithAdapter("name", fakeAdapter, pi.Borrowed)` and retrieve them via `pi.Use[T](ctx)`.
- **Outbound HTTP Services**: Register per-service `service.HTTP` mocks with `pi.WithHTTPService(name, mockSvc, pi.Borrowed)`.
