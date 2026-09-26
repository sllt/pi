package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/mock"
)

type mockMetrics struct {
	mock.Mock
}

func (m *mockMetrics) IncrementCounter(ctx context.Context, name string, labels ...string) {
	m.Called(ctx, name, labels)
}

func (m *mockMetrics) DeltaUpDownCounter(ctx context.Context, name string, value float64, labels ...string) {
	m.Called(ctx, name, value, labels)
}

func (m *mockMetrics) RecordHistogram(ctx context.Context, name string, value float64, labels ...string) {
	m.Called(ctx, name, value, labels)
}

func (m *mockMetrics) SetGauge(name string, value float64, _ ...string) {
	m.Called(name, value)
}

func TestMetrics(t *testing.T) {
	mockMetrics := &mockMetrics{}

	mockMetrics.On("RecordHistogram", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	router := chi.NewRouter()
	router.Use(Metrics(mockMetrics))
	router.Get("/test", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", http.NoBody)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	mockMetrics.AssertCalled(t, "RecordHistogram", mock.Anything, "app_http_response", mock.Anything,
		[]string{"path", "/test", "method", "GET", "status", "200"})
}

func TestMetrics_StaticFile(t *testing.T) {
	mockMetrics := &mockMetrics{}

	mockMetrics.On("RecordHistogram", mock.Anything, "app_http_response", mock.Anything,
		[]string{"path", "/static/example.js", "method", "GET", "status", "200"}).Return(nil)

	// Create a temporary static file for the test
	tempDir := t.TempDir()
	staticFilePath := tempDir + "/example.js"

	err := os.WriteFile(staticFilePath, []byte("console.log('test');"), 0600)
	if err != nil {
		t.Errorf("failed to create temporary static file: %v", err)
	}

	router := chi.NewRouter()
	router.Use(Metrics(mockMetrics))
	router.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.Dir(tempDir))))

	req := httptest.NewRequest(http.MethodGet, "/static/example.js", http.NoBody)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	mockMetrics.AssertCalled(t, "RecordHistogram", mock.Anything, "app_http_response", mock.Anything,
		[]string{"path", "/static/example.js", "method", "GET", "status", "200"})
}

func TestMetrics_StaticFileWithQueryParam(t *testing.T) {
	mockMetrics := &mockMetrics{}

	mockMetrics.On("RecordHistogram", mock.Anything, "app_http_response", mock.Anything,
		[]string{"path", "/static/example.js", "method", "GET", "status", "200"}).Return(nil)

	// Create a temporary static file for the test
	tempDir := t.TempDir()
	staticFilePath := tempDir + "/example.js"

	err := os.WriteFile(staticFilePath, []byte("console.log('test');"), 0600)
	if err != nil {
		t.Errorf("failed to create temporary static file: %v", err)
	}

	router := chi.NewRouter()
	router.Use(Metrics(mockMetrics))
	router.Handle("/static/*", http.StripPrefix("/static/", http.FileServer(http.Dir(tempDir))))

	req := httptest.NewRequest(http.MethodGet, "/static/example.js?v=42", http.NoBody)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	mockMetrics.AssertCalled(t, "RecordHistogram", mock.Anything, "app_http_response", mock.Anything,
		[]string{"path", "/static/example.js", "method", "GET", "status", "200"})
}
