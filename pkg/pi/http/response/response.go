package response

import (
	"net/http"
)

type Response struct {
	Data    any               `json:"data"`
	Meta    map[string]any    `json:"meta,omitempty"`
	Headers map[string]string `json:"-"`
}

func (resp Response) SetCustomHeaders(w http.ResponseWriter) {
	for key, value := range resp.Headers {
		w.Header().Set(key, value)
	}
}
