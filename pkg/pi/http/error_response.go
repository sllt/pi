package http

import (
	"context"
	"errors"
	"net/http"
)

// ErrorResponse classifies an error without exposing wrapper or internal messages.
// Cancellation wins; otherwise errors.As selects the first status-bearing error
// in depth-first, left-to-right order (including errors.Join). Code and message
// come from that same error, never from an unrelated joined branch.
func ErrorResponse(err error) (status, code int, message string) {
	if err == nil {
		return http.StatusOK, 0, "ok"
	}
	if errors.Is(err, context.Canceled) {
		return StatusClientClosedRequest, StatusClientClosedRequest, "request canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusRequestTimeout, http.StatusRequestTimeout, "request timeout"
	}
	status, code, message = http.StatusInternalServerError, -1, "internal server error"
	var classified interface {
		error
		StatusCode() int
	}
	if errors.As(err, &classified) {
		if s := classified.StatusCode(); s >= 400 && s <= 599 {
			status = s
			code = s
		}
		if c, ok := classified.(CodeResponder); ok {
			code = c.Code()
		}
		if status < 500 {
			message = classified.Error()
		}
		if p, ok := classified.(interface{ PublicMessage() string }); ok {
			message = p.PublicMessage()
		}
		return status, code, message
	}
	var coded interface {
		error
		Code() int
	}
	if errors.As(err, &coded) {
		code = coded.Code()
	}
	return status, code, message
}
