package http

import (
	"context"
	"errors"
	"net/http"

	"github.com/sllt/pi/pkg/pi/apperror"
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
	selected := firstClassifiedError(err)
	if app, ok := selected.(*apperror.Error); ok {
		return applicationStatus(app.Kind()), app.Code(), app.PublicMessage()
	}
	var classified interface {
		error
		StatusCode() int
	}
	if selected != nil && errors.As(selected, &classified) {
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

func applicationStatus(kind apperror.Kind) int {
	switch kind {
	case apperror.InvalidArgument:
		return 400
	case apperror.Unauthenticated:
		return 401
	case apperror.Forbidden:
		return 403
	case apperror.NotFound:
		return 404
	case apperror.Conflict:
		return 409
	case apperror.Unavailable:
		return 503
	case apperror.Canceled:
		return 499
	case apperror.DeadlineExceeded:
		return 408
	default:
		return 500
	}
}

// Preserve one depth-first selection across legacy and new error types. Looking
// up each interface separately can accidentally combine unrelated Join branches.
func firstClassifiedError(err error) error {
	if err == nil {
		return nil
	}
	switch err.(type) {
	case *apperror.Error, interface {
		error
		StatusCode() int
	}:
		return err
	}
	if custom, ok := err.(interface{ As(any) bool }); ok {
		var app *apperror.Error
		if custom.As(&app) && app != nil {
			return app
		}
		var legacy interface {
			error
			StatusCode() int
		}
		if custom.As(&legacy) && legacy != nil {
			return legacy
		}
	}
	switch e := err.(type) {
	case interface{ Unwrap() error }:
		return firstClassifiedError(e.Unwrap())
	case interface{ Unwrap() []error }:
		for _, child := range e.Unwrap() {
			if match := firstClassifiedError(child); match != nil {
				return match
			}
		}
	}
	return nil
}

func ErrorDetails(err error) []apperror.Detail {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	if e, ok := firstClassifiedError(err).(*apperror.Error); ok {
		return e.Details()
	}
	return nil
}
