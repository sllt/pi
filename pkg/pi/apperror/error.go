// Package apperror defines transport-independent application errors.
package apperror

import (
	"context"
	"errors"
)

type Kind string

const (
	InvalidArgument  Kind = "invalid_argument"
	Unauthenticated  Kind = "unauthenticated"
	Forbidden        Kind = "forbidden"
	NotFound         Kind = "not_found"
	Conflict         Kind = "conflict"
	Unavailable      Kind = "unavailable"
	Internal         Kind = "internal"
	Canceled         Kind = "canceled"
	DeadlineExceeded Kind = "deadline_exceeded"
)

// Detail describes a public field/rule violation, never its submitted value.
type Detail struct {
	Field string `json:"field"`
	Rule  string `json:"rule"`
}
type Error struct {
	kind    Kind
	code    int
	message string
	cause   error
	details []Detail
}

func New(kind Kind, code int, publicMessage string) *Error {
	return &Error{kind: kind, code: code, message: publicMessage}
}
func (e *Error) Kind() Kind            { return e.kind }
func (e *Error) Code() int             { return e.code }
func (e *Error) PublicMessage() string { return e.message }
func (e *Error) Unwrap() error         { return e.cause }
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t != nil && e != nil && e.kind == t.kind && e.code == t.code
}
func (e *Error) Error() string {
	if e.cause != nil {
		return e.message + ": " + e.cause.Error()
	}
	return e.message
}
func (e *Error) Details() []Detail            { return append([]Detail(nil), e.details...) }
func (e *Error) WithCause(cause error) *Error { copy := *e; copy.cause = cause; return &copy }
func (e *Error) WithDetails(details ...Detail) *Error {
	copy := *e
	copy.details = append([]Detail(nil), details...)
	return &copy
}

// Resolve prioritizes cancellation, then the first typed application error in
// errors.As traversal order, including Join. Unknown causes remain private.
func Resolve(err error) *Error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return New(Canceled, 499, "request canceled").WithCause(err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return New(DeadlineExceeded, 408, "request timeout").WithCause(err)
	}
	var app *Error
	if errors.As(err, &app) {
		return app
	}
	return New(Internal, -1, "internal server error").WithCause(err)
}
