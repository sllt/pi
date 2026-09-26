package response

import (
	"context"
	"io"
)

// Result chooses an operation's success status independently of its HTTP verb.
// Result takes precedence over a nested special response's status, but never an error.
type Result struct {
	Data       any
	StatusCode int
	Headers    map[string]string
}

func OK(data any) Result       { return Result{Data: data, StatusCode: 200} }
func Created(data any) Result  { return Result{Data: data, StatusCode: 201} }
func Accepted(data any) Result { return Result{Data: data, StatusCode: 202} }
func NoContent() Result        { return Result{StatusCode: 204} }

// Stream transfers response writing to Run after the Handler has returned.
// Run executes synchronously and must honor cancellation. It owns its output
// format; after headers commit, errors cannot be replaced by a JSON envelope.
type Stream struct {
	StatusCode  int
	ContentType string
	Run         func(context.Context, io.Writer) error
}

// Handled marks a response already owned by an adapter (for example a hijack).
// Only trusted adapters should use it. A returned error still takes precedence.
type Handled struct{}
