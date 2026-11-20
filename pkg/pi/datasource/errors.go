package datasource

import (
	"errors"
	"fmt"
	"net/http"
	"runtime"
)

var (
	// ErrAdapterNotFound indicates no registered adapter or core resource matches the lookup.
	ErrAdapterNotFound = errors.New("datasource: adapter not found")
	// ErrAmbiguousAdapter indicates two or more registered adapters/resources satisfy the lookup type.
	ErrAmbiguousAdapter = errors.New("datasource: multiple adapters match requested type")
	// ErrInvalidAdapterType indicates an unsupported lookup type (such as empty interface `any`) or incompatible type assertion.
	ErrInvalidAdapterType = errors.New("datasource: invalid or incompatible adapter type")
	// ErrTransactionalDatasource indicates a migration attempted to resolve an underlying non-transactional pool while an active transaction/pipeline owns the connection.
	ErrTransactionalDatasource = errors.New("datasource: use transactional Datasource field instead of underlying pool during migration")
)

// ErrorDB represents an error specific to database operations.
type ErrorDB struct {
	Err     error
	Message string
}

func (e ErrorDB) Error() string {
	switch {
	case e.Message == "" && e.Err == nil:
		return ""
	case e.Message == "":
		return e.Err.Error()
	case e.Err == nil:
		return e.Message
	default:
		return fmt.Sprintf("%s: %s", e.Message, e.Err.Error())
	}
}

func (e ErrorDB) Unwrap() error {
	return e.Err
}

// WithStack adds a stack trace to the Error.
func (e ErrorDB) WithStack() ErrorDB {
	if e.Err == nil {
		return e
	}
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	stack := make([]uintptr, n)
	copy(stack, pcs[:n])
	e.Err = &stackError{
		err: e.Err,
		pcs: stack,
	}
	return e
}

type stackError struct {
	err error
	pcs []uintptr
}

func (s *stackError) Error() string {
	return s.err.Error()
}

func (s *stackError) Unwrap() error {
	return s.err
}

func (s *stackError) Format(st fmt.State, verb rune) {
	switch verb {
	case 'v':
		if st.Flag('+') {
			_, _ = fmt.Fprintf(st, "%+v\n", s.err)
			frames := runtime.CallersFrames(s.pcs)
			for {
				frame, more := frames.Next()
				_, _ = fmt.Fprintf(st, "%s\n\t%s:%d\n", frame.Function, frame.File, frame.Line)
				if !more {
					break
				}
			}
			return
		}
		fallthrough
	case 's':
		_, _ = st.Write([]byte(s.Error()))
	case 'q':
		_, _ = fmt.Fprintf(st, "%q", s.Error())
	}
}

func (ErrorDB) StatusCode() int {
	return http.StatusInternalServerError
}

// ErrorRecordNotFound represents the scenario where no records are found in the DB for the given ID.
type ErrorRecordNotFound ErrorDB

// StatusCode implementation on the ErrorRecordNotFound is an aberration
// since the errors in datasource package should not have anything to do with HTTP status codes.
func (ErrorRecordNotFound) StatusCode() int {
	return http.StatusNotFound
}
