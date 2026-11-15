package datasource

import (
	stdErrors "errors"
	"net/http"

	"github.com/pkg/errors"
)

var (
	// ErrAdapterNotFound indicates no registered adapter or core resource matches the lookup.
	ErrAdapterNotFound = stdErrors.New("datasource: adapter not found")
	// ErrAmbiguousAdapter indicates two or more registered adapters/resources satisfy the lookup type.
	ErrAmbiguousAdapter = stdErrors.New("datasource: multiple adapters match requested type")
	// ErrInvalidAdapterType indicates an unsupported lookup type (such as empty interface `any`) or incompatible type assertion.
	ErrInvalidAdapterType = stdErrors.New("datasource: invalid or incompatible adapter type")
	// ErrTransactionalDatasource indicates a migration attempted to resolve an underlying non-transactional pool while an active transaction/pipeline owns the connection.
	ErrTransactionalDatasource = stdErrors.New("datasource: use transactional Datasource field instead of underlying pool during migration")
)

// ErrorDB represents an error specific to database operations.
type ErrorDB struct {
	Err     error
	Message string
}

func (e ErrorDB) Error() string {
	switch {
	case e.Message == "":
		return e.Err.Error()
	case e.Err == nil:
		return e.Message
	default:
		return errors.Wrap(e.Err, e.Message).Error()
	}
}

// WithStack adds a stack trace to the Error.
func (e ErrorDB) WithStack() ErrorDB {
	e.Err = errors.WithStack(e.Err)
	return e
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
