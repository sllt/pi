package cmd

import (
	"context"
	"os"
	"reflect"
	"strconv"
	"strings"
)

// Request is an abstraction over the actual command with flags. This abstraction is useful because it allows us
// to create cmd applications in same way we would create a HTTP server application.
// Pi's http.Request is another such abstraction.
type Request struct {
	flags  map[string]bool
	params map[string]string
	args   []string // positional arguments (non-flag arguments after command)
}

const trueString = "true"

// TODO - use statement to parse the request to populate the flags and params.

// NewRequest creates a Request from a list of arguments. This way we can simulate running a command without actually
// doing it. It makes the code more testable this way.
func NewRequest(args []string) *Request {
	r := Request{
		flags:  make(map[string]bool),
		params: make(map[string]string),
		args:   make([]string, 0),
	}

	const (
		argsLen1 = 1
		argsLen2 = 2
	)

	// Track which args are consumed as flag values
	skipNext := false

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "" {
			continue // This takes cares of cases where command has multiple space in between.
		}

		if skipNext {
			skipNext = false
			continue
		}

		// Non-flag argument (positional argument)
		if arg[0] != '-' {
			r.args = append(r.args, arg)
			continue
		}

		if len(arg) == 1 {
			continue
		}

		a := ""
		if arg[1] == '-' {
			a = arg[2:]
		} else {
			a = arg[1:]
		}

		switch values := strings.Split(a, "="); len(values) {
		case argsLen1:
			// Support -name value (space-separated)
			if i+1 < len(args) && len(args[i+1]) > 0 && args[i+1][0] != '-' {
				r.params[values[0]] = args[i+1]
				skipNext = true // Skip the next argument as it's the value
			} else {
				// Support -t -a etc. (flags without values)
				r.params[values[0]] = trueString
			}
		case argsLen2:
			// Support -a=b
			r.params[values[0]] = values[1]
		}
	}

	return &r
}

// Param returns the value of the parameter for key.
func (r *Request) Param(key string) string {
	return r.params[key]
}

// PathParam returns the value of the parameter for key. This is equivalent to Param.
func (r *Request) PathParam(key string) string {
	return r.params[key]
}

// Arg returns the positional argument at the given index (0-based, after command words).
// Returns empty string if index is out of range.
func (r *Request) Arg(index int) string {
	if index < 0 || index >= len(r.args) {
		return ""
	}
	return r.args[index]
}

// Args returns all positional arguments.
func (r *Request) Args() []string {
	return r.args
}

func (*Request) Context() context.Context {
	return context.Background()
}

func (*Request) HostName() (hostname string) {
	hostname, _ = os.Hostname()

	return hostname
}

// Params retrieves all values for a given query parameter key, including comma-separated values.
func (r *Request) Params(key string) []string {
	value, exists := r.params[key]
	if !exists {
		return []string{}
	}

	return strings.Split(value, ",")
}

func (r *Request) Bind(i any) error {
	// pointer to struct - addressable
	ps := reflect.ValueOf(i)
	// struct
	s := ps.Elem()

	if s.Kind() != reflect.Struct {
		return nil
	}

	for k, v := range r.params {
		f := s.FieldByName(k)
		// A Value can be changed only if it is addressable and not unexported struct field
		if !f.IsValid() || !f.CanSet() {
			continue
		}
		//nolint:exhaustive // Bind supports only basic field kinds.
		switch f.Kind() {
		case reflect.String:
			f.SetString(v)
		case reflect.Bool:
			if v == trueString {
				f.SetBool(true)
			}
		case reflect.Int:
			n, _ := strconv.Atoi(v)
			f.SetInt(int64(n))
		}
	}

	return nil
}
