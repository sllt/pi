package cmd

import "github.com/sllt/pi/pkg/pi/apperror"

// ErrorResult is safe for JSON/terminal output; it never serializes the cause.
type ErrorResult struct {
	Kind    apperror.Kind     `json:"kind"`
	Code    int               `json:"code"`
	Message string            `json:"message"`
	Details []apperror.Detail `json:"details,omitempty"`
}

func MapError(err error) (int, *ErrorResult) {
	if err == nil {
		return 0, nil
	}
	e := apperror.Resolve(err)
	exit := 1
	switch e.Kind() {
	case apperror.InvalidArgument:
		exit = 2
	case apperror.Canceled:
		exit = 130
	case apperror.DeadlineExceeded:
		exit = 124
	}
	return exit, &ErrorResult{Kind: e.Kind(), Code: e.Code(), Message: e.PublicMessage(), Details: e.Details()}
}
