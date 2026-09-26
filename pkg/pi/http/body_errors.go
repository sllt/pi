package http

type ErrorPayloadTooLarge struct{}

func (ErrorPayloadTooLarge) Error() string   { return "request body too large" }
func (ErrorPayloadTooLarge) StatusCode() int { return 413 }

type ErrorMalformedBody struct{}

func (ErrorMalformedBody) Error() string   { return "invalid request body" }
func (ErrorMalformedBody) StatusCode() int { return 400 }
