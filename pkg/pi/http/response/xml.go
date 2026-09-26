package response

// XML represents a response that should be sent as XML without JSON encoding.
// If ContentType is empty, Responder defaults it to application/xml.
type XML struct {
	Content     []byte
	ContentType string

	// StatusCode overrides Pi's default success HTTP status code when set to a valid HTTP status.
	// If not set (0) or invalid, Pi uses its existing status selection logic.
	StatusCode int
}
