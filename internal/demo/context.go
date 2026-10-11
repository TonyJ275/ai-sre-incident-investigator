package demo

import "context"

// requestIDKey is an unexported type, so no other package can create a
// colliding context key.
type requestIDKey struct{}

// WithRequestID returns a context that carries the request ID, so code deep
// in the call chain (like the DB) can include it in its log lines.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestIDFromContext returns the request ID, or "" if there is none.
func RequestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}
