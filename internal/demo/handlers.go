package demo

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// statusClientClosedRequest is the non-standard code (used by nginx) for
// "the client went away before we answered". It is below 500, so metrics do
// not count it as a service error.
const statusClientClosedRequest = 499

// result is what a handler returns on success.
type result struct {
	Status int
	Body   any
}

// handlerFunc is business logic only: return a result, or an error. Logging,
// metrics, request IDs and status mapping are wrap's job.
type handlerFunc func(r *http.Request) (result, error)

func newRequestID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b) // crypto/rand.Read cannot return an error since Go 1.24
	return hex.EncodeToString(b)
}

// statusForError maps a handler error to the HTTP status the client sees.
func statusForError(err error) int {
	if errors.Is(err, ErrPoolExhausted) {
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

// wrap runs a handler, writes the response, emits exactly one log line and
// records the outcome in the metrics collector.
func (s *Server) wrap(endpoint string, h handlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := newRequestID()
		r = r.WithContext(WithRequestID(r.Context(), reqID))

		res, err := h(r)

		// If the request's own context is done, the client left (or timed out):
		// that is not a failure of this service, whatever error it caused.
		// The client left only if its context is done AND the handler failed
		// because of that. A genuine failure (such as pool exhaustion) stays a
		// failure even if the client also hung up at about the same time.
		clientGone := err != nil && r.Context().Err() != nil &&
			(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))

		status := res.Status
		switch {
		case clientGone:
			status = statusClientClosedRequest
		case err != nil:
			status = statusForError(err)
		}
		latency := ms(time.Since(start))

		w.Header().Set("X-Request-ID", reqID)
		switch {
		case clientGone:
			w.WriteHeader(status) // nobody is listening, so no body
		case err != nil:
			writeJSON(w, status, map[string]string{"error": http.StatusText(status), "request_id": reqID})
		default:
			writeJSON(w, status, res.Body)
		}

		attrs := []any{
			"request_id", reqID, "endpoint", endpoint, "method", r.Method,
			"status", status, "latency_ms", latency,
		}
		switch {
		case clientGone:
			s.logger.Info("request canceled by client", attrs...)
		case err != nil:
			s.logger.Error("request failed", append(attrs, "error", err.Error())...)
		default:
			s.logger.Info("request completed", attrs...)
		}
		s.metrics.Record(latency, status)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// handleHealth is deliberately shallow: it does not touch the database, so it
// keeps answering "ok" even when the service is failing real requests.
func (s *Server) handleHealth(r *http.Request) (result, error) {
	return result{Status: http.StatusOK, Body: map[string]string{"status": "ok"}}, nil
}

func (s *Server) handleProducts(r *http.Request) (result, error) {
	if err := s.db.Query(r.Context(), "select products"); err != nil {
		return result{}, fmt.Errorf("products: %w", err)
	}
	body := []map[string]any{{"id": 1, "name": "keyboard"}, {"id": 2, "name": "mouse"}}
	return result{Status: http.StatusOK, Body: body}, nil
}

func (s *Server) handleCheckout(r *http.Request) (result, error) {
	ctx := r.Context()
	if err := s.db.Query(ctx, "check inventory"); err != nil {
		return result{}, fmt.Errorf("checkout: inventory check: %w", err)
	}
	if err := s.db.Query(ctx, "insert order"); err != nil {
		return result{}, fmt.Errorf("checkout: insert order: %w", err)
	}
	return result{Status: http.StatusOK, Body: map[string]string{"status": "order_created"}}, nil
}
