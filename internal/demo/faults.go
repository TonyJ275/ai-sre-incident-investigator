package demo

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Fault makes a share of an endpoint's requests fail immediately with an
// application error, as a bug introduced by a deploy would.
type Fault struct {
	ErrorRate float64 // probability in [0,1] that a request fails
	Message   string  // the error text, exactly as it will appear in the logs
}

// SetFault activates a fault on an endpoint, replacing any existing one.
func (s *Server) SetFault(endpoint string, f Fault) error {
	switch {
	case !s.endpoints[endpoint]:
		return fmt.Errorf("demo: unknown endpoint %q", endpoint)
	case !(f.ErrorRate >= 0 && f.ErrorRate <= 1): // also rejects NaN
		return fmt.Errorf("demo: fault error rate must be within [0,1], got %v", f.ErrorRate)
	case f.ErrorRate > 0 && strings.TrimSpace(f.Message) == "":
		return errors.New("demo: a fault with a non-zero error rate needs a message")
	}
	s.faultMu.Lock()
	s.faults[endpoint] = f
	s.faultMu.Unlock()
	return nil
}

// ClearFault removes an endpoint's fault, if any.
func (s *Server) ClearFault(endpoint string) {
	s.faultMu.Lock()
	delete(s.faults, endpoint)
	s.faultMu.Unlock()
}

func (s *Server) fault(endpoint string) (Fault, bool) {
	s.faultMu.RLock()
	defer s.faultMu.RUnlock()
	f, ok := s.faults[endpoint]
	return f, ok
}

// withFaults wraps a handler so that, while a fault is active, the chosen
// share of requests fail immediately. The failure happens before the handler
// runs, so it never reaches the database: the fingerprint of a bug that
// breaks a request outright is errors with NORMAL latency and a healthy pool.
func (s *Server) withFaults(endpoint string, h handlerFunc) handlerFunc {
	return func(r *http.Request) (result, error) {
		if f, ok := s.fault(endpoint); ok && f.ErrorRate > 0 && s.rnd() < f.ErrorRate {
			return result{}, errors.New(f.Message)
		}
		return h(r)
	}
}
