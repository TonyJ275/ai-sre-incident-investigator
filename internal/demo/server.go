package demo

import (
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
)

// Server is the demo checkout service: HTTP routes on top of the simulated
// database. Logs and metrics are produced by wrap, not by the handlers.
type Server struct {
	logger  *slog.Logger // already carries the "service" attribute
	db      *DB
	metrics *Collector
	mux     *http.ServeMux

	endpoints map[string]bool // route names; fixed after NewServer, so safe to read without a lock
	rnd       func() float64  // randomness for faults; tests replace it

	faultMu sync.RWMutex
	faults  map[string]Fault
}

func NewServer(logger *slog.Logger, db *DB, metrics *Collector) *Server {
	s := &Server{
		logger: logger, db: db, metrics: metrics,
		mux:       http.NewServeMux(),
		endpoints: map[string]bool{},
		rnd:       rand.Float64,
		faults:    map[string]Fault{},
	}
	s.route("GET /healthz", "/healthz", s.handleHealth)
	s.route("GET /products", "/products", s.handleProducts)
	s.route("POST /checkout", "/checkout", s.handleCheckout)
	return s
}

// route registers a handler and remembers its endpoint name, so SetFault can
// reject names that do not exist.
func (s *Server) route(pattern, endpoint string, h handlerFunc) {
	s.endpoints[endpoint] = true
	s.mux.HandleFunc(pattern, s.wrap(endpoint, s.withFaults(endpoint, h)))
}

func (s *Server) Handler() http.Handler { return s.mux }
