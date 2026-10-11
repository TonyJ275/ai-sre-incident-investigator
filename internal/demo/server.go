package demo

import (
	"log/slog"
	"net/http"
)

// Server is the demo checkout service: HTTP routes on top of the simulated
// database. Logs and metrics are produced by wrap, not by the handlers.
type Server struct {
	logger  *slog.Logger // already carries the "service" attribute
	db      *DB
	metrics *Collector
	mux     *http.ServeMux
}

func NewServer(logger *slog.Logger, db *DB, metrics *Collector) *Server {
	s := &Server{logger: logger, db: db, metrics: metrics}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.wrap("/healthz", s.handleHealth))
	mux.HandleFunc("GET /products", s.wrap("/products", s.handleProducts))
	mux.HandleFunc("POST /checkout", s.wrap("/checkout", s.handleCheckout))
	s.mux = mux
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }
