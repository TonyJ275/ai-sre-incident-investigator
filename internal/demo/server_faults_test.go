package demo

import (
	"math"
	"net/http"
	"testing"
)

const bugMessage = "checkout: pricing: unexpected nil discount rule"

func TestFaultFailsFastWithoutTouchingThePool(t *testing.T) {
	srv, db, c, buf := testServer(exhaustedCfg())
	srv.rnd = func() float64 { return 0 } // always below the error rate
	if err := srv.SetFault("/checkout", Fault{ErrorRate: 0.5, Message: bugMessage}); err != nil {
		t.Fatal(err)
	}
	holder := holdConnection(t, db) // the pool is full: a real DB call would fail with 503

	w := do(srv, http.MethodPost, "/checkout")
	<-holder

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (an application error, not pool exhaustion)", w.Code)
	}
	logs := logEntries(t, buf)
	if len(logs) != 1 {
		t.Fatalf("want exactly the handler's log line, got %+v", logs)
	}
	e := logs[0]
	if e.Level != "ERROR" || e.Msg != "request failed" || e.Status != 500 ||
		e.Endpoint != "/checkout" || e.Error != bugMessage {
		t.Errorf("unexpected log entry: %+v", e)
	}
	if s := flush(c); s.Requests != 1 || s.Errors != 1 {
		t.Errorf("metrics = %+v, want 1 request and 1 error", s)
	}
}

func TestFaultRespectsTheErrorRate(t *testing.T) {
	srv, _, _, _ := testServer(fastCfg())
	draws, i := []float64{0.1, 0.9}, 0
	srv.rnd = func() float64 { v := draws[i]; i++; return v }
	if err := srv.SetFault("/products", Fault{ErrorRate: 0.5, Message: "boom"}); err != nil {
		t.Fatal(err)
	}

	if w := do(srv, http.MethodGet, "/products"); w.Code != http.StatusInternalServerError {
		t.Errorf("draw 0.1 < rate 0.5: status = %d, want 500", w.Code)
	}
	if w := do(srv, http.MethodGet, "/products"); w.Code != http.StatusOK {
		t.Errorf("draw 0.9 >= rate 0.5: status = %d, want 200", w.Code)
	}
}

func TestFaultOnlyAffectsItsEndpoint(t *testing.T) {
	srv, _, _, _ := testServer(fastCfg())
	srv.rnd = func() float64 { return 0 }
	if err := srv.SetFault("/checkout", Fault{ErrorRate: 1, Message: bugMessage}); err != nil {
		t.Fatal(err)
	}

	if w := do(srv, http.MethodGet, "/products"); w.Code != http.StatusOK {
		t.Errorf("/products = %d, want 200", w.Code)
	}
	if w := do(srv, http.MethodPost, "/checkout"); w.Code != http.StatusInternalServerError {
		t.Errorf("/checkout = %d, want 500", w.Code)
	}
}

func TestClearFaultRestoresService(t *testing.T) {
	srv, _, _, _ := testServer(fastCfg())
	srv.rnd = func() float64 { return 0 }
	if err := srv.SetFault("/checkout", Fault{ErrorRate: 1, Message: bugMessage}); err != nil {
		t.Fatal(err)
	}
	if w := do(srv, http.MethodPost, "/checkout"); w.Code != http.StatusInternalServerError {
		t.Fatalf("fault not active: %d", w.Code)
	}

	srv.ClearFault("/checkout")

	if w := do(srv, http.MethodPost, "/checkout"); w.Code != http.StatusOK {
		t.Errorf("after ClearFault status = %d, want 200", w.Code)
	}
}

func TestSetFaultValidation(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		fault    Fault
		wantErr  bool
	}{
		{"valid", "/checkout", Fault{ErrorRate: 0.3, Message: "x"}, false},
		{"rate 1 is allowed", "/checkout", Fault{ErrorRate: 1, Message: "x"}, false},
		{"rate 0 needs no message", "/checkout", Fault{}, false},
		{"unknown endpoint (a typo must not silently do nothing)", "/chekout", Fault{ErrorRate: 0.3, Message: "x"}, true},
		{"negative rate", "/checkout", Fault{ErrorRate: -0.1, Message: "x"}, true},
		{"rate above 1", "/checkout", Fault{ErrorRate: 1.1, Message: "x"}, true},
		{"NaN rate", "/checkout", Fault{ErrorRate: math.NaN(), Message: "x"}, true},
		{"missing message", "/checkout", Fault{ErrorRate: 0.3}, true},
		{"blank message", "/checkout", Fault{ErrorRate: 0.3, Message: "  "}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, _, _, _ := testServer(fastCfg())
			if err := srv.SetFault(tc.endpoint, tc.fault); (err != nil) != tc.wantErr {
				t.Errorf("SetFault error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
