package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"google.golang.org/grpc/status"
)

//go:embed web
var webFS embed.FS

func (d *Dash) routes(listen string) http.Handler {
	static, _ := fs.Sub(webFS, "web")
	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(static))
	mux.HandleFunc("GET /api/stream", d.handleStream)
	mux.HandleFunc("GET /api/job", d.handleJob)
	mux.HandleFunc("POST /api/submit", d.handleSubmit)
	mux.HandleFunc("POST /api/load", d.handleLoad)
	mux.HandleFunc("POST /api/cluster/launch", d.handleLaunch)
	mux.HandleFunc("POST /api/cluster/stop", d.handleStop)
	mux.HandleFunc("POST /api/cluster/scale", d.handleScale)
	mux.HandleFunc("POST /api/fault", d.handleFault)
	mux.HandleFunc("POST /api/scenario", d.handleScenario)
	host, _, _ := net.SplitHostPort(listen)
	return guard(host, mux)
}

// guard keeps other websites out. The dashboard can stop containers, so a
// page elsewhere must not be able to drive it from the user's browser:
// requests must name a loopback host (or the configured listen host),
// which defeats DNS rebinding, and writes must be same-origin JSON, which a
// cross-site form cannot send without a CORS preflight the server never
// grants.
func guard(listenHost string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		host = strings.Trim(host, "[]")
		if !isLoopback(host) && host != listenHost {
			http.Error(w, "forbidden host", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodGet {
			if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != "application/json" {
				http.Error(w, "expected application/json", http.StatusUnsupportedMediaType)
				return
			}
			if o := r.Header.Get("Origin"); o != "" {
				if u, err := url.Parse(o); err != nil || u.Host != r.Host {
					http.Error(w, "cross-origin request refused", http.StatusForbidden)
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (d *Dash) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ch, cancel := d.hub.subscribe()
	defer cancel()
	b, _ := json.Marshal(d.initMessage())
	_, _ = w.Write([]byte("event: init\ndata: " + string(b) + "\n\n"))
	flusher.Flush()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return // too slow; the browser reconnects
			}
			if _, err := w.Write(msg); err != nil {
				return
			}
			flusher.Flush()
		case <-ping.C:
			_, _ = w.Write([]byte(": ping\n\n"))
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	msg := err.Error()
	if st, ok := status.FromError(err); ok {
		msg = st.Code().String() + ": " + st.Message()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return false
	}
	return true
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, errNoCluster), errors.Is(err, errNoDocker), errors.Is(err, errBusy):
		return http.StatusConflict
	}
	if _, ok := status.FromError(err); ok {
		return http.StatusBadGateway
	}
	return http.StatusBadRequest
}

// handleJob returns a job's live state and history, with its lease spans.
func (d *Dash) handleJob(w http.ResponseWriter, r *http.Request) {
	c := d.jobClient()
	if c == nil {
		writeErr(w, http.StatusConflict, errNoCluster)
		return
	}
	id := r.URL.Query().Get("id")
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	resp, err := c.GetJob(ctx, &relayv1.GetJobRequest{JobId: id})
	if err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	clock := d.tracker.offset()
	j := resp.GetJob()
	events := make([]map[string]any, 0, len(resp.GetHistory()))
	for _, e := range resp.GetHistory() {
		events = append(events, map[string]any{"at": clock.local(e.GetAtMs()), "kind": e.GetKind(),
			"worker": e.GetWorkerId(), "token": e.GetLeaseToken(), "detail": e.GetDetail()})
	}
	writeJSON(w, map[string]any{
		"id": j.GetId(), "type": j.GetType(), "payload": string(j.GetPayload()), "status": statusName(j.GetStatus()),
		"attempts": j.GetAttempts(), "max_attempts": j.GetMaxAttempts(), "idempotency_key": j.GetIdempotencyKey(),
		"lease_owner": j.GetLeaseOwner(), "lease_token": j.GetLeaseToken(), "lease_expires_at": clock.local(j.GetLeaseExpiresAtMs()),
		"created_at": clock.local(j.GetCreatedAtMs()), "finished_at": clock.local(j.GetFinishedAtMs()),
		"last_error": j.GetLastError(), "result": string(j.GetResult()), "trace_parent": j.GetTraceParent(),
		"events": events, "spans": leaseSpans(j.GetId(), j.GetType(), resp.GetHistory(), clock.local),
	})
}

func (d *Dash) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Type           string `json:"type"`
		Payload        string `json:"payload"`
		MaxAttempts    int32  `json:"max_attempts"`
		IdempotencyKey string `json:"idempotency_key"`
		Count          int    `json:"count"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if req.Count < 1 || req.Count > 500 {
		writeErr(w, http.StatusBadRequest, errors.New("count must be between 1 and 500"))
		return
	}
	if req.Payload != "" && !json.Valid([]byte(req.Payload)) {
		writeErr(w, http.StatusBadRequest, errors.New("payload must be JSON (the built-in handlers decode it)"))
		return
	}
	var ids []string
	dups := 0
	for range req.Count {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		resp, err := d.submit(ctx, &relayv1.SubmitJobRequest{Type: req.Type, Payload: []byte(req.Payload),
			MaxAttempts: req.MaxAttempts, IdempotencyKey: req.IdempotencyKey}, "manual", true)
		cancel()
		if err != nil {
			writeErr(w, statusFor(err), err)
			return
		}
		ids = append(ids, resp.GetJobId())
		if resp.GetDuplicate() {
			dups++
		}
	}
	text := "Submitted " + plural(float64(len(ids)), req.Type+" job", req.Type+" jobs") + " by hand"
	if dups > 0 {
		text += "; " + plural(float64(dups), "submit was", "submits were") + " answered from the existing idempotency key, so no new job was created"
	}
	d.addFeed("action", "info", text+".", false)
	writeJSON(w, map[string]any{"ids": ids, "duplicates": dups})
}

func (d *Dash) handleLoad(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Running bool       `json:"running"`
		Config  LoadConfig `json:"config"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if !req.Running {
		if d.load.view().Running {
			d.load.stop()
			d.addFeed("action", "info", "Stopped the load generator.", true)
		}
		writeJSON(w, d.load.view())
		return
	}
	if err := req.Config.Validate(); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if d.jobClient() == nil {
		writeErr(w, http.StatusConflict, errNoCluster)
		return
	}
	d.load.start(d.ctx, req.Config, d.submit)
	d.addFeed("action", "info", "Load generator running: "+req.Config.String()+".", true)
	writeJSON(w, d.load.view())
}

func (d *Dash) handleLaunch(w http.ResponseWriter, r *http.Request) {
	var cfg ClusterConfig
	if !readJSON(w, r, &cfg) {
		return
	}
	if err := d.launch(cfg); err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func (d *Dash) handleStop(w http.ResponseWriter, r *http.Request) {
	var req struct{}
	if !readJSON(w, r, &req) {
		return
	}
	if err := d.stop(); err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func (d *Dash) handleScale(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Workers int `json:"workers"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if err := d.scale(req.Workers); err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func (d *Dash) handleFault(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Action string `json:"action"`
		Target string `json:"target"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	if _, err := d.fault(ctx, req.Action, req.Target); err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func (d *Dash) handleScenario(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
		Stop bool   `json:"stop"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	if req.Stop {
		d.stopScenario()
		writeJSON(w, map[string]bool{"ok": true})
		return
	}
	if err := d.startScenario(req.Name); err != nil {
		writeErr(w, statusFor(err), err)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}
