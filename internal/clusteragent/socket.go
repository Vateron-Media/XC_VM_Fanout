package clusteragent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// The local socket (plan, section 7, "Local socket and data plane"): the
// node's PHP asks MAIN something that needs an answer before it can go on,
// through the agent, which holds the node's token:
//
//	POST /v1/main/{op}   body: the op's JSON payload; reply: MAIN's opened reply
//
// Only the ops in SocketOps pass. The socket is xc_vm's alone (0660, in the
// agent's state directory); everything that is only a report goes through
// the event spool instead (events.go).

// SocketOps are the MAIN ops the node's PHP may call through the socket.
var SocketOps = map[string]bool{"recording_complete": true}

// MaxSocketBody caps a request from PHP.
const MaxSocketBody = 1 << 20

// ServeSocket listens on path until ctx ends.
func (a *Agent) ServeSocket(ctx context.Context, path string) error {
	os.Remove(path) // a stale socket from a previous run
	l, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	if err := os.Chmod(path, 0o660); err != nil {
		l.Close()
		return err
	}
	srv := &http.Server{Handler: a.socketHandler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	err = srv.Serve(l)
	os.Remove(path)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (a *Agent) socketHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		op, ok := strings.CutPrefix(r.URL.Path, "/v1/main/")
		if r.Method != http.MethodPost || !ok || !SocketOps[op] {
			http.Error(w, "not allowed", http.StatusForbidden)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, MaxSocketBody+1))
		if err != nil || len(body) > MaxSocketBody || !json.Valid(body) {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		var out json.RawMessage
		if err := a.Client.Call(r.Context(), op, json.RawMessage(body), &out, false); err != nil {
			status := http.StatusBadGateway
			var d *Denial
			if errors.As(err, &d) {
				status = http.StatusConflict
			}
			a.logf("cluster: socket %s: %v", op, err)
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(out)
	})
}
