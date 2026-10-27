// Command origin is the deterministic, self-hosted upstream the probers target.
// It serves reproducible payloads so connect-ms / throughput are pure SLA signal
// with no third-party variance. Bind it dual-stack (":8080" listens on v4 and v6)
// so the ipv6 / ipv6-datacenter packages actually exercise v6 egress.
package main

import (
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address (use :PORT for dual-stack v4+v6)")
	maxBytes := flag.Int("max-bytes", 5<<20, "hard cap on /bytes/{n} response size; larger requests are served this many bytes")
	flag.Parse()
	if *maxBytes < 1 {
		log.Fatalf("origin: -max-bytes must be >= 1, got %d", *maxBytes)
	}

	mux := http.NewServeMux()

	// /connect — instant 200, the connect-ms probe target.
	mux.HandleFunc("/connect", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Origin", "flashproxy-status")
		io.WriteString(w, "ok")
	})

	// /small — a few KB, mimics the hi-freq small-payload archetype.
	mux.HandleFunc("/small", func(w http.ResponseWriter, r *http.Request) {
		w.Write(deterministic(4096))
	})

	// /bytes/{n} — n deterministic bytes (capped at -max-bytes), for streaming/large-object.
	mux.HandleFunc("/bytes/", bytesHandler(*maxBytes))

	// /hold?ms=N — trickles bytes for N ms, for the long-session archetype.
	mux.HandleFunc("/hold", func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		if ms <= 0 || ms > 300000 {
			ms = 60000
		}
		fl, _ := w.(http.Flusher)
		deadline := time.Now().Add(time.Duration(ms) * time.Millisecond)
		for time.Now().Before(deadline) {
			io.WriteString(w, "x")
			if fl != nil {
				fl.Flush()
			}
			time.Sleep(500 * time.Millisecond)
		}
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })

	log.Printf("origin listening on %s (max-bytes %d)", *addr, *maxBytes)
	srv := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

// bytesHandler serves /bytes/{n}: exactly n deterministic bytes. A malformed or negative
// n is a 400. An n above maxBytes (including one too large to parse) is not an error: the
// response is exactly maxBytes bytes (Content-Length = maxBytes) and carries the header
//
//	X-Bytes-Capped: requested=<n as sent>; served=<maxBytes>
//
// which is absent on uncapped responses.
func bytesHandler(maxBytes int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw := strings.TrimPrefix(r.URL.Path, "/bytes/")
		n, err := strconv.ParseInt(raw, 10, 64) // out of range -> ±MaxInt64 with ErrRange
		if (err != nil && !errors.Is(err, strconv.ErrRange)) || n < 0 {
			http.Error(w, "bad size", http.StatusBadRequest)
			return
		}
		if n > int64(maxBytes) {
			w.Header().Set("X-Bytes-Capped", "requested="+raw+"; served="+strconv.Itoa(maxBytes))
			n = int64(maxBytes)
		}
		w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
		writeDeterministic(w, int(n))
	}
}

func deterministic(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('A' + i%26)
	}
	return b
}

func writeDeterministic(w io.Writer, n int) {
	chunk := deterministic(32 * 1024)
	for n > 0 {
		k := n
		if k > len(chunk) {
			k = len(chunk)
		}
		if _, err := w.Write(chunk[:k]); err != nil {
			return
		}
		n -= k
	}
}
