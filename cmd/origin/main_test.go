package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestBytesHandler(t *testing.T) {
	const maxBytes = 100000 // > one 32 KiB chunk, so capped bodies span several writes
	h := bytesHandler(maxBytes)
	tests := []struct {
		name   string
		path   string
		status int
		length int    // expected body length / Content-Length (status 200 only)
		capped string // expected X-Bytes-Capped, "" = header absent
	}{
		{"zero", "/bytes/0", 200, 0, ""},
		{"under cap", "/bytes/1234", 200, 1234, ""},
		{"exactly cap", "/bytes/100000", 200, maxBytes, ""},
		{"over cap", "/bytes/100001", 200, maxBytes, "requested=100001; served=100000"},
		{"overflow", "/bytes/99999999999999999999", 200, maxBytes, "requested=99999999999999999999; served=100000"},
		{"non-numeric", "/bytes/abc", 400, 0, ""},
		{"empty", "/bytes/", 400, 0, ""},
		{"negative", "/bytes/-5", 400, 0, ""},
		{"negative overflow", "/bytes/-99999999999999999999", 400, 0, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
			if got := rec.Header().Get("X-Bytes-Capped"); got != tc.capped {
				t.Errorf("X-Bytes-Capped = %q, want %q", got, tc.capped)
			}
			if tc.status != 200 {
				return
			}
			if got := rec.Header().Get("Content-Length"); got != strconv.Itoa(tc.length) {
				t.Errorf("Content-Length = %q, want %d", got, tc.length)
			}
			if rec.Body.Len() != tc.length {
				t.Fatalf("body length = %d, want %d", rec.Body.Len(), tc.length)
			}
			// writeDeterministic restarts the pattern per 32 KiB chunk, so compare the first chunk.
			k := min(tc.length, 32*1024)
			if !bytes.Equal(rec.Body.Bytes()[:k], deterministic(k)) {
				t.Errorf("body prefix is not the deterministic payload")
			}
		})
	}
}
