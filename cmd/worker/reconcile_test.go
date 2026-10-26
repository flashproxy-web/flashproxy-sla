package main

import (
	"fmt"
	"testing"

	"github.com/flashproxy/flashproxy-status/internal/alertgate"
)

func msg(prev, st string) string { return fmt.Sprintf(alertgate.MessageFormat, prev, st, 12.0, 99.0) }

func TestSeedConfirmed(t *testing.T) {
	cases := []struct {
		name, last, current, want string
	}{
		{"no events, healthy", "", "operational", "operational"},
		{"no events, no_data", "", "no_data", "no_data"},
		{"no events, degraded", "", "degraded", "operational"},
		{"no events, down", "", "down", "operational"},
		{"open degraded, still degraded", msg("operational", "degraded"), "degraded", "degraded"},
		{"open degraded, recovered while down", msg("operational", "degraded"), "operational", "degraded"},
		{"open down, now degraded", msg("operational", "down"), "degraded", "down"},
		{"legacy startup row, recovered", msg("startup", "degraded"), "operational", "degraded"},
		{"last no_data, now operational", msg("operational", "no_data"), "operational", "no_data"},
		{"closed episode, operational", msg("degraded", "operational"), "operational", "operational"},
		{"unparseable row, degraded", "garbage", "degraded", "operational"},
	}
	for _, c := range cases {
		if got := seedConfirmed(c.last, c.current); got != c.want {
			t.Errorf("%s: seedConfirmed(%q,%q)=%q want %q", c.name, c.last, c.current, got, c.want)
		}
	}
}
