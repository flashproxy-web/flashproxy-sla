package slo

import "testing"

// TestDegradedThresholdFor: the displayed threshold is the package's own entry when
// one is configured (>= 1 ms), else the global one, else the 50 ms default.
func TestDegradedThresholdFor(t *testing.T) {
	cases := []struct {
		name string
		slo  SLO
		pkg  string
		want float64
	}{
		{"no list", SLO{}, "residential", 50},
		{"per-package entry", SLO{Packages: []Package{{Short: "residential", TargetMs: 1000}}}, "residential", 1000},
		{"unlisted package", SLO{Packages: []Package{{Short: "residential", TargetMs: 1000}}}, "datacenter", 50},
		{"entry without target_ms", SLO{Packages: []Package{{Short: "residential"}}}, "residential", 50},
		{"global fallback", SLO{DegradedAvgMs: 80, Packages: []Package{{Short: "residential"}}}, "residential", 80},
		{"not a whole number ignored", SLO{Packages: []Package{{Short: "residential", TargetMs: 1000.5}}}, "residential", 50},
		{"alt key is not a threshold key", SLO{Packages: []Package{{Short: "residential", Alt: strp("resi"), TargetMs: 1000}}}, "resi", 50},
		{"entry below 1 ignored", SLO{Packages: []Package{{Short: "residential", TargetMs: 0.4}}}, "residential", 50},
	}
	for _, c := range cases {
		if got := c.slo.DegradedThresholdFor(c.pkg); got != c.want {
			t.Errorf("%s: DegradedThresholdFor(%q) = %v, want %v", c.name, c.pkg, got, c.want)
		}
	}
}

func strp(s string) *string { return &s }
