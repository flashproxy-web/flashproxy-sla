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
		{"no map", SLO{}, "residential", 50},
		{"per-package entry", SLO{PackageDegradedMs: map[string]float64{"residential": 1000}}, "residential", 1000},
		{"unlisted package", SLO{PackageDegradedMs: map[string]float64{"residential": 1000}}, "datacenter", 50},
		{"entry below 1 ignored", SLO{PackageDegradedMs: map[string]float64{"residential": 0.4}}, "residential", 50},
	}
	for _, c := range cases {
		if got := c.slo.DegradedThresholdFor(c.pkg); got != c.want {
			t.Errorf("%s: DegradedThresholdFor(%q) = %v, want %v", c.name, c.pkg, got, c.want)
		}
	}
}
