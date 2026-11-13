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
		{"per-package entry", SLO{PackageDegradedMs: map[string]float64{"residential": 1000}}, "residential", 1000},
		{"unlisted package", SLO{PackageDegradedMs: map[string]float64{"residential": 1000}}, "datacenter", 50},
		{"entry without target_ms", SLO{PackageDegradedMs: PackageTargets([]Package{{Short: "residential"}})}, "residential", 50},
		{"global fallback", SLO{DegradedAvgMs: 80, PackageDegradedMs: map[string]float64{"residential": 0}}, "residential", 80},
		{"not a whole number ignored", SLO{PackageDegradedMs: map[string]float64{"residential": 1000.5}}, "residential", 50},
		{"alt key is not a threshold key", SLO{PackageDegradedMs: PackageTargets([]Package{{Short: "residential", Alt: strp("resi"), TargetMs: 1000}})}, "resi", 50},
		{"old and new key each have their own entry", SLO{PackageDegradedMs: map[string]float64{"resi": 1000, "residential": 2000}}, "resi", 1000},
		{"entry below 1 ignored", SLO{PackageDegradedMs: map[string]float64{"residential": 0.4}}, "residential", 50},
	}
	for _, c := range cases {
		if got := c.slo.DegradedThresholdFor(c.pkg); got != c.want {
			t.Errorf("%s: DegradedThresholdFor(%q) = %v, want %v", c.name, c.pkg, got, c.want)
		}
	}
}

// TestCheckPackageDegradedMs: an empty map and ignorable values pass; a bad key or a
// value above the cap is a config error.
func TestCheckPackageDegradedMs(t *testing.T) {
	for _, m := range []map[string]float64{nil, {}, {"resi": 1000, "residential-lite": 0.4, "mobile_usa": MaxPackageDegradedMs}} {
		if err := CheckPackageDegradedMs(m); err != nil {
			t.Errorf("CheckPackageDegradedMs(%v) = %v, want nil", m, err)
		}
	}
	for _, m := range []map[string]float64{{"resi": MaxPackageDegradedMs + 1}, {"bad key": 50}, {"": 50}} {
		if err := CheckPackageDegradedMs(m); err == nil {
			t.Errorf("CheckPackageDegradedMs(%v) = nil, want an error", m)
		}
	}
}

func strp(s string) *string { return &s }
