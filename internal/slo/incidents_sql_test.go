package slo

import (
	"strings"
	"testing"
)

// TestIncidentsSQLShape checks the generated incident SQL without a database: alt
// folding, per-package thresholds, the global defaults and literal escaping.
func TestIncidentsSQLShape(t *testing.T) {
	alts := []Package{
		{Short: "isp", Alt: strp("isp-old")},
		{Short: "dc", Alt: strp("dc")}, // identity pair: no-op
		{Short: "lite"},                // no alt
		{Short: "o'k", Alt: strp(`b\a'd`)},
	}
	s := SLO{
		DegradedAvgMs:  250,
		DegradedForMin: 3,
		DownSuccessPct: 95.5,
		PackageDegradedMs: map[string]float64{
			"isp":  1000000, // must not render as 1e+06
			"lite": 0.5,     // unusable: ignored
			"o'k":  700,
		},
	}
	q := IncidentsSQL("sla", 43200, s, alts)

	for _, want := range []string{
		// alt folding, sorted, escaped; identity pair and no-alt entries absent
		`multiIf(package = 'b\\a\'d', 'o\'k', package = 'isp-old', 'isp', package)`,
		// per-package thresholds (valid ones only), global as the fallback
		`multiIf(pkg = 'isp', 1000000, pkg = 'o\'k', 700, 250)`,
		`best_ms > multiIf(`,
		`max(succ >= 95.5)`,
		`minIf(ms, succ >= 95.5)`,
		`o.p >= 3, 'degraded'`,
		`FROM sla.probe_raw`,
		`INTERVAL 43200 MINUTE`,
		`- 60 * 43200 + 60`,
		`AND t <= toUnixTimestamp(toStartOfMinute(now()))`,
		`argMin(ms, is_alt)`,
	} {
		if !strings.Contains(q, want) {
			t.Errorf("SQL lacks %q\n%s", want, q)
		}
	}
	for _, bad := range []string{`'dc', 'dc'`, `'lite', 0.5`, `e+06`} {
		if strings.Contains(q, bad) {
			t.Errorf("SQL contains %q\n%s", bad, q)
		}
	}
}

// TestIncidentsSQLDefaults: with no alts and no per-package targets the raw
// package name is used and the defaulted global thresholds are inlined.
func TestIncidentsSQLDefaults(t *testing.T) {
	q := IncidentsSQL("db", 60, SLO{}, nil)
	for _, want := range []string{
		`package AS raw, package AS pkg`,
		`best_ms > 50 AS is_over`,
		`max(succ >= 90)`,
		`o.p >= 5, 'degraded'`,
	} {
		if !strings.Contains(q, want) {
			t.Errorf("SQL lacks %q\n%s", want, q)
		}
	}
	if strings.Contains(q, "multiIf(package") || strings.Contains(q, "multiIf(pkg") {
		t.Errorf("unexpected multiIf without alts/targets\n%s", q)
	}
}
