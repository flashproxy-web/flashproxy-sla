package slo

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/flashproxy/flashproxy-status/internal/chstore"
)

// TestIncidentsSQLMatchesGo compares the SQL incident rollup with the Go rollup on
// live data. It runs only when FP_LIVE_CONFIG (a website.json path) is set; the
// ClickHouse password is read from SLA_WEBSITE_PASSWORD, never from the config.
func TestIncidentsSQLMatchesGo(t *testing.T) {
	cfgPath := os.Getenv("FP_LIVE_CONFIG")
	if cfgPath == "" {
		t.Skip("FP_LIVE_CONFIG not set")
	}
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		Packages   []Package `json:"packages"`
		SLO        SLO       `json:"slo"`
		ClickHouse struct {
			URL, DB, User string
		} `json:"clickhouse"`
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatal(err)
	}
	s := c.SLO
	s.PackageDegradedMs = PackageTargets(c.Packages)
	ch := chstore.New(c.ClickHouse.URL, c.ClickHouse.DB, c.ClickHouse.User, os.Getenv("SLA_WEBSITE_PASSWORD"))
	ctx := context.Background()
	const window = 30 * 24 * 60

	goInc, err := RollupIncidentsAlts(ctx, ch, c.ClickHouse.DB, window, s, c.Packages)
	if err != nil {
		t.Fatal("go rollup:", err)
	}
	sqlInc, err := FetchIncidentsAlts(ctx, ch, c.ClickHouse.DB, window, s, c.Packages)
	if err != nil {
		t.Fatal("sql rollup:", err)
	}
	key := func(in []Incident) []Incident {
		sort.Slice(in, func(i, j int) bool {
			if in[i].Package != in[j].Package {
				return in[i].Package < in[j].Package
			}
			return in[i].Start < in[j].Start
		})
		return in
	}
	goInc, sqlInc = key(goInc), key(sqlInc)
	t.Logf("go incidents=%d sql incidents=%d", len(goInc), len(sqlInc))
	if !reflect.DeepEqual(goInc, sqlInc) {
		for i := 0; i < len(goInc) || i < len(sqlInc); i++ {
			var g, q any = "-", "-"
			if i < len(goInc) {
				g = goInc[i]
			}
			if i < len(sqlInc) {
				q = sqlInc[i]
			}
			if !reflect.DeepEqual(g, q) {
				t.Errorf("diff at %d:\n go: %+v\n sql: %+v", i, g, q)
			}
		}
	}
}
