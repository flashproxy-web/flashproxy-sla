package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/flashproxy/flashproxy-status/internal/slo"
)

func TestPkgWhere(t *testing.T) {
	plain := &server{cfg: Config{ClickHouse: chConn{DB: "sla"}}}
	if got, want := plain.pkgWhere("datacenter", "10 MINUTE"), "package = 'datacenter'"; got != want {
		t.Errorf("no alts: got %q want %q", got, want)
	}
	s := &server{cfg: Config{ClickHouse: chConn{DB: "sla"}, Packages: pkgList("datacenter", "dc", "isp", "isp", "mobile", "")}}
	// Identity pair and unpaired package: same output as without a packages list.
	for _, pkg := range []string{"isp", "residential"} {
		if got, want := s.pkgWhere(pkg, "10 MINUTE"), "package = '"+pkg+"'"; got != want {
			t.Errorf("%s: got %q want %q", pkg, got, want)
		}
	}
	got := s.pkgWhere("datacenter", "10 MINUTE")
	for _, want := range []string{
		"(package = 'datacenter' OR (package = 'dc' AND (scenario, vantage, toStartOfMinute(ts)) NOT IN (",
		"FROM sla.probe_raw",
		"WHERE package = 'datacenter' AND ts >= toStartOfMinute(now() - INTERVAL 10 MINUTE))))",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("alt: %q does not contain %q", got, want)
		}
	}
}

// pkgList builds a packages list from short, alt pairs; alt "" = null (no alt).
func pkgList(kv ...string) []slo.Package {
	var out []slo.Package
	for i := 0; i+1 < len(kv); i += 2 {
		p := slo.Package{Short: kv[i]}
		if kv[i+1] != "" {
			alt := kv[i+1]
			p.Alt = &alt
		}
		out = append(out, p)
	}
	return out
}

func TestCheckPackageAlts(t *testing.T) {
	empty := ""
	ok := [][]slo.Package{
		nil,
		pkgList("datacenter", "dc", "isp", "", "mobile", "mobile", "isp_eu", "isp-eu"),
	}
	for _, pkgs := range ok {
		if err := checkPackageAlts(pkgs); err != nil {
			t.Errorf("%v: unexpected error %v", pkgs, err)
		}
	}
	bad := map[string][]slo.Package{
		"chain":           pkgList("datacenter", "dc", "dc", "old-dc"),
		"alt is a short":  pkgList("datacenter", "isp", "isp", "isp"),
		"shared alt":      pkgList("datacenter", "dc", "ipv6-datacenter", "dc"),
		"invalid alt":     pkgList("datacenter", "d'c"),
		"empty alt":       {{Short: "datacenter", Alt: &empty}},
		"invalid short":   pkgList("data'center", ""),
		"empty short":     {{}},
		"duplicate short": pkgList("datacenter", "dc", "datacenter", ""),
	}
	for name, pkgs := range bad {
		if err := checkPackageAlts(pkgs); err == nil {
			t.Errorf("%s: %v accepted, want an error", name, pkgs)
		}
	}
}

// The shipped example configs must parse and pass the packages check, and carry
// the names, alts and thresholds of the list.
func TestExampleConfigAlts(t *testing.T) {
	for _, f := range []string{"website"} {
		b, err := os.ReadFile("../../config/" + f + ".example.json")
		if err != nil {
			t.Fatal(err)
		}
		var c Config
		if err := json.Unmarshal(b, &c); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if len(c.Packages) != 9 {
			t.Fatalf("%s example config has %d packages, want 9", f, len(c.Packages))
		}
		if err := checkPackageAlts(c.Packages); err != nil {
			t.Errorf("%s example config: %v", f, err)
		}
		if got := c.DisplayName("isp_eu"); got != "Shared ISP · EU" {
			t.Errorf("%s: DisplayName(isp_eu) = %q", f, got)
		}
		if got := c.DisplayName("unlisted"); got != "unlisted" {
			t.Errorf("%s: DisplayName(unlisted) = %q, want the key", f, got)
		}
		if got := c.altKey("residential"); got != "resi" {
			t.Errorf("%s: altKey(residential) = %q, want resi", f, got)
		}
		for _, pkg := range []string{"isp", "mobile"} { // null alt, identity alt
			if got := c.altKey(pkg); got != "" {
				t.Errorf("%s: altKey(%s) = %q, want none", f, pkg, got)
			}
		}
		c.SLO.PackageDegradedMs = slo.PackageTargets(c.Packages)
		for pkg, want := range map[string]float64{"datacenter": 50, "residential-lite": 1000, "mobile_usa": 2000, "unlisted": 50} {
			if got := c.SLO.DegradedThresholdFor(pkg); got != want {
				t.Errorf("%s: DegradedThresholdFor(%s) = %v, want %v", f, pkg, got, want)
			}
		}
	}
	var p slo.Package
	if err := json.Unmarshal([]byte(`{"short":"isp","name":""}`), &p); err == nil {
		t.Error("empty name accepted, want an error")
	}
}

func TestAltKeyRedirect(t *testing.T) {
	s := &server{cfg: Config{Packages: pkgList("datacenter", "dc", "isp", "isp", "mobile", "")}}
	w := httptest.NewRecorder()
	s.handleRoot(w, httptest.NewRequest("GET", "/dc", nil))
	if w.Code != http.StatusFound || w.Header().Get("Location") != "/datacenter" {
		t.Errorf("/dc: got %d Location %q, want 302 /datacenter", w.Code, w.Header().Get("Location"))
	}
	if cc := w.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("/dc: Cache-Control %q, want no-cache", cc)
	}
	// Identity pairs and short keys are not redirected.
	for _, pkg := range []string{"isp", "datacenter", "mobile", "residential"} {
		if got := s.cfg.shortKey(pkg); got != pkg {
			t.Errorf("shortKey(%q) = %q, want no redirect", pkg, got)
		}
	}
}
