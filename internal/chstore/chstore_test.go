package chstore

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// query runs QueryJSON against a server that answers with body (HTTP 200).
func query(body string) ([]map[string]any, error) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	defer srv.Close()
	return New(srv.URL, "sla", "u", "p").QueryJSON(context.Background(), "SELECT 1")
}

// row decodes one ClickHouse FORMAT JSON body through QueryJSON (the real decoder).
func row(t *testing.T, body string) map[string]any {
	t.Helper()
	rows, err := query(`{"meta":[],"data":[` + body + `],"rows":1}`)
	if err != nil || len(rows) != 1 {
		t.Fatalf("QueryJSON: %v (%d rows)", err, len(rows))
	}
	return rows[0]
}

func TestQueryJSONRejectsTrailingData(t *testing.T) {
	for _, tail := range []string{"\nCode: 241. DB::Exception: Memory limit exceeded", "}", "]", `{"data":[]}`} {
		if _, err := query(`{"data":[{"a":1}],"rows":1}` + tail); err == nil {
			t.Errorf("trailing %q: no error", tail)
		}
	}
	if rows, err := query(`{"data":[{"a":1}],"rows":1}` + "\n\t "); err != nil || len(rows) != 1 {
		t.Errorf("trailing whitespace: %v", err)
	}
}

func TestQueryJSONKeepsNumbersExact(t *testing.T) {
	m := row(t, `{"n":9007199254740993,"f":12.5}`)
	if _, ok := m["n"].(json.Number); !ok {
		t.Fatalf("number decoded as %T, want json.Number", m["n"])
	}
	var err error
	if got := Uint[uint64](m, "n", &err); got != 9007199254740993 || err != nil {
		t.Fatalf("2^53+1 as JSON number: got %d, %v", got, err)
	}
	if got := Num(m, "f"); got != 12.5 {
		t.Fatalf("Num float column: got %v", got)
	}
}

func TestIntegerAccessors(t *testing.T) {
	m := row(t, `{
		"u32n": 4294967295, "u32s": "4294967295", "u32over": 4294967296, "u32oversmax": "4294967296",
		"u64s": "18446744073709551615", "u64big": "9007199254740993", "u64over": "18446744073709551616",
		"i64n": -9223372036854775808, "i64s": "9223372036854775807", "i64over": "9223372036854775808",
		"u8": 255, "u8over": 256, "zero": 0,
		"neg": -1, "negs": "-1", "frac": 1.5, "fracz": 1.0, "exp": 1e3, "word": "abc", "empty": "",
		"null": null, "bool": true, "arr": [1]
	}`)

	ok := func(name string, got, want any, err error) {
		t.Helper()
		if err != nil || got != want {
			t.Errorf("%s: got %v (%T), err %v; want %v", name, got, got, err, want)
		}
	}
	var err error
	ok("UInt32 number max", Uint[uint32](m, "u32n", &err), uint32(4294967295), err)
	ok("UInt32 string max", Uint[uint32](m, "u32s", &err), uint32(4294967295), err)
	ok("UInt64 string max", Uint[uint64](m, "u64s", &err), uint64(18446744073709551615), err)
	ok("UInt64 > 2^53 exact", Uint[uint64](m, "u64big", &err), uint64(9007199254740993), err)
	ok("Int64 number min", Int[int64](m, "i64n", &err), int64(-9223372036854775808), err)
	ok("Int64 string max", Int[int64](m, "i64s", &err), int64(9223372036854775807), err)
	ok("Int64 negative string", Int[int64](m, "negs", &err), int64(-1), err)
	ok("UInt8 max", Uint[uint8](m, "u8", &err), uint8(255), err)
	ok("zero", Uint[uint32](m, "zero", &err), uint32(0), err)

	bad := []struct {
		name string
		f    func(*error) any
	}{
		{"UInt32 number over", func(e *error) any { return Uint[uint32](m, "u32over", e) }},
		{"UInt32 string over", func(e *error) any { return Uint[uint32](m, "u32oversmax", e) }},
		{"UInt64 over", func(e *error) any { return Uint[uint64](m, "u64over", e) }},
		{"Int64 over", func(e *error) any { return Int[int64](m, "i64over", e) }},
		{"Int32 from Int64 max", func(e *error) any { return Int[int32](m, "i64s", e) }},
		{"UInt8 over", func(e *error) any { return Uint[uint8](m, "u8over", e) }},
		{"negative unsigned (number)", func(e *error) any { return Uint[uint64](m, "neg", e) }},
		{"negative unsigned (string)", func(e *error) any { return Uint[uint32](m, "negs", e) }},
		{"fractional", func(e *error) any { return Uint[uint32](m, "frac", e) }},
		{"fractional signed", func(e *error) any { return Int[int64](m, "frac", e) }},
		{"1.0 is not an integer literal", func(e *error) any { return Int[int64](m, "fracz", e) }},
		{"exponent", func(e *error) any { return Uint[uint32](m, "exp", e) }},
		{"non-numeric string", func(e *error) any { return Uint[uint64](m, "word", e) }},
		{"empty string", func(e *error) any { return Int[int64](m, "empty", e) }},
		{"missing", func(e *error) any { return Uint[uint32](m, "nope", e) }},
		{"null", func(e *error) any { return Uint[uint32](m, "null", e) }},
		{"bool", func(e *error) any { return Int[int64](m, "bool", e) }},
		{"array", func(e *error) any { return Uint[uint64](m, "arr", e) }},
		{"float32 from word", func(e *error) any { return Float32(m, "word", e) }},
		{"float32 missing", func(e *error) any { return Float32(m, "nope", e) }},
	}
	for _, c := range bad {
		var err error
		got := c.f(&err)
		if err == nil {
			t.Errorf("%s: no error (got %v)", c.name, got)
		} else if got != reflectZero(got) {
			t.Errorf("%s: got %v with error, want 0", c.name, got)
		}
	}
}

func reflectZero(v any) any {
	switch v.(type) {
	case uint8:
		return uint8(0)
	case uint32:
		return uint32(0)
	case uint64:
		return uint64(0)
	case int32:
		return int32(0)
	case int64:
		return int64(0)
	case float32:
		return float32(0)
	}
	return nil
}

func TestFirstErrorSticks(t *testing.T) {
	m := row(t, `{"a":"x","b":"y","c":7}`)
	var err error
	Uint[uint32](m, "a", &err)
	first := err
	Uint[uint32](m, "b", &err)
	if got := Uint[uint32](m, "c", &err); got != 7 {
		t.Fatalf("valid column after error: got %d", got)
	}
	if err != first || err == nil {
		t.Fatalf("err = %v, want first error %v", err, first)
	}
}

func TestFloatColumns(t *testing.T) {
	m := row(t, `{"mbps":12.345,"mbps_s":"0.1","avg":71.3,"int_as_float":5,"pct":"bad"}`)
	var err error
	if got := Float32(m, "mbps", &err); got != float32(12.345) || err != nil {
		t.Errorf("Float32 number: %v, %v", got, err)
	}
	if got := Float32(m, "mbps_s", &err); got != float32(0.1) || err != nil {
		t.Errorf("Float32 string: %v, %v", got, err)
	}
	if got := Num(m, "avg"); got != 71.3 {
		t.Errorf("Num: %v", got)
	}
	if got := Num(m, "int_as_float"); got != 5 {
		t.Errorf("Num integer literal: %v", got)
	}
	if got := Num(m, "pct"); got != 0 { // Num stays lenient (display values only)
		t.Errorf("Num malformed: %v", got)
	}
}
