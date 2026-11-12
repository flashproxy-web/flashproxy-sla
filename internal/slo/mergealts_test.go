package slo

import (
	"reflect"
	"testing"
)

func TestMergeAlts(t *testing.T) {
	short := Minute{T: 60, ResponseMsAvg: 10, SuccessPct: 100, Samples: 4}
	altSame := Minute{T: 60, ResponseMsAvg: 99, SuccessPct: 0, Samples: 4}
	altOnly := Minute{T: 120, ResponseMsAvg: 20, SuccessPct: 100, Samples: 4}
	mk := func() map[string]map[string]map[int64]Minute {
		return map[string]map[string]map[int64]Minute{
			"datacenter": {"aws-a": {60: short}},
			"dc":         {"aws-a": {60: altSame, 120: altOnly}, "aws-b": {120: altOnly}},
			"isp":        {"aws-a": {60: short}},
		}
	}
	data := mk()
	got := MergeAlts(data, []Package{{Short: "datacenter", Alt: strp("dc")}, {Short: "isp", Alt: strp("isp")}, {Short: "residential"}})

	if _, ok := got["dc"]; ok {
		t.Error("alt key dc still present")
	}
	if m := got["datacenter"]["aws-a"][60]; m != short {
		t.Errorf("overlap minute: got %+v, want the short row %+v", m, short)
	}
	if m, ok := got["datacenter"]["aws-a"][120]; !ok || m != altOnly {
		t.Errorf("alt-only minute: got %+v ok=%v, want %+v under the short key", m, ok, altOnly)
	}
	if m, ok := got["datacenter"]["aws-b"][120]; !ok || m != altOnly {
		t.Errorf("alt-only vantage: got %+v ok=%v, want %+v under the short key", m, ok, altOnly)
	}
	// alt == short is a no-op.
	if !reflect.DeepEqual(got["isp"], mk()["isp"]) {
		t.Errorf("identity pair changed isp: %+v", got["isp"])
	}
	if !reflect.DeepEqual(data, mk()) {
		t.Errorf("input was modified: %+v", data)
	}
}

func TestMergeAltsNoAlts(t *testing.T) {
	data := map[string]map[string]map[int64]Minute{"dc": {"aws-a": {60: {T: 60}}}}
	for _, alts := range [][]Package{nil, {}, {{Short: "datacenter"}}} {
		got := MergeAlts(data, alts)
		if reflect.ValueOf(got).Pointer() != reflect.ValueOf(data).Pointer() {
			t.Errorf("alts %v: want the input map returned as is", alts)
		}
	}
}
