package main

import (
	"strings"
	"testing"
)

func TestParseSpecs(t *testing.T) {
	good := `[{"name":"storage-shard-0-manifest","groupId":"shard-0","shard":0,"ringVersion":1,
		"peers":[{"podName":"storage-shard-0-0","address":"storage-shard-0-0.storage-shard-0.reliefgrid.svc.cluster.local:7000"}]}]`
	ms, err := parseSpecs(good)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 || ms[0].Name != "storage-shard-0-manifest" || ms[0].Phase != phasePending || ms[0].RingVersion != 1 || len(ms[0].Peers) != 1 {
		t.Fatalf("%+v", ms)
	}
	for name, in := range map[string]string{
		"empty":     ``,
		"not json":  `storage-shard-0-manifest`,
		"no specs":  `[]`,
		"no name":   strings.Replace(good, `"name":"storage-shard-0-manifest",`, ``, 1),
		"no peers":  strings.Replace(good, `"peers"`, `"x"`, 1),
		"no group":  strings.Replace(good, `"groupId":"shard-0"`, `"groupId":""`, 1),
		"duplicate": `[` + good[1:len(good)-1] + `,` + good[1:len(good)-1] + `]`,
	} {
		if _, err := parseSpecs(in); err == nil {
			t.Errorf("%s: parsed", name)
		}
	}
}
