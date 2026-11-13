package dataset_test

import (
	"bytes"
	"encoding/json"
	"math"
	"slices"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/google/uuid"

	"github.com/ParthMalhotra07/ReliefGrid/loadtest/dataset"
)

// The S5.2 dataset: one seeded sequence of public-API operations that the
// k6 scenarios replay. The same seed gives the same payloads in the same
// order, so a before/after comparison offers the same work; the run name
// only changes the Idempotency-Keys, so reruns on one cluster never replay.

func cfg(seed int64, run string) dataset.Config {
	return dataset.Config{Seed: seed, Run: run, Ops: 20_000}
}

func gen(t *testing.T, c dataset.Config) dataset.Dataset {
	t.Helper()
	d, err := dataset.Generate(c)
	if err != nil {
		t.Fatalf("Generate(%+v): %v", c, err)
	}
	return d
}

func encode(t *testing.T, d dataset.Dataset) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := d.Write(&b); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestDeterministic(t *testing.T) {
	a, b := gen(t, cfg(52, "r1")), gen(t, cfg(52, "r1"))
	if !bytes.Equal(encode(t, a), encode(t, b)) {
		t.Fatal("same seed and run gave different datasets")
	}
	if a.SHA256() != b.SHA256() || a.PayloadSHA256() != b.PayloadSHA256() {
		t.Fatal("hashes differ for identical datasets")
	}
	other := gen(t, cfg(53, "r1"))
	if other.PayloadSHA256() == a.PayloadSHA256() {
		t.Fatal("seeds 52 and 53 gave the same payloads")
	}
	rerun := gen(t, cfg(52, "r2"))
	if rerun.PayloadSHA256() != a.PayloadSHA256() {
		t.Fatal("a new run name changed the payloads; it must change only the keys")
	}
	i := slices.IndexFunc(a.Ops, func(op dataset.Op) bool { return op.Key != "" })
	if rerun.SHA256() == a.SHA256() || i < 0 || rerun.Ops[i].Key == a.Ops[i].Key {
		t.Fatal("a new run name kept the Idempotency-Keys; a rerun on the same cluster would replay")
	}
}

func TestMixAndCounts(t *testing.T) {
	d := gen(t, cfg(52, "r1"))
	if len(d.Ops) != 20_000 {
		t.Fatalf("%d ops, want 20000", len(d.Ops))
	}
	counts := map[dataset.OpKind]int{}
	for _, op := range d.Ops {
		counts[op.Op]++
	}
	want := dataset.DefaultMix()
	classes := map[string]float64{}
	for kind, share := range want {
		got := float64(counts[kind]) / float64(len(d.Ops))
		if math.Abs(got-share) > 0.015 {
			t.Errorf("%s: %.3f of ops, want %.3f +- 0.015", kind, got, share)
		}
		classes[dataset.ClassOf(kind)] += got
	}
	// The plan's mix: 15% creates, 10% registrations, 10% matches, 65% reads,
	// of which 55% by ID and 10% listings (their own class and threshold).
	for class, share := range map[string]float64{"create_request": 0.15, "register_resource": 0.10, "match": 0.10, "read": 0.55, "list": 0.10} {
		if math.Abs(classes[class]-share) > 0.02 {
			t.Errorf("class %s: %.3f, want %.2f", class, classes[class], share)
		}
	}
	if d.Counts.Requests != counts[dataset.CreateRequest] || d.Counts.Resources != counts[dataset.RegisterResource] {
		t.Errorf("Counts %+v disagree with the ops %v", d.Counts, counts)
	}
}

func TestOpsAreValid(t *testing.T) {
	spec, err := openapi3.NewLoader().LoadFromFile("../../contracts/public.yaml")
	if err != nil {
		t.Fatal(err)
	}
	schema := map[dataset.OpKind]*openapi3.Schema{
		dataset.CreateRequest:    spec.Components.Schemas["CreateRequestBody"].Value,
		dataset.RegisterResource: spec.Components.Schemas["RegisterResourceBody"].Value,
	}
	d := gen(t, cfg(52, "r1"))
	keys := map[string]bool{}
	creates := 0
	for i, op := range d.Ops {
		switch op.Op {
		case dataset.CreateRequest, dataset.RegisterResource:
			creates++
			if _, err := uuid.Parse(op.Key); err != nil || len(op.Key) != 36 || keys[op.Key] {
				t.Fatalf("op %d: Idempotency-Key %q is not a fresh 36-character UUID", i, op.Key)
			}
			keys[op.Key] = true
			var body any
			if err := json.Unmarshal(op.Body, &body); err != nil {
				t.Fatalf("op %d: %v", i, err)
			}
			if err := schema[op.Op].VisitJSON(body); err != nil {
				t.Fatalf("op %d %s: body fails the contract: %v\n%s", i, op.Op, err, op.Body)
			}
			var loc struct {
				Location struct{ Lat, Lng float64 }
			}
			_ = json.Unmarshal(op.Body, &loc)
			if km := distanceKm(loc.Location.Lat, loc.Location.Lng, dataset.CenterLat, dataset.CenterLng); km > dataset.RadiusKm+0.01 {
				t.Fatalf("op %d: %.2f km from the centre, past %v km", i, km, dataset.RadiusKm)
			}
		case dataset.Match, dataset.ReadRequest:
			if op.Ref < 1 || op.Key != "" || op.Body != nil {
				t.Fatalf("op %d %s: want only ref >= 1 (the n-th most recent request this VU created), got %+v", i, op.Op, op)
			}
		case dataset.ListRequests, dataset.ListResources:
			if op.Query == "" || op.Key != "" || op.Body != nil {
				t.Fatalf("op %d %s: want only a query, got %+v", i, op.Op, op)
			}
		default:
			t.Fatalf("op %d: unknown kind %q", i, op.Op)
		}
	}
	if creates == 0 {
		t.Fatal("no creates")
	}
}

func TestRejectsBadConfig(t *testing.T) {
	for name, c := range map[string]dataset.Config{
		"no ops":          {Seed: 1, Run: "r", Ops: 0},
		"no run":          {Seed: 1, Ops: 10},
		"mix not 1":       {Seed: 1, Run: "r", Ops: 10, Mix: dataset.Mix{dataset.CreateRequest: 0.5}},
		"negative share":  {Seed: 1, Run: "r", Ops: 10, Mix: dataset.Mix{dataset.CreateRequest: 1.5, dataset.ReadRequest: -0.5}},
		"unknown op kind": {Seed: 1, Run: "r", Ops: 10, Mix: dataset.Mix{"delete_everything": 1}},
	} {
		if _, err := dataset.Generate(c); err == nil {
			t.Errorf("%s: Generate accepted %+v", name, c)
		}
	}
}

func distanceKm(lat1, lng1, lat2, lng2 float64) float64 {
	const r = 6371.0
	rad := math.Pi / 180
	dlat, dlng := (lat2-lat1)*rad, (lng2-lng1)*rad
	a := math.Sin(dlat/2)*math.Sin(dlat/2) + math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dlng/2)*math.Sin(dlng/2)
	return 2 * r * math.Asin(math.Sqrt(a))
}
