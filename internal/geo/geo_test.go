package geo

import (
	"math"
	"strings"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func TestEncodeKnownCellsAndInvalidCoordinates(t *testing.T) {
	for _, tc := range []struct {
		point store.Location
		want  string
	}{
		{store.Location{Lat: 0, Lng: 0}, "s000000"},
		{store.Location{Lat: 0, Lng: -0.000001}, "ebpbpbp"},
		{store.Location{Lat: 0, Lng: 179.99}, "xbpbpb0"},
		{store.Location{Lat: 0, Lng: -179.99}, "800000p"},
	} {
		got, err := Encode(tc.point, len(tc.want))
		if err != nil || got != tc.want {
			t.Errorf("Encode(%+v) = %q, %v; want %q", tc.point, got, err, tc.want)
		}
	}
	for _, point := range []store.Location{
		{Lat: 90.01}, {Lat: -90.01}, {Lng: 180.01}, {Lng: -180.01},
		{Lat: math.NaN()}, {Lng: math.Inf(1)},
	} {
		if _, err := Encode(point, 7); err == nil {
			t.Errorf("Encode accepted invalid point %+v", point)
		}
	}
}

func TestHaversineKM(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b store.Location
		want float64
	}{
		{"identical", store.Location{}, store.Location{}, 0},
		{"equatorial degree", store.Location{}, store.Location{Lng: 1}, 111.195},
		{"dateline", store.Location{Lng: 179.99}, store.Location{Lng: -179.99}, 2.224},
		{"antipodes", store.Location{}, store.Location{Lng: 180}, 20015.114},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := HaversineKM(tc.a, tc.b)
			if math.IsNaN(got) || math.Abs(got-tc.want) > 0.02 {
				t.Fatalf("distance = %v km; want %.3f +/- .02 km", got, tc.want)
			}
			if reverse := HaversineKM(tc.b, tc.a); math.Abs(got-reverse) > 1e-9 {
				t.Errorf("asymmetric distance: %v vs %v", got, reverse)
			}
		})
	}
}

func TestCoverIncludesAdjacentCellsDatelineAndPoles(t *testing.T) {
	for _, tc := range []struct {
		name   string
		center store.Location
		nearby []store.Location
	}{
		{"equator boundary", store.Location{}, []store.Location{
			{Lat: .44}, {Lat: -.44}, {Lng: .44}, {Lng: -.44},
			{Lat: .31, Lng: .31}, {Lat: -.31, Lng: -.31},
			{Lng: -.000001}, {Lng: .000001},
		}},
		{"dateline", store.Location{Lng: 179.99}, []store.Location{{Lng: -179.99}, {Lng: 179.6}}},
		{"north pole", store.Location{Lat: 89.9, Lng: 90}, []store.Location{{Lat: 89.9, Lng: -90}, {Lat: 90}}},
		{"south pole", store.Location{Lat: -89.9, Lng: -90}, []store.Location{{Lat: -89.9, Lng: 90}, {Lat: -90}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefixes, err := Cover(tc.center, 50)
			if err != nil || len(prefixes) == 0 {
				t.Fatalf("Cover = %v, %v", prefixes, err)
			}
			for _, point := range append(tc.nearby, tc.center) {
				if distance := HaversineKM(tc.center, point); distance > 50 {
					t.Fatalf("bad fixture: %+v is %v km away", point, distance)
				}
				hash, err := Encode(point, 7)
				if err != nil {
					t.Fatal(err)
				}
				covered := false
				for _, prefix := range prefixes {
					if prefix == "" {
						t.Fatal("empty prefix scans the entire world")
					}
					covered = covered || strings.HasPrefix(hash, prefix)
				}
				if !covered {
					t.Errorf("50 km cover misses %+v (%s): %v", point, hash, prefixes)
				}
			}
		})
	}
}
