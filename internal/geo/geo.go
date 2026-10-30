// Package geo supplies candidate-index covers and spherical distances. These
// floating-point calculations run in the application, never in the storage FSM.
package geo

import (
	"fmt"
	"math"
	"slices"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const (
	earthRadiusKM = 6371.0088
	alphabet      = "0123456789bcdefghjkmnpqrstuvwxyz"
)

func valid(point store.Location) bool {
	return !math.IsNaN(point.Lat) && !math.IsNaN(point.Lng) &&
		point.Lat >= -90 && point.Lat <= 90 && point.Lng >= -180 && point.Lng <= 180
}

// Encode returns a standard geohash with 1..12 characters. Coordinates at either
// representation of the dateline are accepted; covers include both edge cells.
func Encode(point store.Location, precision int) (string, error) {
	if !valid(point) || precision < 1 || precision > 12 {
		return "", fmt.Errorf("geo: invalid coordinate or precision %d", precision)
	}
	lo := [2]float64{-180, -90}
	hi := [2]float64{180, 90}
	coordinates := [2]float64{point.Lng, point.Lat}
	result := make([]byte, precision)
	value := 0
	for bit := 0; bit < precision*5; bit++ {
		axis := bit % 2
		middle := (lo[axis] + hi[axis]) / 2
		value <<= 1
		if coordinates[axis] >= middle {
			value |= 1
			lo[axis] = middle
		} else {
			hi[axis] = middle
		}
		if bit%5 == 4 {
			result[bit/5] = alphabet[value]
			value = 0
		}
	}
	return string(result), nil
}

// HaversineKM uses the mean Earth radius. Clamp the haversine before taking its
// square root so rounding at antipodes cannot produce NaN.
func HaversineKM(a, b store.Location) float64 {
	const radians = math.Pi / 180
	dlat := (b.Lat - a.Lat) * radians
	dlng := (b.Lng - a.Lng) * radians
	h := math.Pow(math.Sin(dlat/2), 2) + math.Cos(a.Lat*radians)*math.Cos(b.Lat*radians)*math.Pow(math.Sin(dlng/2), 2)
	h = math.Max(0, math.Min(1, h))
	return 2 * earthRadiusKM * math.Asin(math.Sqrt(h))
}

// Cover returns sorted, unique index prefixes covering the circle's bounding
// box. A 50 km search uses precision 3; Haversine is the final radius predicate.
// Longitude wraps at the dateline and covers every longitude at a pole.
func Cover(center store.Location, radiusKM float64) ([]string, error) {
	if !valid(center) || math.IsNaN(radiusKM) || math.IsInf(radiusKM, 0) || radiusKM < 0 {
		return nil, fmt.Errorf("geo: invalid center or radius %v", radiusKM)
	}
	precision := 3
	if radiusKM > 500 {
		precision = 2
	}
	if radiusKM > 2500 {
		precision = 1
	}
	angle := math.Min(math.Pi, radiusKM/earthRadiusKM)
	dlat := angle * 180 / math.Pi
	latMin, latMax := math.Max(-90, center.Lat-dlat), math.Min(90, center.Lat+dlat)
	if latMin == -90 || latMax == 90 {
		// Polar caps touch every longitude. Coarsen their index cover to avoid
		// hundreds of SQL OR predicates; the exact distance check is unchanged.
		precision = min(precision, 2)
	}
	intervals := [][2]float64{{-180, 180}}
	if latMin > -90 && latMax < 90 && angle < math.Pi/2 {
		dlng := math.Asin(math.Min(1, math.Sin(angle)/math.Cos(center.Lat*math.Pi/180))) * 180 / math.Pi
		lo, hi := center.Lng-dlng, center.Lng+dlng
		switch {
		case lo <= -180:
			intervals = [][2]float64{{-180, hi}, {lo + 360, 180}}
		case hi >= 180:
			intervals = [][2]float64{{lo, 180}, {-180, hi - 360}}
		default:
			intervals = [][2]float64{{lo, hi}}
		}
	}
	latCells := 1 << (precision * 5 / 2)
	lngCells := 1 << ((precision*5 + 1) / 2)
	latStep, lngStep := 180/float64(latCells), 360/float64(lngCells)
	index := func(value, minimum, step float64, count int) int {
		return max(0, min(count-1, int(math.Floor((value-minimum)/step))))
	}
	prefixes := make(map[string]bool)
	for lat := index(latMin, -90, latStep, latCells); lat <= index(latMax, -90, latStep, latCells); lat++ {
		for _, interval := range intervals {
			for lng := index(interval[0], -180, lngStep, lngCells); lng <= index(interval[1], -180, lngStep, lngCells); lng++ {
				prefix, err := Encode(store.Location{
					Lat: -90 + (float64(lat)+.5)*latStep,
					Lng: -180 + (float64(lng)+.5)*lngStep,
				}, precision)
				if err != nil {
					return nil, fmt.Errorf("geo cover cell: %w", err)
				}
				prefixes[prefix] = true
			}
		}
	}
	result := make([]string, 0, len(prefixes))
	for prefix := range prefixes {
		result = append(result, prefix)
	}
	slices.Sort(result)
	return result, nil
}
