package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/ParthMalhotra07/ReliefGrid/internal/geo"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

type inputLocation struct {
	Lat *float64 `json:"lat"`
	Lng *float64 `json:"lng"`
}

func (p *inputLocation) point() (store.Location, error) {
	if p == nil || p.Lat == nil || p.Lng == nil || math.IsNaN(*p.Lat) || math.IsNaN(*p.Lng) ||
		math.IsInf(*p.Lat, 0) || math.IsInf(*p.Lng, 0) || *p.Lat < -90 || *p.Lat > 90 || *p.Lng < -180 || *p.Lng > 180 {
		return store.Location{}, invalid("location", "lat and lng must be present and in range")
	}
	point := store.Location{Lat: *p.Lat, Lng: *p.Lng}
	if point.Lat == 0 {
		point.Lat = 0 // canonicalize negative zero
	}
	if point.Lng == 0 {
		point.Lng = 0
	}
	return point, nil
}

func validType(value string) bool {
	switch store.ResourceType(value) {
	case store.TypeWater, store.TypeFood, store.TypeMedicine, store.TypeTransportation, store.TypeShelter:
		return true
	}
	return false
}

func utcTime(value string) (int64, error) {
	at, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || (!strings.HasSuffix(value, "Z") && !strings.HasSuffix(value, "+00:00")) {
		return 0, invalid("window", "timestamps must be UTC ISO 8601")
	}
	return at.UnixMilli(), nil
}

// uniqueValue rejects duplicate JSON keys, including nested ones, before a
// typed decode could silently pick a different canonical payload.
func uniqueValue(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, container := token.(json.Delim)
	if !container {
		return nil
	}
	seen := make(map[string]bool)
	for d.More() {
		if delim == '{' {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return fmt.Errorf("duplicate or invalid object key")
			}
			seen[name] = true
		}
		if err := uniqueValue(d); err != nil {
			return err
		}
	}
	_, err = d.Token()
	return err
}

func decodeBody(w http.ResponseWriter, r *http.Request, out any, fields ...string) error {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return invalid("body", "Content-Type must be application/json")
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	body, err := io.ReadAll(r.Body)
	if err != nil || !utf8.Valid(body) {
		return invalid("body", "body is too large or invalid UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	if err := uniqueValue(d); err != nil {
		return invalid("body", "invalid JSON or duplicate field")
	}
	if _, err := d.Token(); err != io.EOF {
		return invalid("body", "exactly one JSON object is required")
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil || raw == nil {
		return invalid("body", "a JSON object is required")
	}
	allowed := make(map[string]bool, len(fields))
	for _, field := range fields {
		allowed[field] = true
	}
	for field, value := range raw {
		if !allowed[field] {
			return invalid(field, "field is not allowed")
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return invalid(field, "null is not allowed")
		}
	}
	if location, ok := raw["location"]; ok {
		var keys map[string]json.RawMessage
		if err := json.Unmarshal(location, &keys); err != nil {
			return invalid("location", "a coordinate object is required")
		}
		for field := range keys {
			if field != "lat" && field != "lng" {
				return invalid("location", "field is not allowed")
			}
		}
	}
	d = json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return invalid("body", "fields have invalid types")
	}
	return nil
}

func pathID(r *http.Request, name string) (string, error) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil {
		return "", invalid(name, "must be a UUID")
	}
	return id.String(), nil
}

type requestInput struct {
	Type     string         `json:"requestType"`
	Location *inputLocation `json:"location"`
	Details  *string        `json:"details"`
	People   *int32         `json:"peopleAffected"`
	Urgency  *string        `json:"urgency"`
}

func requestData(w http.ResponseWriter, r *http.Request) (store.Request, error) {
	var in requestInput
	if err := decodeBody(w, r, &in, "requestType", "location", "details", "peopleAffected", "urgency"); err != nil {
		return store.Request{}, err
	}
	point, err := in.Location.point()
	if err != nil {
		return store.Request{}, err
	}
	if !validType(in.Type) || in.Details == nil || utf8.RuneCountInString(*in.Details) > 4096 {
		return store.Request{}, invalid("requestType/details", "valid type and details of at most 4096 characters are required")
	}
	people := int32(1)
	if in.People != nil {
		people = *in.People
	}
	if people < 1 {
		return store.Request{}, invalid("peopleAffected", "must be between 1 and 2147483647")
	}
	urgency := 1
	if in.Urgency != nil {
		switch *in.Urgency {
		case "NORMAL":
		case "HIGH":
			urgency = 2
		case "CRITICAL":
			urgency = 3
		default:
			return store.Request{}, invalid("urgency", "must be NORMAL, HIGH or CRITICAL")
		}
	}
	hash, err := geo.Encode(point, 7)
	if err != nil {
		return store.Request{}, fmt.Errorf("request geohash: %w", err)
	}
	details, err := json.Marshal(*in.Details)
	if err != nil {
		return store.Request{}, fmt.Errorf("request details: %w", err)
	}
	return store.Request{Type: store.ResourceType(in.Type), Location: point, Geohash: hash,
		Details: details, PeopleAffected: int(people), Urgency: urgency, Status: store.RequestPending}, nil
}

type resourceInput struct {
	Type     string         `json:"resourceType"`
	Name     string         `json:"name"`
	Location *inputLocation `json:"location"`
	From     string         `json:"windowStart"`
	Until    string         `json:"windowEnd"`
	Reusable *bool          `json:"reusable"`
}

func resourceData(w http.ResponseWriter, r *http.Request) (store.Resource, error) {
	var in resourceInput
	if err := decodeBody(w, r, &in, "resourceType", "name", "location", "windowStart", "windowEnd", "reusable"); err != nil {
		return store.Resource{}, err
	}
	point, err := in.Location.point()
	if err != nil {
		return store.Resource{}, err
	}
	if !validType(in.Type) || in.Reusable == nil || utf8.RuneCountInString(in.Name) < 1 || utf8.RuneCountInString(in.Name) > 255 {
		return store.Resource{}, invalid("resourceType/name/reusable", "valid type, name of 1–255 characters and reusable are required")
	}
	from, err := utcTime(in.From)
	if err != nil {
		return store.Resource{}, err
	}
	until, err := utcTime(in.Until)
	if err != nil {
		return store.Resource{}, err
	}
	if from >= until {
		return store.Resource{}, invalid("window", "windowStart must precede windowEnd")
	}
	hash, err := geo.Encode(point, 7)
	if err != nil {
		return store.Resource{}, fmt.Errorf("resource geohash: %w", err)
	}
	return store.Resource{Type: store.ResourceType(in.Type), Name: in.Name, Location: point, Geohash: hash,
		AvailableFrom: from, AvailableUntil: until, Reusable: *in.Reusable, Status: store.ResourceAvailable}, nil
}
