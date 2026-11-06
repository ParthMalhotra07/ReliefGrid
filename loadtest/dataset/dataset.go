// Package dataset generates the seeded S5.2 load dataset: a fixed sequence
// of public-API operations the k6 scenarios (loadtest/k6) replay in order.
//
// The seed fixes every payload and the order of operations, so a
// before/after comparison offers the same work. The run name feeds only the
// Idempotency-Keys, so a rerun on the same cluster creates new entities
// instead of replaying old ones; PayloadSHA256 is equal across runs of one
// seed, SHA256 is not.
//
// Reads and matches do not name an entity: Ref n means "the n-th most
// recent request this k6 VU created", because the IDs exist only once the
// API answers. A VU with fewer creates than Ref uses its oldest; one with
// none skips the op and counts it in skipped_ops (loadtest/k6/lib.js).
package dataset

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// OpKind is one public-API call.
type OpKind string

const (
	CreateRequest    OpKind = "create_request"    // POST /api/v1/requests
	RegisterResource OpKind = "register_resource" // POST /api/v1/resources
	Match            OpKind = "match"             // POST /api/v1/match/{requestId}
	ReadRequest      OpKind = "read_request"      // GET /api/v1/requests/{requestId}
	ListRequests     OpKind = "list_requests"     // GET /api/v1/requests?...
	ListResources    OpKind = "list_resources"    // GET /api/v1/resources?...
)

var kinds = []OpKind{CreateRequest, RegisterResource, Match, ReadRequest, ListRequests, ListResources}

// ClassOf is the mix class of an op: a read by ID is "read", a listing
// (a scan of every shard) is "list", every other kind is its own class.
func ClassOf(k OpKind) string {
	switch k {
	case ReadRequest:
		return "read"
	case ListRequests, ListResources:
		return "list"
	}
	return string(k)
}

// Mix is each kind's share of the ops; the shares sum to 1.
type Mix map[OpKind]float64

// DefaultMix is the S5.2 mix: 15% creates, 10% resource registrations, 10%
// matches and 65% reads (55% by ID, 5% request listings, 5% resource
// listings).
func DefaultMix() Mix {
	return Mix{CreateRequest: 0.15, RegisterResource: 0.10, Match: 0.10, ReadRequest: 0.55, ListRequests: 0.05, ListResources: 0.05}
}

// The dataset's area: a disc inside the matcher's 50 km radius, so any
// request has candidates of its type.
const (
	CenterLat = 28.6139
	CenterLng = 77.2090
	RadiusKm  = 20.0
)

// Config selects a dataset.
type Config struct {
	Seed int64
	Run  string // run name: feeds the Idempotency-Keys only
	Ops  int
	Mix  Mix // nil: DefaultMix
	// Resource availability window; zero: 2026-11-01 to 2027-01-01 UTC.
	WindowStart, WindowEnd time.Time
}

// Op is one operation. Creates carry Key (the Idempotency-Key) and Body;
// Match and ReadRequest carry Ref; listings carry Query.
type Op struct {
	Op    OpKind          `json:"op"`
	Key   string          `json:"key,omitempty"`
	Body  json.RawMessage `json:"body,omitempty"`
	Ref   int             `json:"ref,omitempty"`
	Query string          `json:"query,omitempty"`
}

// Counts summarises a dataset.
type Counts struct {
	Ops       int `json:"ops"`
	Requests  int `json:"requests"`
	Resources int `json:"resources"`
}

// Dataset is the file the k6 scenarios load.
type Dataset struct {
	Schema int    `json:"schema"`
	Seed   int64  `json:"seed"`
	Run    string `json:"run"`
	Mix    Mix    `json:"mix"`
	Counts Counts `json:"counts"`
	Ops    []Op   `json:"ops"`
}

var keyNamespace = uuid.NewSHA1(uuid.NameSpaceURL, []byte("reliefgrid/loadtest/S5.2"))

var (
	types     = []string{"WATER", "FOOD", "MEDICINE", "TRANSPORTATION", "SHELTER"}
	statuses  = []string{"WAITING", "MATCHED", "PENDING"}
	urgencies = []struct {
		name  string
		share float64
	}{{"NORMAL", 0.7}, {"HIGH", 0.2}, {"CRITICAL", 0.1}}
)

type location struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type requestBody struct {
	RequestType    string   `json:"requestType"`
	Location       location `json:"location"`
	Details        string   `json:"details"`
	PeopleAffected int      `json:"peopleAffected"`
	Urgency        string   `json:"urgency"`
}

type resourceBody struct {
	ResourceType string   `json:"resourceType"`
	Name         string   `json:"name"`
	Location     location `json:"location"`
	Reusable     bool     `json:"reusable"`
	WindowStart  string   `json:"windowStart"`
	WindowEnd    string   `json:"windowEnd"`
}

// Generate builds the dataset for c.
func Generate(c Config) (Dataset, error) {
	if c.Ops <= 0 {
		return Dataset{}, errors.New("dataset: Ops must be positive")
	}
	if c.Run == "" {
		return Dataset{}, errors.New("dataset: Run is required (it makes the Idempotency-Keys unique per run)")
	}
	mix := c.Mix
	if mix == nil {
		mix = DefaultMix()
	}
	sum := 0.0
	for k, v := range mix {
		if !slices.Contains(kinds, k) {
			return Dataset{}, fmt.Errorf("dataset: unknown op kind %q", k)
		}
		if v < 0 || math.IsNaN(v) {
			return Dataset{}, fmt.Errorf("dataset: share of %s is %v", k, v)
		}
		sum += v
	}
	if math.Abs(sum-1) > 1e-9 {
		return Dataset{}, fmt.Errorf("dataset: mix sums to %v, want 1", sum)
	}
	start, end := c.WindowStart, c.WindowEnd
	if start.IsZero() {
		start = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	}
	if end.IsZero() {
		end = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	if !end.After(start) {
		return Dataset{}, errors.New("dataset: WindowEnd must be after WindowStart")
	}

	rng := rand.New(rand.NewPCG(uint64(c.Seed), uint64(c.Seed)^0x5352_4c4f_4144)) //nolint:gosec // a seeded workload, not a secret
	d := Dataset{Schema: 1, Seed: c.Seed, Run: c.Run, Mix: mix, Ops: make([]Op, 0, c.Ops)}
	for i := range c.Ops {
		kind := pick(rng, mix)
		op := Op{Op: kind}
		var body any
		switch kind {
		case CreateRequest:
			d.Counts.Requests++
			body = requestBody{RequestType: types[rng.IntN(len(types))], Location: randomLocation(rng),
				Details: "S5.2 load request " + strconv.Itoa(i), PeopleAffected: 1 + rng.IntN(20), Urgency: pickUrgency(rng)}
		case RegisterResource:
			d.Counts.Resources++
			body = resourceBody{ResourceType: types[rng.IntN(len(types))], Name: "S5.2 load resource " + strconv.Itoa(i),
				Location: randomLocation(rng), Reusable: rng.IntN(2) == 0,
				WindowStart: start.UTC().Format(time.RFC3339), WindowEnd: end.UTC().Format(time.RFC3339)}
		case Match, ReadRequest:
			op.Ref = 1 + rng.IntN(8)
		case ListRequests:
			op.Query = "status=" + statuses[rng.IntN(len(statuses))] + "&limit=20"
		case ListResources:
			l := randomLocation(rng)
			op.Query = fmt.Sprintf("type=%s&near=%.5f,%.5f&radiusKm=10&limit=20", types[rng.IntN(len(types))], l.Lat, l.Lng)
		}
		if body != nil {
			b, err := json.Marshal(body)
			if err != nil {
				return Dataset{}, err
			}
			op.Body = b
			op.Key = uuid.NewSHA1(keyNamespace, []byte(c.Run+"/"+strconv.Itoa(i))).String()
		}
		d.Ops = append(d.Ops, op)
	}
	d.Counts.Ops = len(d.Ops)
	return d, nil
}

func pick(rng *rand.Rand, mix Mix) OpKind {
	x := rng.Float64()
	var last OpKind
	for _, k := range kinds { // fixed order: map iteration would break determinism
		share, ok := mix[k]
		if !ok || share == 0 {
			continue
		}
		last = k
		if x < share {
			return k
		}
		x -= share
	}
	return last
}

func pickUrgency(rng *rand.Rand) string {
	x := rng.Float64()
	for _, u := range urgencies {
		if x < u.share {
			return u.name
		}
		x -= u.share
	}
	return urgencies[len(urgencies)-1].name
}

// randomLocation is uniform over the disc of RadiusKm around the centre,
// rounded to 5 decimals (about 1 m).
func randomLocation(rng *rand.Rand) location {
	r := RadiusKm * math.Sqrt(rng.Float64()) * 0.999 // headroom for rounding
	theta := 2 * math.Pi * rng.Float64()
	lat := CenterLat + r*math.Cos(theta)/111.32
	lng := CenterLng + r*math.Sin(theta)/(111.32*math.Cos(CenterLat*math.Pi/180))
	round := func(v float64) float64 { return math.Round(v*1e5) / 1e5 }
	return location{Lat: round(lat), Lng: round(lng)}
}

// Write encodes d as the k6 scenarios read it.
func (d Dataset) Write(w io.Writer) error {
	b, err := json.Marshal(d)
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\n'))
	return err
}

// SHA256 is the hash of d as written.
func (d Dataset) SHA256() string {
	var b bytes.Buffer
	_ = d.Write(&b)
	return fmt.Sprintf("%x", sha256.Sum256(b.Bytes()))
}

// PayloadSHA256 is the hash of d without its run name and keys: equal for
// every run of one seed, so it says two runs offered the same work.
func (d Dataset) PayloadSHA256() string {
	p := d
	p.Run = ""
	p.Ops = make([]Op, len(d.Ops))
	for i, op := range d.Ops {
		op.Key = ""
		p.Ops[i] = op
	}
	return p.SHA256()
}
