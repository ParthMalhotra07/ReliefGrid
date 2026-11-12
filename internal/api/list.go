package api

import (
	"context"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/ParthMalhotra07/ReliefGrid/internal/api/gen"
	"github.com/ParthMalhotra07/ReliefGrid/internal/geo"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

func requestStatusKnown(status string) bool {
	switch store.RequestStatus(status) {
	case store.RequestPending, store.RequestWaiting, store.RequestMatching, store.RequestMatched,
		store.RequestInProgress, store.RequestRetrying, store.RequestCancelling, store.RequestFulfilling,
		store.RequestCancelled, store.RequestFulfilled:
		return true
	}
	return false
}

type listQuery struct {
	scan   store.ScanQuery
	limit  int
	near   *store.Location
	radius float64
	after  string
}

// listConsistency is the read mode of every public list scan, named
// explicitly so the response reports the mode the scan was read in rather
// than a fixed label: linearizable, the read fence (S4.1). The store refuses
// an answer served weaker than asked, and strong also satisfies it.
const listConsistency = store.ConsistencyLinearizable

// Same accepted coordinate text as the frozen public near parameter.
var nearPattern = regexp.MustCompile(`^-?(90(\.0+)?|[0-8]?\d(\.\d+)?),-?(180(\.0+)?|1[0-7]\d(\.\d+)?|\d?\d(\.\d+)?)$`)

func parseList(r *http.Request, kind store.Kind) (listQuery, error) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return listQuery{}, invalid("query", "invalid query encoding")
	}
	for field, values := range query {
		allowed := field == "type" || field == "limit" || field == "cursor" ||
			(kind == store.KindRequest && field == "status") ||
			(kind == store.KindResource && (field == "near" || field == "radiusKm"))
		if !allowed || len(values) != 1 {
			return listQuery{}, invalid(field, "unknown or repeated query parameter")
		}
	}
	q := listQuery{scan: store.ScanQuery{Kind: kind, Consistency: listConsistency}, limit: 50, radius: 50}
	if query.Has("limit") {
		q.limit, err = strconv.Atoi(query.Get("limit"))
		if err != nil || q.limit < 1 || q.limit > 100 {
			return listQuery{}, invalid("limit", "must be an integer between 1 and 100")
		}
	}
	q.scan.Limit = q.limit
	if query.Has("type") {
		if !validType(query.Get("type")) {
			return listQuery{}, invalid("type", "unknown resource type")
		}
		q.scan.Type = store.ResourceType(query.Get("type"))
	}
	if query.Has("status") {
		if !requestStatusKnown(query.Get("status")) {
			return listQuery{}, invalid("status", "unknown request status")
		}
		q.scan.Statuses = []string{query.Get("status")}
	}
	if query.Has("radiusKm") {
		q.radius, err = strconv.ParseFloat(query.Get("radiusKm"), 64)
		if err != nil || math.IsNaN(q.radius) || math.IsInf(q.radius, 0) || q.radius <= 0 || q.radius > 50 || !query.Has("near") {
			return listQuery{}, invalid("radiusKm", "requires near and a radius greater than 0, at most 50")
		}
	}
	if query.Has("near") {
		if !nearPattern.MatchString(query.Get("near")) {
			return listQuery{}, invalid("near", "must be decimal latitude,longitude in range")
		}
		parts := strings.Split(query.Get("near"), ",")
		if len(parts) != 2 {
			return listQuery{}, invalid("near", "must be latitude,longitude")
		}
		lat, latErr := strconv.ParseFloat(parts[0], 64)
		lng, lngErr := strconv.ParseFloat(parts[1], 64)
		if latErr != nil || lngErr != nil {
			return listQuery{}, invalid("near", "must contain two numbers")
		}
		point, err := (&inputLocation{Lat: &lat, Lng: &lng}).point()
		if err != nil {
			return listQuery{}, invalid("near", "coordinates are out of range")
		}
		q.near = &point
		q.scan.GeohashPrefixes, err = geo.Cover(point, q.radius)
		if err != nil {
			return listQuery{}, invalid("near", "cannot cover this search")
		}
	}
	if query.Has("cursor") {
		if err := decodePublicCursor(&q, query.Get("cursor")); err != nil {
			return listQuery{}, err
		}
	}
	return q, nil
}

func (s *Server) list(ctx context.Context, q listQuery) ([]store.Record, []gen.ShardPosition, string, error) {
	rows := make([]store.Record, 0, q.limit)
	positions := make(map[int]store.ShardPosition)
	finish := func(next string) ([]store.Record, []gen.ShardPosition, string, error) {
		out := make([]gen.ShardPosition, 0, len(positions))
		for shard := 0; shard < s.cfg.Store.Shards(); shard++ {
			if pos, ok := positions[shard]; ok {
				more := pos.HasMore || next != ""
				out = append(out, gen.ShardPosition{Shard: shard, AppliedIndex: pos.AppliedIndex, HasMore: &more})
			}
		}
		return rows, out, next, nil
	}

	// A one-row probe obtains a ring-bearing storage cursor even when a full
	// first page would exhaust every shard. Bind the actual scan before reading
	// the public page; a ring change between probe and scan is rejected by Store.
	if q.scan.Cursor == "" {
		probe := q.scan
		probe.Limit = 1
		result, err := invoke(ctx, s.cfg.CallTimeout, func(ctx context.Context) (store.ScanResult, error) { return s.cfg.Store.Scan(ctx, probe) })
		if err != nil {
			return nil, nil, "", remoteError(err, false)
		}
		if result.Partial || len(result.FailedShards) > 0 || len(result.Positions) != s.cfg.Store.Shards() {
			return nil, nil, "", unavailable("STORAGE_UNAVAILABLE", false)
		}
		for _, pos := range result.Positions {
			if pos.Shard < 0 || pos.Shard >= s.cfg.Store.Shards() {
				return nil, nil, "", unavailable("STORAGE_UNAVAILABLE", false)
			}
			if _, exists := positions[pos.Shard]; exists {
				return nil, nil, "", unavailable("STORAGE_UNAVAILABLE", false)
			}
			positions[pos.Shard] = pos
		}
		if result.NextCursor == "" {
			if len(result.Records) != 0 {
				return nil, nil, "", unavailable("STORAGE_UNAVAILABLE", false)
			}
			return finish("")
		}
		token, err := store.DecodeScanCursor(result.NextCursor)
		if err != nil || token.Query != store.ScanQueryHash(probe) {
			return nil, nil, "", unavailable("STORAGE_UNAVAILABLE", false)
		}
		after := make(map[int]string)
		for shard := 0; shard < s.cfg.Store.Shards(); shard++ {
			after[shard] = ""
		}
		q.scan.Cursor = store.EncodeScanCursor(store.ScanCursor{RingVersion: token.RingVersion, Query: store.ScanQueryHash(q.scan), After: after})
	}
	for page := 0; page < 32; page++ {
		before := q.scan.Cursor
		result, err := invoke(ctx, s.cfg.CallTimeout, func(ctx context.Context) (store.ScanResult, error) { return s.cfg.Store.Scan(ctx, q.scan) })
		if err != nil {
			return nil, nil, "", remoteError(err, false)
		}
		if result.Partial || len(result.FailedShards) != 0 || len(result.Positions) == 0 {
			return nil, nil, "", unavailable("STORAGE_UNAVAILABLE", false)
		}
		current := make(map[int]bool)
		for _, pos := range result.Positions {
			if pos.Shard < 0 || pos.Shard >= s.cfg.Store.Shards() || current[pos.Shard] {
				return nil, nil, "", unavailable("STORAGE_UNAVAILABLE", false)
			}
			current[pos.Shard] = true
			positions[pos.Shard] = pos
		}
		expected, decodeErr := store.DecodeScanCursor(before)
		if decodeErr != nil || len(current) != len(expected.After) {
			return nil, nil, "", unavailable("STORAGE_UNAVAILABLE", false)
		}
		for shard := range current {
			if _, ok := expected.After[shard]; !ok {
				return nil, nil, "", unavailable("STORAGE_UNAVAILABLE", false)
			}
		}
		sort.Slice(result.Records, func(i, j int) bool { return result.Records[i].Key < result.Records[j].Key })
		for i, rec := range result.Records {
			if rec.Deleted || rec.Key <= q.after {
				continue
			}
			if q.near != nil && (rec.Resource == nil || geo.HaversineKM(*q.near, rec.Resource.Location) > q.radius) {
				continue
			}
			rows = append(rows, rec)
			if len(rows) == q.limit {
				next := ""
				if i+1 < len(result.Records) {
					next = encodePublicCursor(q, before, rec.Key)
				} else if result.NextCursor != "" {
					next = encodePublicCursor(q, result.NextCursor, "")
				}
				return finish(next)
			}
		}
		if result.NextCursor == "" {
			return finish("")
		}
		if result.NextCursor == before {
			return nil, nil, "", unavailable("STORAGE_UNAVAILABLE", false)
		}
		q.scan.Cursor, q.after = result.NextCursor, ""
	}
	return nil, nil, "", unavailable("STORAGE_UNAVAILABLE", false)
}

func (s *Server) listRequests(w http.ResponseWriter, r *http.Request) error {
	q, err := parseList(r, store.KindRequest)
	if err != nil {
		return err
	}
	rows, positions, next, err := s.list(r.Context(), q)
	if err != nil {
		return err
	}
	items := make([]requestResponse, 0, len(rows))
	for _, row := range rows {
		item, err := renderRequest(row)
		if err != nil {
			return err
		}
		items = append(items, item)
	}
	writeJSON(w, 200, struct {
		Items        []requestResponse   `json:"items"`
		Positions    []gen.ShardPosition `json:"positions"`
		Partial      bool                `json:"partial"`
		FailedShards []int               `json:"failedShards"`
		Consistency  string              `json:"consistency"`
		NextCursor   string              `json:"nextCursor,omitempty"`
	}{Items: items, Positions: positions, FailedShards: []int{}, Consistency: string(q.scan.Consistency), NextCursor: next})
	return nil
}

func (s *Server) listResources(w http.ResponseWriter, r *http.Request) error {
	q, err := parseList(r, store.KindResource)
	if err != nil {
		return err
	}
	rows, positions, next, err := s.list(r.Context(), q)
	if err != nil {
		return err
	}
	items := make([]resourceResponse, 0, len(rows))
	for _, row := range rows {
		item, err := renderResource(row)
		if err != nil {
			return err
		}
		items = append(items, item)
	}
	writeJSON(w, 200, struct {
		Items        []resourceResponse  `json:"items"`
		Positions    []gen.ShardPosition `json:"positions"`
		Partial      bool                `json:"partial"`
		FailedShards []int               `json:"failedShards"`
		Consistency  string              `json:"consistency"`
		NextCursor   string              `json:"nextCursor,omitempty"`
	}{Items: items, Positions: positions, FailedShards: []int{}, Consistency: string(q.scan.Consistency), NextCursor: next})
	return nil
}
