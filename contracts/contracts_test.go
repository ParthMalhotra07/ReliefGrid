// Package contracts checks contracts/internal.yaml against the plan (v3.1 §40,
// M5), internal/store and ring.yaml, and the shared outcome table in README.md.
package contracts

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"go.yaml.in/yaml/v3"

	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

const (
	specFile   = "internal.yaml"
	readmeFile = "README.md"
	ringFile   = "ring.yaml"
	storeFile  = "../internal/store/store.go"
	jsonMedia  = "application/json"
)

// Operation extensions that tie the spec to internal/store:
//
//	x-store-method: the Store method(s) an operation serves (string or list)
//	x-store-field:  on a parameter, the command field it carries
//	x-store-fixed:  command fields implied by the operation, e.g. {delete: true}
//	x-store-result: the 200-body property holding the method's return value
//	                (absent: the whole body)
const (
	extMethod = "x-store-method"
	extField  = "x-store-field"
	extFixed  = "x-store-fixed"
	extResult = "x-store-result"
	// x-store-encoding: etag on a header parameter: the field travels as a
	// strong entity tag (internalapi.FormatETag), not as a JSON integer.
	extEncoding = "x-store-encoding"
)

// envelopeFields may appear in a command body without being a store field.
var envelopeFields = []string{"ringVersion"}

// readMethods take an explicit consistency mode (M5: strong unless asked).
var readMethods = []string{"ReadRecord", "ReadClosure", "Scan", "PendingOutbox", "Operation"}

var readModes = []string{"linearizable", "stale", "strong"}

// section40 lists every route in v3.1 §40.
var section40 = []string{
	"POST /internal/nodes/register",
	"POST /internal/nodes/heartbeat",
	"PUT /internal/data/{key}",
	"GET /internal/data/{key}",
	"DELETE /internal/data/{key}",
	"GET /internal/scan",
	"POST /internal/requests/{id}/transition",
	"POST /internal/resources/{id}/claim",
	"POST /internal/resources/{id}/close-attempt",
	"GET /internal/outbox",
	"POST /internal/outbox/published",
	"GET /internal/operations/{operationId}",
	"GET /admin/ring",
	"GET /admin/nodes",
	"GET /admin/locate/{key}",
	"GET /admin/distribution",
}

type route struct {
	name   string // "GET /internal/scan"
	op     *openapi3.Operation
	params openapi3.Parameters // path-level, then operation-level
}

func loadSpec(t *testing.T) *openapi3.T {
	t.Helper()
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile(specFile)
	if err != nil {
		t.Fatalf("load %s: %v", specFile, err)
	}
	return doc
}

func routes(doc *openapi3.T) []route {
	var out []route
	for path, item := range doc.Paths.Map() {
		for method, op := range item.Operations() {
			params := append(slices.Clone(item.Parameters), op.Parameters...)
			out = append(out, route{name: method + " " + path, op: op, params: params})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

func storeMethods(op *openapi3.Operation) []string {
	switch v := op.Extensions[extMethod].(type) {
	case string:
		return []string{v}
	case []any:
		var out []string
		for _, m := range v {
			s, _ := m.(string)
			out = append(out, s)
		}
		return out
	}
	return nil
}

var storeType = reflect.TypeFor[store.Store]()

// commandType is the command struct a Store mutation takes, or nil for a
// read. Mutations are the methods that return a committed store.Result from
// a single command (Scan takes a ScanQuery but is a read).
func commandType(method string) reflect.Type {
	m, ok := storeType.MethodByName(method)
	if !ok || m.Type.NumIn() != 2 || m.Type.Out(0) != reflect.TypeFor[store.Result]() {
		return nil
	}
	if in := m.Type.In(1); in.Kind() == reflect.Struct {
		return in
	}
	return nil
}

func TestInternalSpecIsValid(t *testing.T) {
	doc := loadSpec(t)
	if doc.OpenAPI != "3.0.3" {
		t.Errorf("openapi %q; want 3.0.3 like public.yaml (oapi-codegen v2.8.0)", doc.OpenAPI)
	}
	// Validate checks every example against its schema unless disabled.
	if err := doc.Validate(context.Background(), openapi3.EnableExamplesValidation()); err != nil {
		t.Fatalf("%s is invalid: %v", specFile, err)
	}
}

func TestEveryBodyHasAValidExample(t *testing.T) {
	doc := loadSpec(t)
	for _, r := range routes(doc) {
		if rb := r.op.RequestBody; rb != nil {
			checkExamples(t, r.name+" request", rb.Value.Content.Get(jsonMedia))
		}
		for status, resp := range r.op.Responses.Map() {
			if mt := resp.Value.Content.Get(jsonMedia); mt != nil {
				checkExamples(t, r.name+" "+status, mt)
			}
		}
	}
}

func checkExamples(t *testing.T, where string, mt *openapi3.MediaType) {
	t.Helper()
	if mt == nil || mt.Schema == nil {
		t.Errorf("%s: no application/json schema", where)
		return
	}
	values := map[string]any{}
	if mt.Example != nil {
		values["example"] = mt.Example
	}
	for name, ex := range mt.Examples {
		values[name] = ex.Value.Value
	}
	if len(values) == 0 {
		t.Errorf("%s: no example", where)
	}
	for name, v := range values {
		if err := mt.Schema.Value.VisitJSON(v); err != nil {
			t.Errorf("%s: example %q does not match its schema: %v", where, name, err)
		}
	}
}

func TestSection40RoutesExist(t *testing.T) {
	doc := loadSpec(t)
	have := map[string]bool{}
	for _, r := range routes(doc) {
		have[r.name] = true
	}
	for _, want := range section40 {
		if !have[want] {
			t.Errorf("missing §40 route %s", want)
		}
	}
}

func TestEveryStoreMethodHasARoute(t *testing.T) {
	doc := loadSpec(t)
	served := map[string][]string{}
	for _, r := range routes(doc) {
		for _, m := range storeMethods(r.op) {
			if _, ok := storeType.MethodByName(m); !ok {
				t.Errorf("%s: %s %q is not a store.Store method", r.name, extMethod, m)
			}
			served[m] = append(served[m], r.name)
		}
	}
	for i := range storeType.NumMethod() {
		if m := storeType.Method(i).Name; len(served[m]) == 0 {
			t.Errorf("store.Store.%s has no route (%s)", m, extMethod)
		}
	}
}

// TestCommandsMatchStore checks that each command route carries exactly the
// store command's fields (body, parameter or fixed), with the same types and
// enums, and that every mutation carries ringVersion.
func TestCommandsMatchStore(t *testing.T) {
	doc := loadSpec(t)
	enums := storeEnums(t)
	for _, r := range routes(doc) {
		methods := storeMethods(r.op)
		var cmds []reflect.Type
		for _, m := range methods {
			if c := commandType(m); c != nil {
				cmds = append(cmds, c)
			}
		}
		if len(cmds) == 0 {
			continue
		}
		t.Run(r.name, func(t *testing.T) {
			if len(cmds) != len(methods) {
				t.Fatalf("mixes commands and reads: %v", methods)
			}
			body := requestSchema(r.op)
			carried := carriedFields(r, body)
			if !carried["ringVersion"].required {
				t.Errorf("mutation does not require ringVersion")
			}
			known := map[string]bool{}
			for _, f := range envelopeFields {
				known[f] = true
			}
			for _, cmd := range cmds {
				for _, f := range jsonFields(cmd) {
					known[f.name] = true
					c, ok := carried[f.name]
					if !ok {
						t.Errorf("%s.%s is not carried", cmd.Name(), f.name)
						continue
					}
					// One route serving two commands (PUT: create or CAS)
					// cannot require fields only one of them has.
					if len(cmds) == 1 && f.required && !c.required {
						t.Errorf("%s.%s must be required", cmd.Name(), f.name)
					}
					if c.schema != nil {
						compareSchema(t, cmd.Name()+"."+f.name, f.typ, c.schema, enums)
					}
				}
			}
			if body != nil {
				for name := range body.Properties {
					if !known[name] {
						t.Errorf("body property %q is not a field of %v", name, methods)
					}
				}
			}
		})
	}
}

type carried struct {
	required bool
	schema   *openapi3.Schema
}

func requestSchema(op *openapi3.Operation) *openapi3.Schema {
	if op.RequestBody == nil {
		return nil
	}
	if mt := op.RequestBody.Value.Content.Get(jsonMedia); mt != nil && mt.Schema != nil {
		return mt.Schema.Value
	}
	return nil
}

func carriedFields(r route, body *openapi3.Schema) map[string]carried {
	out := map[string]carried{}
	if body != nil {
		for name, p := range body.Properties {
			out[name] = carried{required: slices.Contains(body.Required, name), schema: p.Value}
		}
	}
	for _, p := range r.params {
		name := p.Value.Name
		if f, ok := p.Value.Extensions[extField].(string); ok {
			name = f
		}
		c := carried{required: p.Value.Required, schema: p.Value.Schema.Value}
		if p.Value.Extensions[extEncoding] == "etag" {
			c.schema = nil // checked by TestEntityTagsUseOneEncoding
		}
		out[name] = c
	}
	if fixed, ok := r.op.Extensions[extFixed].(map[string]any); ok {
		for name := range fixed {
			out[name] = carried{required: true}
		}
	}
	return out
}

// TestResultsMatchStore checks each store route's 200 body against the
// method's return type, that mutations echo ringVersion, and that committed
// rejections (409, 412) carry the store Result.
func TestResultsMatchStore(t *testing.T) {
	doc := loadSpec(t)
	enums := storeEnums(t)
	resultType := reflect.TypeFor[store.Result]()
	for _, r := range routes(doc) {
		for _, m := range storeMethods(r.op) {
			method, ok := storeType.MethodByName(m)
			if !ok {
				continue
			}
			t.Run(r.name+"/"+m, func(t *testing.T) {
				ok := responseSchema(r.op, 200)
				if ok == nil {
					t.Fatal("no 200 application/json response")
				}
				target := ok
				if prop, set := r.op.Extensions[extResult].(string); set {
					p, found := ok.Properties[prop]
					if !found {
						t.Fatalf("%s %q is not a 200 body property", extResult, prop)
					}
					target = p.Value
				}
				compareSchema(t, m+" result", method.Type.Out(0), target, enums)
				if commandType(m) == nil {
					return
				}
				if _, found := ok.Properties["ringVersion"]; !found {
					t.Error("mutation 200 body does not include ringVersion (§40)")
				}
				for _, status := range []int{409, 412} {
					if s := responseSchema(r.op, status); s != nil {
						p, found := s.Properties["result"]
						if !found {
							t.Errorf("%d body has no committed result", status)
							continue
						}
						compareSchema(t, m+" "+strconv.Itoa(status)+" result", resultType, p.Value, enums)
					}
				}
			})
		}
	}
}

func responseSchema(op *openapi3.Operation, status int) *openapi3.Schema {
	resp := op.Responses.Status(status)
	if resp == nil || resp.Value == nil {
		return nil
	}
	if mt := resp.Value.Content.Get(jsonMedia); mt != nil && mt.Schema != nil {
		return mt.Schema.Value
	}
	return nil
}

// TestReadsDefaultToStrong: M5 ships strong reads as the default;
// linearizable and stale are explicit opt-ins.
func TestReadsDefaultToStrong(t *testing.T) {
	doc := loadSpec(t)
	for _, r := range routes(doc) {
		for _, m := range storeMethods(r.op) {
			if !slices.Contains(readMethods, m) {
				continue
			}
			p := r.params.GetByInAndName("query", "consistency")
			if p == nil {
				t.Errorf("%s (%s): no consistency query parameter", r.name, m)
				continue
			}
			s := p.Schema.Value
			var modes []string
			for _, v := range s.Enum {
				modes = append(modes, v.(string))
			}
			slices.Sort(modes)
			if !slices.Equal(modes, readModes) {
				t.Errorf("%s: consistency enum %v; want %v", r.name, modes, readModes)
			}
			if p.Required || s.Default != "strong" {
				t.Errorf("%s: consistency must be optional with default strong (M5); required=%v default=%v", r.name, p.Required, s.Default)
			}
		}
	}
}

// TestUnavailableCarriesLeaderHint: every store route can answer 503, and the
// error body distinguishes unknown outcome from no leader and gives a hint.
func TestUnavailableCarriesLeaderHint(t *testing.T) {
	doc := loadSpec(t)
	ref, ok := doc.Components.Schemas["Error"]
	if !ok {
		t.Fatal("components.schemas.Error is missing")
	}
	errSchema := ref.Value
	if !slices.Contains(errSchema.Required, "outcomeUnknown") {
		t.Error("Error.outcomeUnknown must be required")
	}
	for _, p := range []string{"code", "message", "outcomeUnknown", "retryable", "leaderHint"} {
		if _, ok := errSchema.Properties[p]; !ok {
			t.Errorf("Error has no %s", p)
		}
	}
	if hint, ok := errSchema.Properties["leaderHint"]; ok {
		for _, p := range []string{"nodeId", "address"} {
			if _, ok := hint.Value.Properties[p]; !ok {
				t.Errorf("Error.leaderHint has no %s", p)
			}
		}
	}
	for _, r := range routes(doc) {
		if len(storeMethods(r.op)) == 0 {
			continue
		}
		resp := r.op.Responses.Status(503)
		if resp == nil {
			t.Errorf("%s: no 503 response", r.name)
			continue
		}
		if mt := resp.Value.Content.Get(jsonMedia); mt == nil || mt.Schema == nil || mt.Schema.Ref != "#/components/schemas/Error" {
			t.Errorf("%s: 503 body is not the Error schema", r.name)
		}
		if resp.Value.Headers["Retry-After"] == nil {
			t.Errorf("%s: 503 has no Retry-After header", r.name)
		}
	}
}

// TestEntityTagsUseOneEncoding: every If-Match parameter and ETag response
// header uses the EntityTag schema, and its pattern accepts exactly what
// internalapi.ParseETag accepts.
func TestEntityTagsUseOneEncoding(t *testing.T) {
	doc := loadSpec(t)
	const ref = "#/components/schemas/EntityTag"
	tagRef, ok := doc.Components.Schemas["EntityTag"]
	if !ok {
		t.Fatal("components.schemas.EntityTag is missing")
	}
	pattern := regexp.MustCompile(tagRef.Value.Pattern)
	for _, tag := range []string{`"0"`, `"7"`, `"18446744073709551615"`, `7`, `""`, `"07"`, `W/"7"`, `*`, `"7", "8"`, `"-1"`} {
		_, err := internalapi.ParseETag(tag)
		if pattern.MatchString(tag) != (err == nil) {
			t.Errorf("EntityTag pattern and ParseETag disagree on %s (pattern %v, ParseETag %v)", tag, pattern.MatchString(tag), err)
		}
	}
	if tag := internalapi.FormatETag(7); !pattern.MatchString(tag) {
		t.Errorf("FormatETag(7) = %s does not match the EntityTag pattern", tag)
	}
	etags := 0
	for _, r := range routes(doc) {
		for _, p := range r.params {
			if !strings.EqualFold(p.Value.Name, "If-Match") {
				continue
			}
			if p.Value.Schema.Ref != ref || p.Value.Extensions[extEncoding] != "etag" {
				t.Errorf("%s: If-Match must use %s with %s: etag", r.name, ref, extEncoding)
			}
		}
		for status, resp := range r.op.Responses.Map() {
			if h, ok := resp.Value.Headers["ETag"]; ok {
				etags++
				if h.Value.Schema.Ref != ref {
					t.Errorf("%s %s: ETag header must use %s", r.name, status, ref)
				}
			}
		}
	}
	if etags == 0 {
		t.Error("no response returns an ETag header")
	}
}

// TestServerFilledFieldsAreReadOnly: a store.go field commented
// "server-filled" is readOnly in the schema of the same name.
func TestServerFilledFieldsAreReadOnly(t *testing.T) {
	doc := loadSpec(t)
	fields := serverFilled(t)
	if len(fields) == 0 {
		t.Fatal(`store.go has no "server-filled" fields`)
	}
	for typeName, names := range fields {
		ref, ok := doc.Components.Schemas[typeName]
		if !ok {
			t.Errorf("components.schemas.%s is missing", typeName)
			continue
		}
		for _, name := range names {
			p, ok := ref.Value.Properties[name]
			switch {
			case !ok:
				t.Errorf("%s has no %s", typeName, name)
			case !p.Value.ReadOnly:
				t.Errorf("%s.%s is server-filled and must be readOnly", typeName, name)
			case slices.Contains(ref.Value.Required, name):
				t.Errorf("%s.%s is server-filled and cannot be required", typeName, name)
			}
		}
	}
}

func TestScanCarriesPartialAndPositions(t *testing.T) {
	doc := loadSpec(t)
	for _, r := range routes(doc) {
		if !slices.Contains(storeMethods(r.op), "Scan") {
			continue
		}
		s := responseSchema(r.op, 200)
		if s == nil {
			t.Fatalf("%s: no 200 body", r.name)
		}
		for _, p := range []string{"partial", "failedShards", "positions"} {
			if _, ok := s.Properties[p]; !ok {
				t.Errorf("%s: scan result has no %s", r.name, p)
			}
		}
		for _, p := range []string{"partial", "positions"} {
			if !slices.Contains(s.Required, p) {
				t.Errorf("%s: %s must be required", r.name, p)
			}
		}
		return
	}
	t.Fatal("no route serves Scan")
}

// TestRingSchemaMatchesRingYAML: the Ring schema has every ring.yaml field and
// its example is ring.yaml itself.
func TestRingSchemaMatchesRingYAML(t *testing.T) {
	doc := loadSpec(t)
	raw, err := os.ReadFile(ringFile)
	if err != nil {
		t.Fatal(err)
	}
	var ring map[string]any
	if err := yaml.Unmarshal(raw, &ring); err != nil {
		t.Fatal(err)
	}
	ref, ok := doc.Components.Schemas["Ring"]
	if !ok {
		t.Fatal("components.schemas.Ring is missing")
	}
	example, _ := ref.Value.Example.(map[string]any)
	for k, want := range ring {
		if _, ok := ref.Value.Properties[k]; !ok {
			t.Errorf("Ring has no %s", k)
		}
		if !slices.Contains(ref.Value.Required, k) {
			t.Errorf("Ring.%s must be required", k)
		}
		if got := example[k]; !sameJSON(got, want) {
			t.Errorf("Ring example %s = %v; ring.yaml has %v", k, got, want)
		}
	}
}

func sameJSON(a, b any) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return string(ja) == string(jb)
}

// TestOutcomeTable checks the shared store-outcome → HTTP table in README.md:
// one row per store outcome, error and transport condition, the statuses
// agreed for A0.1/S0.4, and that internal.yaml's Error.code enum is exactly
// the table's internal error codes.
func TestOutcomeTable(t *testing.T) {
	rows := outcomeTable(t)

	want := slices.Clone(storeEnums(t)["Outcome"])
	want = append(want, storeErrors(t)...)
	want = append(want, "no leader", "not leader", "local storage failure", "idempotency mismatch")
	for _, k := range want {
		if _, ok := rows[strings.ToLower(k)]; !ok {
			t.Errorf("table has no row for %s", k)
		}
	}

	status := func(key, column, want string) {
		t.Helper()
		row, ok := rows[strings.ToLower(key)]
		if !ok {
			return
		}
		if got := firstStatus(row[column]); got != want {
			t.Errorf("%s %s status %q; want %s (cell %q)", key, column, got, want, row[column])
		}
	}
	contains := func(key, column, want string) {
		t.Helper()
		if row, ok := rows[strings.ToLower(key)]; ok && !strings.Contains(strings.ToLower(row[column]), strings.ToLower(want)) {
			t.Errorf("%s %s cell %q does not mention %q", key, column, row[column], want)
		}
	}
	for _, c := range []string{"public", "internal"} {
		status("INVALID", c, "400")
		status("NOT_FOUND", c, "404")
		status("ErrNotFound", c, "404")
		status("DOMAIN_CONFLICT", c, "409")
		status("OPERATION_ID_REUSED", c, "409")
		status("ErrUnknownOutcome", c, "503")
		status("no leader", c, "503")
		status("local storage failure", c, "503")
	}
	// Internal follows §40 and the store.go comments.
	status("VERSION_CONFLICT", "internal", "412")
	status("EXISTS", "internal", "412")
	status("not leader", "internal", "503")
	for _, k := range []string{"OK", "SUCCESS", "CONFLICT_CLOSED", "REJECTED_CLOSED", "CLOSED"} {
		status(k, "internal", "200")
	}
	// Public is what the API returns until If-Match/412 lands (week three).
	status("VERSION_CONFLICT", "public", "409")
	status("idempotency mismatch", "public", "409")
	contains("EXISTS", "public", "replay")
	contains("EXISTS", "public", "201")
	contains("EXISTS", "public", "409")
	if row, ok := rows["idempotency mismatch"]; ok && strings.Trim(row["code"], "` ") != "CONFLICT_IDEMPOTENCY_KEY" {
		t.Errorf("idempotency mismatch code %q; want CONFLICT_IDEMPOTENCY_KEY (A0.1)", row["code"])
	}
	contains("ErrUnknownOutcome", "notes", "outcomeUnknown=true")
	contains("ErrUnknownOutcome", "notes", "Retry-After")
	contains("no leader", "notes", "outcomeUnknown=false")
	contains("not leader", "notes", "leaderHint")
	contains("not leader", "notes", "retryable=true")
	contains("not leader", "notes", "outcomeUnknown=false")
	if row, ok := rows["local storage failure"]; ok && strings.Trim(row["code"], "` ") != "STORAGE_UNAVAILABLE" {
		t.Errorf("local storage failure code %q; want STORAGE_UNAVAILABLE", row["code"])
	}
	contains("local storage failure", "notes", "retryable=true")
	contains("local storage failure", "notes", "outcomeUnknown=false")

	// internal.yaml's error codes are exactly the table's internal errors.
	var tableCodes []string
	for _, row := range rows {
		code := strings.Trim(row["code"], "` ")
		if s := firstStatus(row["internal"]); code != "" && code != "—" && s != "" && s[0] != '2' {
			tableCodes = append(tableCodes, code)
		}
	}
	slices.Sort(tableCodes)
	tableCodes = slices.Compact(tableCodes)
	doc := loadSpec(t)
	ref, ok := doc.Components.Schemas["Error"]
	if !ok {
		t.Fatal("components.schemas.Error is missing")
	}
	var specCodes []string
	if code, ok := ref.Value.Properties["code"]; ok {
		for _, v := range code.Value.Enum {
			specCodes = append(specCodes, v.(string))
		}
	}
	slices.Sort(specCodes)
	if !slices.Equal(specCodes, tableCodes) {
		t.Errorf("Error.code enum %v; README internal error codes %v", specCodes, tableCodes)
	}
}

var statusRE = regexp.MustCompile(`\b[1-5]\d\d\b`)

func firstStatus(cell string) string { return statusRE.FindString(cell) }

// outcomeTable parses the first table under the "Store outcome" heading into
// rows keyed by the lower-cased first cell, each a map of lower-cased header
// (first word) to cell.
func outcomeTable(t *testing.T) map[string]map[string]string {
	t.Helper()
	raw, err := os.ReadFile(readmeFile)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(raw), "\n")
	start := slices.IndexFunc(lines, func(l string) bool {
		return strings.HasPrefix(l, "#") && strings.Contains(l, "Store outcome")
	})
	if start < 0 {
		t.Fatal(`README.md has no "Store outcome" heading`)
	}
	var header []string
	rows := map[string]map[string]string{}
	for _, l := range lines[start+1:] {
		l = strings.TrimSpace(l)
		if !strings.HasPrefix(l, "|") {
			if header != nil {
				break
			}
			continue
		}
		cells := strings.Split(strings.Trim(l, "|"), "|")
		for i := range cells {
			cells[i] = strings.TrimSpace(cells[i])
		}
		switch {
		case header == nil:
			for _, c := range cells {
				header = append(header, strings.ToLower(strings.Fields(c)[0]))
			}
		case strings.HasPrefix(cells[0], "-"):
		default:
			row := map[string]string{}
			for i, c := range cells {
				if i < len(header) {
					row[header[i]] = c
				}
			}
			rows[strings.ToLower(strings.Trim(cells[0], "` "))] = row
		}
	}
	for _, col := range []string{"code", "public", "internal", "notes"} {
		if !slices.Contains(header, col) {
			t.Fatalf("outcome table header %v has no %q column", header, col)
		}
	}
	return rows
}

// storeEnums returns the string constants of each named type in store.go.
func storeEnums(t *testing.T) map[string][]string {
	t.Helper()
	file := parseStore(t)
	out := map[string][]string{}
	for _, decl := range file.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, spec := range gd.Specs {
			vs := spec.(*ast.ValueSpec)
			ident, ok := vs.Type.(*ast.Ident)
			if !ok {
				continue
			}
			for _, v := range vs.Values {
				if lit, ok := v.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					s, _ := strconv.Unquote(lit.Value)
					out[ident.Name] = append(out[ident.Name], s)
				}
			}
		}
	}
	return out
}

// serverFilled returns, per store struct, the json names of fields whose
// line comment says "server-filled".
func serverFilled(t *testing.T) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	ast.Inspect(parseStore(t), func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok {
			return true
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok {
			return false
		}
		for _, f := range st.Fields.List {
			if f.Comment == nil || !strings.Contains(f.Comment.Text(), "server-filled") || f.Tag == nil {
				continue
			}
			tag, _ := strconv.Unquote(f.Tag.Value)
			name, _, _ := strings.Cut(reflect.StructTag(tag).Get("json"), ",")
			out[ts.Name.Name] = append(out[ts.Name.Name], name)
		}
		return false
	})
	return out
}

// storeErrors returns the exported Err* sentinel names in store.go.
func storeErrors(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, decl := range parseStore(t).Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, spec := range gd.Specs {
			for _, n := range spec.(*ast.ValueSpec).Names {
				if strings.HasPrefix(n.Name, "Err") {
					out = append(out, n.Name)
				}
			}
		}
	}
	return out
}

func parseStore(t *testing.T) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), storeFile, nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

type field struct {
	name     string
	typ      reflect.Type
	required bool
}

// jsonFields flattens embedded structs and reads json tags; a field without
// omitempty is required.
func jsonFields(typ reflect.Type) []field {
	var out []field
	for i := range typ.NumField() {
		f := typ.Field(i)
		if f.Anonymous {
			out = append(out, jsonFields(f.Type)...)
			continue
		}
		name, opts, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "" || name == "-" {
			continue
		}
		out = append(out, field{name: name, typ: f.Type, required: !strings.Contains(opts, "omitempty")})
	}
	return out
}

var rawMessage = reflect.TypeFor[json.RawMessage]()

// compareSchema checks that schema has the shape of typ: objects carry every
// json field (required unless omitempty), store enum types list exactly the
// store constants, and scalar types agree.
func compareSchema(t *testing.T, where string, typ reflect.Type, schema *openapi3.Schema, enums map[string][]string) {
	t.Helper()
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ == rawMessage {
		return
	}
	want := map[reflect.Kind]string{
		reflect.String: "string", reflect.Bool: "boolean",
		reflect.Int: "integer", reflect.Int64: "integer", reflect.Uint64: "integer",
		reflect.Float64: "number", reflect.Slice: "array", reflect.Struct: "object",
	}[typ.Kind()]
	if want == "" {
		t.Errorf("%s: unhandled Go kind %s", where, typ.Kind())
		return
	}
	if schema.Type == nil || !schema.Type.Is(want) {
		t.Errorf("%s: schema type %v; Go %s wants %s", where, schema.Type, typ, want)
		return
	}
	switch typ.Kind() {
	case reflect.Slice:
		if schema.Items == nil {
			t.Errorf("%s: array without items", where)
			return
		}
		compareSchema(t, where+"[]", typ.Elem(), schema.Items.Value, enums)
	case reflect.Struct:
		for _, f := range jsonFields(typ) {
			p, ok := schema.Properties[f.name]
			if !ok {
				t.Errorf("%s: no property %s (%s.%s)", where, f.name, typ.Name(), f.name)
				continue
			}
			if f.required && !slices.Contains(schema.Required, f.name) {
				t.Errorf("%s.%s must be required", where, f.name)
			}
			compareSchema(t, where+"."+f.name, f.typ, p.Value, enums)
		}
	case reflect.String:
		values, isEnum := enums[typ.Name()]
		if typ.PkgPath() != reflect.TypeFor[store.Kind]().PkgPath() || !isEnum {
			return
		}
		var got []string
		for _, v := range schema.Enum {
			s, _ := v.(string)
			got = append(got, s)
		}
		// A command may narrow an enum (TransitionResource.status is
		// AVAILABLE or OFFLINE); an outcome list must be complete.
		if len(got) == 0 {
			t.Errorf("%s: no enum; store.%s has %v", where, typ.Name(), values)
		}
		for _, v := range got {
			if !slices.Contains(values, v) {
				t.Errorf("%s: enum value %q is not a store.%s", where, v, typ.Name())
			}
		}
		if typ == reflect.TypeFor[store.Outcome]() && len(got) != len(values) {
			t.Errorf("%s: enum %v; store.Outcome has %v", where, got, values)
		}
	}
}

// incarnationRE is a disk-incarnation UUID; serverIDRE is the Raft ServerID
// podName/incarnation (v3.1 §10).
var (
	incarnationRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	serverIDRE    = regexp.MustCompile(`^[a-z0-9-]+/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

// TestNodeIdentity: registration and heartbeats carry the full identity
// (groupId, shardId, incarnation; §10, M7). nodeId is the pod name and
// leaderId is the Raft ServerID, in the schemas and in every example.
func TestNodeIdentity(t *testing.T) {
	doc := loadSpec(t)
	for _, name := range []string{"NodeRegistration", "Heartbeat", "NodeStatus"} {
		ref, ok := doc.Components.Schemas[name]
		if !ok {
			t.Errorf("components.schemas.%s is missing", name)
			continue
		}
		s := ref.Value
		for _, p := range []string{"nodeId", "groupId", "shardId", "incarnation"} {
			if _, ok := s.Properties[p]; !ok {
				t.Errorf("%s has no %s", name, p)
			}
			if !slices.Contains(s.Required, p) {
				t.Errorf("%s.%s must be required", name, p)
			}
		}
		if p, ok := s.Properties["nodeId"]; ok && !strings.Contains(p.Value.Description, "pod name") {
			t.Errorf("%s.nodeId description %q must say it is the pod name", name, p.Value.Description)
		}
	}
	for _, name := range []string{"Heartbeat", "NodeStatus", "ShardLeader", "Placement"} {
		ref, ok := doc.Components.Schemas[name]
		if !ok {
			continue
		}
		if p, ok := ref.Value.Properties["leaderId"]; !ok || !strings.Contains(p.Value.Description, "podName/incarnation") {
			t.Errorf("%s.leaderId must be described as the Raft ServerID podName/incarnation", name)
		}
	}

	tree := specTree(t)
	seen := map[string]int{}
	walkYAML(tree, func(k string, v any) {
		s, ok := v.(string)
		if !ok {
			return
		}
		switch k {
		case "nodeId":
			seen[k]++
			if strings.Contains(s, "/") {
				t.Errorf("example nodeId %q is a ServerID; nodeId is the pod name", s)
			}
		case "leaderId":
			seen[k]++
			if !serverIDRE.MatchString(s) {
				t.Errorf("example leaderId %q is not a ServerID podName/incarnation", s)
			}
		case "incarnation":
			seen[k]++
			if !incarnationRE.MatchString(s) {
				t.Errorf("example incarnation %q is not a UUID", s)
			}
		}
	})
	for _, k := range []string{"nodeId", "leaderId", "incarnation"} {
		if seen[k] == 0 {
			t.Errorf("no example %s in %s", k, specFile)
		}
	}
}

// exchanges is the planned RabbitMQ topology (v3.1 §20): there is no other
// exchange, so resource.registered.* goes to relief.events.
var exchanges = []string{"relief.requests", "relief.retry", "relief.events", "relief.dlx"}

// TestExamplesUseKnownExchanges: every exchange named in internal.yaml (an
// exchange field or any relief.* value) is one of the planned exchanges, so a
// copied example cannot publish to an exchange that does not exist (#42).
func TestExamplesUseKnownExchanges(t *testing.T) {
	seen := 0
	walkYAML(specTree(t), func(k string, v any) {
		s, ok := v.(string)
		if !ok || (k != "exchange" && !strings.HasPrefix(s, "relief.")) {
			return
		}
		seen++
		if !slices.Contains(exchanges, s) {
			t.Errorf("example %s %q is not a planned exchange %v", k, s, exchanges)
		}
	})
	if seen == 0 {
		t.Errorf("no example exchange in %s", specFile)
	}
}

// specTree is internal.yaml as plain YAML, examples included.
func specTree(t *testing.T) any {
	t.Helper()
	raw, err := os.ReadFile(specFile)
	if err != nil {
		t.Fatal(err)
	}
	var tree any
	if err := yaml.Unmarshal(raw, &tree); err != nil {
		t.Fatal(err)
	}
	return tree
}

// walkYAML calls fn for every mapping key and value under v.
func walkYAML(v any, fn func(k string, v any)) {
	switch n := v.(type) {
	case map[string]any:
		for k, child := range n {
			fn(k, child)
			walkYAML(child, fn)
		}
	case []any:
		for _, child := range n {
			walkYAML(child, fn)
		}
	}
}

// maxScanLimit is the documented per-shard bound on one scan page (§14
// bounded lists; §40 bounded scan).
const maxScanLimit = 1000

// TestScanIsBoundedAndRingFenced: §40 scans are bounded and carry
// ringVersion; limit is required and in [1, maxScanLimit], ringVersion is
// optional and a mismatch is a 409.
func TestScanIsBoundedAndRingFenced(t *testing.T) {
	doc := loadSpec(t)
	for _, r := range routes(doc) {
		if !slices.Contains(storeMethods(r.op), "Scan") {
			continue
		}
		limit := r.params.GetByInAndName("query", "limit")
		switch {
		case limit == nil:
			t.Errorf("%s: no limit parameter", r.name)
		case !limit.Required:
			t.Errorf("%s: limit must be required", r.name)
		default:
			s := limit.Schema.Value
			if s.Min == nil || *s.Min != 1 || s.Max == nil || *s.Max != maxScanLimit {
				t.Errorf("%s: limit must be in [1, %d]; have min %v max %v", r.name, maxScanLimit, s.Min, s.Max)
			}
			if strings.Contains(limit.Description, "no limit") {
				t.Errorf("%s: limit description %q still allows an unbounded scan", r.name, limit.Description)
			}
		}
		rv := r.params.GetByInAndName("query", "ringVersion")
		if rv == nil || rv.Required {
			t.Errorf("%s: ringVersion must be an optional query parameter", r.name)
		}
		if r.op.Responses.Status(409) == nil {
			t.Errorf("%s: no 409 for RING_VERSION_MISMATCH", r.name)
		}
		return
	}
	t.Fatal("no route serves Scan")
}

// TestVersionConflictStatusByRoute: §40 gives 412 for a failed generic CAS
// and 409 for a domain/attempt conflict. The FSM reports a version, status
// or attempt mismatch on a domain transition as VERSION_CONFLICT, so the
// status follows the route: 412 on the generic data routes, 409 on the
// domain transition routes.
func TestVersionConflictStatusByRoute(t *testing.T) {
	doc := loadSpec(t)
	generic := []string{"CreateEntity", "CompareAndSet"}
	domain := []string{"TransitionRequest", "RetryWorkflow", "TransitionResource"}
	found := map[string]bool{}
	for _, r := range routes(doc) {
		methods := storeMethods(r.op)
		switch {
		case slices.ContainsFunc(methods, func(m string) bool { return slices.Contains(generic, m) }):
			if !responseHasCode(r.op, 412, "VERSION_CONFLICT") {
				t.Errorf("%s: 412 must give VERSION_CONFLICT (generic CAS)", r.name)
			}
		case slices.ContainsFunc(methods, func(m string) bool { return slices.Contains(domain, m) }):
			for _, m := range methods {
				found[m] = true
			}
			if r.op.Responses.Status(412) != nil {
				t.Errorf("%s: a domain transition never answers 412", r.name)
			}
			if !responseHasCode(r.op, 409, "VERSION_CONFLICT") {
				t.Errorf("%s: 409 must give VERSION_CONFLICT (expected version, status or attempt mismatch)", r.name)
			}
		}
	}
	for _, m := range domain {
		if !found[m] {
			t.Errorf("no route serves %s", m)
		}
	}
	rows := outcomeTable(t)
	row := rows["version_conflict"]
	for _, want := range []string{"412", "409"} {
		if !strings.Contains(row["internal"], want) {
			t.Errorf("README VERSION_CONFLICT internal cell %q does not mention %s", row["internal"], want)
		}
	}
}

// responseHasCode reports whether the operation's response for status has an
// example whose code is code.
func responseHasCode(op *openapi3.Operation, status int, code string) bool {
	resp := op.Responses.Status(status)
	if resp == nil {
		return false
	}
	mt := resp.Value.Content.Get(jsonMedia)
	if mt == nil {
		return false
	}
	values := []any{mt.Example}
	for _, ex := range mt.Examples {
		values = append(values, ex.Value.Value)
	}
	for _, v := range values {
		if m, ok := v.(map[string]any); ok && m["code"] == code {
			return true
		}
	}
	return false
}

// TestNoLeaderMeansNothingProposed: NO_LEADER covers no known leader and a
// read that could not be fenced; a leader that loses quorum after proposing
// is OUTCOME_UNKNOWN (invariant 5), never NO_LEADER.
func TestNoLeaderMeansNothingProposed(t *testing.T) {
	notes := strings.ToLower(outcomeTable(t)["no leader"]["notes"])
	for _, want := range []string{"no leader known", "could not be fenced", "nothing was proposed"} {
		if !strings.Contains(notes, want) {
			t.Errorf("README no-leader notes %q do not say %q", notes, want)
		}
	}
	if strings.Contains(notes, "quorum lost") {
		t.Errorf("README no-leader notes %q: a lost quorum after proposing is OUTCOME_UNKNOWN", notes)
	}
}

// TestAuthErrorsHaveErrorBody: 401 and 403 use the Error body like every
// other error (README: every error body has code, message, retryable and
// outcomeUnknown).
func TestAuthErrorsHaveErrorBody(t *testing.T) {
	doc := loadSpec(t)
	n := 0
	for _, r := range routes(doc) {
		for _, status := range []int{401, 403} {
			resp := r.op.Responses.Status(status)
			if resp == nil {
				continue
			}
			n++
			if mt := resp.Value.Content.Get(jsonMedia); mt == nil || mt.Schema == nil || mt.Schema.Ref != "#/components/schemas/Error" {
				t.Errorf("%s: %d body is not the Error schema", r.name, status)
			}
		}
	}
	if n == 0 {
		t.Error("no route answers 401 or 403")
	}
}

// TestOutboxLongPoll: GET /internal/outbox may long-poll (§14) for at most
// one second, within the storage wait budget, and its read is strong unless
// stale is asked for (M5).
func TestOutboxLongPoll(t *testing.T) {
	doc := loadSpec(t)
	for _, r := range routes(doc) {
		if !slices.Contains(storeMethods(r.op), "PendingOutbox") {
			continue
		}
		wait := r.params.GetByInAndName("query", "wait")
		switch {
		case wait == nil:
			t.Errorf("%s: no wait parameter", r.name)
		case wait.Required:
			t.Errorf("%s: wait must be optional", r.name)
		default:
			s := wait.Schema.Value
			if s.Min == nil || *s.Min != 0 || s.Max == nil || *s.Max != 1 {
				t.Errorf("%s: wait must be in [0, 1] seconds; have min %v max %v", r.name, s.Min, s.Max)
			}
		}
		if s := responseSchema(r.op, 200); s == nil || !slices.Contains(s.Required, "read") {
			t.Errorf("%s: 200 body must report how the read was served (read)", r.name)
		}
		return
	}
	t.Fatal("no route serves PendingOutbox")
}
