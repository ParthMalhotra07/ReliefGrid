package contracts

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

func publicSpec(t *testing.T) *openapi3.T {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile("public.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return doc
}

// These checks pin the reviewed public/store boundary: accepting a field
// the FSM cannot persist would silently lose client input in A1.1.
func TestPublicContractStoreBoundary(t *testing.T) {
	doc := publicSpec(t)
	for _, name := range []string{"CreateRequestBody", "RequestResponse"} {
		s := doc.Components.Schemas[name].Value
		for _, field := range []string{"details", "peopleAffected", "urgency"} {
			if s.Properties[field] == nil {
				t.Errorf("%s missing stored field %s", name, field)
			}
		}
		for _, field := range []string{"title", "requestorId", "windowStart", "windowEnd", "currentAttempt", "workflowGeneration"} {
			if s.Properties[field] != nil {
				t.Errorf("%s exposes unsupported or private field %s", name, field)
			}
		}
	}
	req := doc.Components.Schemas["RequestResponse"].Value
	if p := req.Properties["assignedResourceId"]; p == nil || !p.Value.Nullable || !slices.Contains(req.Required, "assignedResourceId") {
		t.Error("assignedResourceId must be present and nullable when unassigned")
	}
	for _, field := range []string{"retryCount", "nextAttemptAt", "lastError"} {
		if p := req.Properties[field]; p == nil || !p.Value.ReadOnly {
			t.Errorf("request workflow summary %s must be read-only", field)
		}
	}
	for _, name := range []string{"RegisterResourceBody", "ResourceResponse"} {
		s := doc.Components.Schemas[name].Value
		if p := s.Properties["reusable"]; p == nil || !p.Value.Type.Is("boolean") {
			t.Errorf("%s must use reusable boolean", name)
		}
		if s.Properties["disposition"] != nil {
			t.Errorf("%s confuses reusability with cleanup disposition", name)
		}
	}
	resource := doc.Components.Schemas["ResourceResponse"].Value
	for _, field := range []string{"activeClaim", "locationRevision"} {
		if resource.Properties[field] == nil {
			t.Errorf("ResourceResponse missing %s", field)
		}
	}
	for _, name := range []string{"RequestResponse", "ResourceResponse"} {
		if !doc.Components.Schemas[name].Value.Properties["version"].Value.Type.Is("string") {
			t.Errorf("%s version must be an opaque string", name)
		}
	}
}

func TestPublicCreateReplayAndErrorContract(t *testing.T) {
	doc := publicSpec(t)
	key := doc.Components.Parameters["IdempotencyKey"].Value
	if key.Required {
		t.Error("Idempotency-Key must be optional; no-key retries can duplicate")
	}
	for _, path := range []string{"/requests", "/resources"} {
		op := doc.Paths.Value(path).Post
		if op.Responses.Status(200) != nil {
			t.Errorf("POST %s must replay the original 201", path)
		}
		if r := op.Responses.Status(201); r == nil || r.Value.Headers["Location"] == nil {
			t.Errorf("POST %s must return the original Location", path)
		}
	}
	errorSchema := doc.Components.Schemas["ErrorEnvelope"].Value
	for _, code := range []string{"INVALID", "NOT_FOUND", "DOMAIN_CONFLICT", "OPERATION_ID_REUSED", "CONFLICT_IDEMPOTENCY_KEY", "VERSION_CONFLICT", "OUTCOME_UNKNOWN", "NO_LEADER"} {
		if !slices.Contains(errorSchema.Properties["code"].Value.Enum, any(code)) {
			t.Errorf("missing public error code %s", code)
		}
	}
	for _, code := range []int{401, 403} {
		if doc.Paths.Value("/events").Get.Responses.Status(code) == nil {
			t.Errorf("SSE missing %d response", code)
		}
	}
	for _, field := range []string{"positions", "failedShards"} {
		if doc.Components.Schemas["ResourceListResponse"].Value.Properties[field] == nil {
			t.Errorf("resource listing missing %s", field)
		}
	}
}

// Validate examples and adversarial client inputs against the actual schema,
// rather than checking only that expected field names occur in YAML.
func TestPublicContractExamplesAndInvalidInputs(t *testing.T) {
	doc := publicSpec(t)
	for name, ref := range doc.Components.Schemas {
		if ex := ref.Value.Example; ex != nil {
			if err := ref.Value.VisitJSON(ex); err != nil {
				t.Errorf("%s example: %v", name, err)
			}
		}
	}
	create := doc.Components.Schemas["CreateRequestBody"].Value
	valid := map[string]any{"requestType": "WATER", "location": map[string]any{"lat": 0.0, "lng": 0.0}, "details": "help"}
	if err := create.VisitJSON(valid); err != nil {
		t.Fatalf("valid control create rejected: %v", err)
	}
	for _, input := range []string{
		`{"requestType":"WATER","location":{"lat":91,"lng":0},"details":"help"}`,
		`{"requestType":"WATER","location":{"lat":0,"lng":181},"details":"help"}`,
		`{"requestType":"WATER","location":{"lat":0,"lng":0},"details":"help","peopleAffected":0}`,
		`{"requestType":"WATER","location":{"lat":0,"lng":0},"details":"help","urgency":"UNKNOWN"}`,
		`{"requestType":"WATER","location":{"lat":0,"lng":0},"details":"help","assignedResourceId":"x"}`,
		`{"requestType":"WATER","location":{"lat":0,"lng":0},"details":"help","windowEnd":"2026-10-26T00:00:00Z"}`,
	} {
		var value any
		if err := json.Unmarshal([]byte(input), &value); err != nil {
			t.Fatal(err)
		}
		if err := create.VisitJSON(value); err == nil {
			t.Errorf("accepted invalid create: %s", input)
		}
	}
	input := map[string]any{"requestType": "WATER", "location": map[string]any{"lat": 0.0, "lng": 0.0}, "details": strings.Repeat("x", 4097)}
	if err := create.VisitJSON(input); err == nil {
		t.Error("accepted unbounded request details")
	}
}

func TestPublicEventSchemaVersionsAndClaimIdentity(t *testing.T) {
	paths, err := os.ReadDir("events")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if !strings.HasSuffix(path.Name(), ".json") {
			continue
		}
		body, err := os.ReadFile("events/" + path.Name())
		if err != nil {
			t.Fatal(err)
		}
		var schema struct {
			Properties map[string]json.RawMessage `json:"properties"`
		}
		if err := json.Unmarshal(body, &schema); err != nil {
			t.Fatal(err)
		}
		var version struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(schema.Properties["schemaVersion"], &version); err != nil {
			t.Fatal(err)
		}
		if version.Type != "integer" {
			t.Errorf("%s schemaVersion must match OutboxEvent's integer", path.Name())
		}
		if path.Name() == "resource_state_changed.json" {
			var payload struct {
				Properties map[string]json.RawMessage `json:"properties"`
			}
			if err := json.Unmarshal(schema.Properties["payload"], &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Properties["claimId"] != nil || payload.Properties["requestId"] == nil || payload.Properties["attemptId"] == nil {
				t.Error("resource claim event must use requestId/attemptId, not an invented claimId")
			}
		}
	}
}
