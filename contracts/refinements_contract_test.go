package contracts

import "testing"

func TestPublicRefinementsContract(t *testing.T) {
	doc := publicSpec(t)
	for _, path := range []string{"/requests/{requestId}/status", "/resources/{resourceId}/status"} {
		op := doc.Paths.Value(path).Put
		found := false
		for _, p := range op.Parameters {
			if p.Value.Name == "If-Match" && p.Value.In == "header" {
				found = true
				if p.Value.Required {
					t.Error("If-Match must preserve unconditional clients")
				}
			}
		}
		if !found {
			t.Errorf("%s: missing If-Match", path)
		}
		if op.Responses.Status(412) == nil {
			t.Errorf("%s: missing 412", path)
		}
		if op.Responses.Status(200).Value.Headers["ETag"] == nil {
			t.Errorf("%s: missing ETag", path)
		}
	}
	for _, path := range []string{"/requests", "/resources"} {
		op := doc.Paths.Value(path).Get
		found := false
		for _, p := range op.Parameters {
			if p.Value.Name == "cursor" && p.Value.In == "query" {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: missing cursor", path)
		}
		if doc.Paths.Value(path).Post.Responses.Status(201).Value.Headers["ETag"] == nil {
			t.Errorf("%s: create lacks ETag", path)
		}
	}
	for _, name := range []string{"RequestListResponse", "ResourceListResponse"} {
		if doc.Components.Schemas[name].Value.Properties["nextCursor"] == nil {
			t.Errorf("%s: missing continuation", name)
		}
	}
	if doc.Paths.Value("/requests/{requestId}").Get.Responses.Status(200).Value.Headers["ETag"] == nil {
		t.Error("request read lacks ETag")
	}
}
