package mq_test

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/mq"
)

func TestEmbeddedCreatedSchemaMatchesFrozenContract(t *testing.T) {
	frozen, err := os.ReadFile("../../contracts/events/request_created.json")
	if err != nil {
		t.Fatal(err)
	}
	embedded, err := os.ReadFile("schemas/request_created.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(frozen, embedded) {
		t.Fatal("consumer event schema differs from the frozen contract")
	}
}

func TestTopologyDefinitionsAndHelmStayIdentical(t *testing.T) {
	var d struct {
		Exchanges []struct {
			Name, Type string
			Durable    bool
		}
		Queues []struct {
			Name      string
			Durable   bool
			Arguments map[string]any
		}
		Bindings []struct {
			Source, Destination string
			RoutingKey          string `json:"routing_key"`
		}
		Policies []struct {
			Name, Pattern string
			Definition    map[string]any
		}
	}
	if err := json.Unmarshal(mq.Definitions(), &d); err != nil {
		t.Fatal(err)
	}
	if len(d.Exchanges) != 4 || len(d.Queues) != 5 {
		t.Fatalf("exchanges=%d queues=%d; want 4/5", len(d.Exchanges), len(d.Queues))
	}
	wantExchanges := map[string]string{"relief.requests": "topic", "relief.retry": "direct", "relief.events": "topic", "relief.dlx": "fanout"}
	for _, e := range d.Exchanges {
		if !e.Durable || wantExchanges[e.Name] != e.Type {
			t.Errorf("incorrect exchange: %+v", e)
		}
		delete(wantExchanges, e.Name)
	}
	if len(wantExchanges) != 0 {
		t.Fatalf("missing exchanges: %v", wantExchanges)
	}
	wantTTL := map[string]float64{"retry.5s": 5000, "retry.30s": 30000, "retry.120s": 120000}
	for _, q := range d.Queues {
		if !q.Durable || q.Arguments["x-queue-type"] != "quorum" || q.Arguments["x-quorum-initial-group-size"] != float64(1) {
			t.Errorf("unsafe queue: %+v", q)
		}
		if ttl, ok := wantTTL[q.Name]; ok {
			if q.Arguments["x-message-ttl"] != ttl {
				t.Errorf("%s TTL=%v want %v", q.Name, q.Arguments["x-message-ttl"], ttl)
			}
			delete(wantTTL, q.Name)
		}
	}
	if len(wantTTL) != 0 {
		t.Fatalf("missing retry tiers: %v", wantTTL)
	}
	for _, want := range [][3]string{
		{"relief.requests", "matching.requests", "request.created.*"}, {"relief.requests", "matching.requests", "request.retry"},
		{"relief.retry", "retry.5s", "delay.5s"}, {"relief.retry", "retry.30s", "delay.30s"},
		{"relief.retry", "retry.120s", "delay.120s"}, {"relief.dlx", "matching.dead", ""},
	} {
		found := false
		for _, b := range d.Bindings {
			if b.Source == want[0] && b.Destination == want[1] && b.RoutingKey == want[2] {
				found = true
			}
		}
		if !found {
			t.Errorf("missing binding: %v", want)
		}
	}
	if len(d.Policies) < 2 {
		t.Fatal("retry/matching safety policies missing")
	}
	for _, p := range d.Policies {
		if p.Definition["dead-letter-strategy"] != "at-least-once" || p.Definition["overflow"] != "reject-publish" {
			t.Errorf("unsafe policy: %+v", p)
		}
	}
	b, err := os.ReadFile("../../deploy/helm/reliefgrid/files/rabbitmq-definitions.json")
	if err != nil {
		t.Fatal(err)
	}
	var helm, embedded any
	if err := json.Unmarshal(b, &helm); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(mq.Definitions(), &embedded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(helm, embedded) {
		t.Fatal("Helm and runtime topology drift")
	}
}
