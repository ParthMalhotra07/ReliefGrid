package coordclient_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ParthMalhotra07/ReliefGrid/internal/coordclient"
	"github.com/ParthMalhotra07/ReliefGrid/internal/internalapi"
	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// withoutServedMode serves h's answer with the served-mode field removed
// from the wire entirely (the top-level consistency of a scan, the read
// info's of every other read), not merely empty.
func withoutServedMode(h func(http.ResponseWriter, *http.Request, int)) func(http.ResponseWriter, *http.Request, int) {
	return func(w http.ResponseWriter, r *http.Request, n int) {
		rec := httptest.NewRecorder()
		h(rec, r, n)
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		delete(body, "consistency")
		if read, ok := body["read"].(map[string]any); ok {
			delete(read, "consistency")
		}
		writeJSON(w, rec.Code, body)
	}
}

// #166 review: an authoritative read (linearizable, the default, or strong)
// must be answered with a served mode at least as strong, on every read
// path, whether the mode came from WithReadConsistency, the query, or the
// client's linearizable default. A missing, empty, unknown or stale served
// mode is the same retryable read error as a stale answer, never a result.
// An explicit stale read accepts any label: it is only a hint.
func TestAuthoritativeReadRequiresAnAuthoritativeServedMode(t *testing.T) {
	served := []struct {
		name   string
		answer func(http.ResponseWriter, *http.Request, int)
		ok     bool
	}{
		{"missing", withoutServedMode(readAnswer(internalapi.ReadInfoConsistencyLinearizable)), false},
		{"empty", readAnswer(""), false},
		{"unknown", readAnswer("eventual"), false},
		{"stale", readAnswer(internalapi.ReadInfoConsistencyStale), false},
		{"linearizable", readAnswer(internalapi.ReadInfoConsistencyLinearizable), true},
		{"strong", readAnswer(internalapi.ReadInfoConsistencyStrong), true},
	}
	clients := []struct {
		name string
		opts []coordclient.Option
		scan store.Consistency // the scan's and outbox page's own mode
	}{
		{"default client", nil, ""},
		{"default client, query asks linearizable", nil, store.ConsistencyLinearizable},
		{"default client, query asks strong", nil, store.ConsistencyStrong},
		{"WithReadConsistency(linearizable)", []coordclient.Option{coordclient.WithReadConsistency(store.ConsistencyLinearizable)}, ""},
		{"WithReadConsistency(strong)", []coordclient.Option{coordclient.WithReadConsistency(store.ConsistencyStrong)}, ""},
	}
	for _, cl := range clients {
		for _, sv := range served {
			t.Run(cl.name+"/served "+sv.name, func(t *testing.T) {
				f := newFake(t)
				c := newClient(t, f, cl.opts...)
				f.setAnswer(sv.answer)
				errs := readAll(t, c, cl.scan)
				if len(errs) != 5 {
					t.Fatalf("made %d reads, want 5", len(errs))
				}
				for name, err := range errs {
					var ce *coordclient.Error
					switch {
					case sv.ok && err != nil:
						t.Errorf("%s: %v", name, err)
					case !sv.ok && (!errors.As(err, &ce) || !ce.Retryable || errors.Is(err, store.ErrUnknownOutcome) || errors.Is(err, store.ErrNotFound)):
						t.Errorf("%s: err = %v, want a retryable read error", name, err)
					}
				}
			})
		}
	}
}

// An explicit stale read keeps its diagnostic meaning: whatever the answer
// says about how it was served, it is returned (and never proof of anything).
func TestStaleReadAcceptsAnyServedMode(t *testing.T) {
	for _, served := range []internalapi.ReadInfoConsistency{"", "eventual", internalapi.ReadInfoConsistencyStale, internalapi.ReadInfoConsistencyLinearizable} {
		f := newFake(t)
		c := newClient(t, f, coordclient.WithReadConsistency(store.ConsistencyStale))
		f.setAnswer(readAnswer(served))
		for name, err := range readAll(t, c, "") {
			if err != nil {
				t.Errorf("stale read, served %q: %s: %v", served, name, err)
			}
		}
	}
}
