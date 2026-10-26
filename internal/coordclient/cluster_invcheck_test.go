package coordclient_test

import (
	"context"
	"testing"
	"time"

	"github.com/ParthMalhotra07/ReliefGrid/internal/invcheck"
	"github.com/ParthMalhotra07/ReliefGrid/internal/invcheck/invchecktest"
	"github.com/ParthMalhotra07/ReliefGrid/loadtest/ledger"
)

// Verify (S2.2) on the real cluster: invcheck passes the clean workflow read
// through coordclient, paging every shard with the cursor, and the ledger of
// a load run through coordclient verifies with no acknowledged write lost.
func TestClusterInvcheckAndLedger(t *testing.T) {
	c := newCluster(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sd, err := invchecktest.Seed(ctx, c.client, "cl", 30)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	rep, err := invcheck.Run(ctx, c.client, invcheck.Options{PageLimit: 8})
	if err != nil || rep.Verdict() != "clean" {
		t.Fatalf("invcheck = %s %+v, %v; want clean", rep.Verdict(), rep.Violations, err)
	}
	if rep.Requests != sd.Requests || rep.Resources != sd.Resources || rep.PartialPages != 0 {
		t.Fatalf("read %d requests, %d resources, %d partial pages; seeded %d and %d", rep.Requests, rep.Resources, rep.PartialPages, sd.Requests, sd.Resources)
	}
	pending := 0
	for _, b := range rep.Outbox {
		pending += b.Pending
	}
	if len(rep.Outbox) != 3 || pending != sd.Events {
		t.Fatalf("backlog %+v, want %d events over 3 shards", rep.Outbox, sd.Events)
	}

	l, err := ledger.Driver{Store: c.client, Prefix: "cl-load", Workers: 4, Keys: 2, Ops: 20, Retries: 3, At: invchecktest.T0}.Run(ctx)
	if err != nil {
		t.Fatalf("driver: %v", err)
	}
	if res, err := l.Resolve(ctx, c.client); err != nil || res.StillUnknown != 0 {
		t.Fatalf("resolve = %+v, %v", res, err)
	}
	v, err := l.Verify(ctx, c.client)
	if err != nil || !v.OK() || v.Checked != len(l.Acked) || len(l.Acked) == 0 {
		t.Fatalf("verdict %+v over %d acknowledged writes, %v; want OK", v, len(l.Acked), err)
	}
}
