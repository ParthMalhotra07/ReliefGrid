package fsm

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/ParthMalhotra07/ReliefGrid/internal/store"
)

// Op names a command in the replicated log.
type Op string

const (
	OpCreateEntity        Op = "CreateEntity"
	OpCompareAndSet       Op = "CompareAndSet"
	OpTransitionRequest   Op = "TransitionRequest"
	OpRetryWorkflow       Op = "RetryWorkflow"
	OpTransitionResource  Op = "TransitionResource"
	OpTryClaim            Op = "TryClaim"
	OpCloseAttempt        Op = "CloseAttempt"
	OpMarkOutboxPublished Op = "MarkOutboxPublished"
)

type envelope struct {
	Op   Op              `json:"op"`
	Body json.RawMessage `json:"body"`
}

// command is a decoded log entry.
type command struct {
	op   Op
	meta store.Meta
	body any    // pointer to one of the store command types
	hash string // payload hash over op and the canonical body
}

// Encode builds the log entry for one command body (a store.CreateEntity,
// store.CompareAndSet, ... value or pointer).
func Encode(body any) ([]byte, error) {
	op, err := OpOf(body)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("fsm: encode %s: %w", op, err)
	}
	return json.Marshal(envelope{Op: op, Body: b})
}

// OpOf names the command body would be encoded as.
func OpOf(body any) (Op, error) {
	switch body.(type) {
	case store.CreateEntity, *store.CreateEntity:
		return OpCreateEntity, nil
	case store.CompareAndSet, *store.CompareAndSet:
		return OpCompareAndSet, nil
	case store.TransitionRequest, *store.TransitionRequest:
		return OpTransitionRequest, nil
	case store.RetryWorkflow, *store.RetryWorkflow:
		return OpRetryWorkflow, nil
	case store.TransitionResource, *store.TransitionResource:
		return OpTransitionResource, nil
	case store.TryClaim, *store.TryClaim:
		return OpTryClaim, nil
	case store.CloseAttempt, *store.CloseAttempt:
		return OpCloseAttempt, nil
	case store.MarkOutboxPublished, *store.MarkOutboxPublished:
		return OpMarkOutboxPublished, nil
	}
	return "", fmt.Errorf("fsm: no command for %T", body)
}

// decode parses an entry strictly: unknown fields are rejected, so a forged
// entry cannot smuggle fields (such as activeClaim) past the typed commands.
func decode(entry []byte) (command, error) {
	var env envelope
	if err := strict(entry, &env); err != nil {
		return command{}, err
	}
	var (
		c    command
		meta *store.Meta
	)
	c.op = env.Op
	switch env.Op {
	case OpCreateEntity:
		v := &store.CreateEntity{}
		c.body, meta = v, &v.Meta
	case OpCompareAndSet:
		v := &store.CompareAndSet{}
		c.body, meta = v, &v.Meta
	case OpTransitionRequest:
		v := &store.TransitionRequest{}
		c.body, meta = v, &v.Meta
	case OpRetryWorkflow:
		v := &store.RetryWorkflow{}
		c.body, meta = v, &v.Meta
	case OpTransitionResource:
		v := &store.TransitionResource{}
		c.body, meta = v, &v.Meta
	case OpTryClaim:
		v := &store.TryClaim{}
		c.body, meta = v, &v.Meta
	case OpCloseAttempt:
		v := &store.CloseAttempt{}
		c.body, meta = v, &v.Meta
	case OpMarkOutboxPublished:
		v := &store.MarkOutboxPublished{}
		c.body, meta = v, &v.Meta
	default:
		return command{}, fmt.Errorf("unknown op %q", env.Op)
	}
	if err := strict(env.Body, c.body); err != nil {
		return command{}, fmt.Errorf("%s body: %w", env.Op, err)
	}
	c.meta = *meta
	hash, err := payloadHash(env.Op, c.body)
	if err != nil {
		return command{}, err
	}
	c.hash = hash
	return c, nil
}

// payloadHash is the operationId dedupe hash stored in operation_results.
// The canonical form is the re-encoded typed body, so formatting differences
// in the entry do not change the hash. It is persisted replicated state (in
// every snapshot): changing how it is computed is a state migration.
func payloadHash(op Op, body any) (string, error) {
	canon, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("%s canonical form: %w", op, err)
	}
	sum := sha256.Sum256(append([]byte(string(op)+"\x00"), canon...))
	return hex.EncodeToString(sum[:]), nil
}

// outboxOf returns the outbox events of a decoded command body, or nil for
// commands that carry none.
func outboxOf(body any) *[]store.OutboxEvent {
	switch c := body.(type) {
	case *store.CreateEntity:
		return &c.Outbox
	case *store.TransitionRequest:
		return &c.Outbox
	case *store.RetryWorkflow:
		return &c.Outbox
	case *store.TransitionResource:
		return &c.Outbox
	}
	return nil
}

// traceField is one top-level "traceparent" member of a JSON object: span is
// the byte range from just after the key to the end of the value (the colon
// included), value is the raw value.
type traceField struct {
	start, end int
	value      json.RawMessage
}

// traceparentField finds the single top-level "traceparent" member of an
// event envelope. ok is false if p is not one JSON object or has no such
// member or more than one.
func traceparentField(p []byte) (traceField, bool) {
	dec := json.NewDecoder(bytes.NewReader(p))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return traceField{}, false
	}
	var found []traceField
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return traceField{}, false
		}
		key, ok := tok.(string)
		if !ok {
			return traceField{}, false
		}
		start := int(dec.InputOffset())
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return traceField{}, false
		}
		if key == "traceparent" {
			found = append(found, traceField{start: start, end: int(dec.InputOffset()), value: v})
		}
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return traceField{}, false
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return traceField{}, false
	}
	if len(found) != 1 {
		return traceField{}, false
	}
	return found[0], true
}

// withTraceparent returns p with the value of its top-level "traceparent"
// member replaced by value, every other byte unchanged.
func withTraceparent(p []byte, value json.RawMessage) (json.RawMessage, bool) {
	f, ok := traceparentField(p)
	if !ok {
		return nil, false
	}
	out := make([]byte, 0, len(p)-(f.end-f.start)+len(value)+1)
	out = append(out, p[:f.start]...)
	out = append(out, ':')
	out = append(out, value...)
	out = append(out, p[f.end:]...)
	return out, true
}

func strict(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing data")
	}
	return nil
}

// Describe returns a log entry's op and operationId without applying it.
// It is for observers outside Apply (the S4.6b test seams); Apply never
// calls it.
func Describe(entry []byte) (Op, string, error) {
	c, err := decode(entry)
	if err != nil {
		return "", "", fmt.Errorf("fsm: describe entry: %w", err)
	}
	return c.op, c.meta.OperationID, nil
}
