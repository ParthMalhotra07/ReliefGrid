package fsm

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

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
	op, err := opOf(body)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("fsm: encode %s: %w", op, err)
	}
	return json.Marshal(envelope{Op: op, Body: b})
}

func opOf(body any) (Op, error) {
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
	// The canonical form is the re-encoded typed body, so formatting
	// differences in the entry do not change the hash.
	canon, err := json.Marshal(c.body)
	if err != nil {
		return command{}, fmt.Errorf("%s canonical form: %w", env.Op, err)
	}
	sum := sha256.Sum256(append([]byte(string(env.Op)+"\x00"), canon...))
	c.hash = hex.EncodeToString(sum[:])
	return c, nil
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
