package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// errRefused is permanent: the Job exits 2 and its podFailurePolicy fails
// the Job instead of retrying.
var errRefused = errors.New("refused")

// bootstrapper is storagectl bootstrap (v3.1 §10): one pass over the
// shards' manifests, in order. Pods never bootstrap themselves; this is the
// only caller of /mgmt/bootstrap.
type bootstrapper struct {
	store    manifestStore
	peers    peerClient
	mgmtPort string
	poll     time.Duration // between status rounds
	call     time.Duration // per management call
	// hold, tests and the Verify only: after this run bootstraps its first
	// group and the fence applies, wait this long before recording
	// BOOTSTRAPPED, so the Job can be killed inside that window.
	hold time.Duration
	held bool
	log  *slog.Logger
}

// run takes the chart's manifest specs, in shard order. Every manifest
// exists before any group is bootstrapped.
func (b *bootstrapper) run(ctx context.Context, specs []manifest) error {
	for _, spec := range specs {
		if err := b.ensure(ctx, spec); err != nil {
			return err
		}
	}
	for _, spec := range specs {
		if err := b.shard(ctx, spec.Name); err != nil {
			return err
		}
	}
	return nil
}

// ensure creates spec's manifest as PENDING if there is none. An existing
// manifest is the record and is used as it is: a spec never resets it, so
// nothing the chart renders (or a helm rollback replays) can undo a phase.
func (b *bootstrapper) ensure(ctx context.Context, spec manifest) error {
	spec.Phase, spec.PeerIDs = phasePending, nil
	switch err := b.store.Create(ctx, spec); {
	case err == nil:
		b.log.Info(spec.GroupID+": created manifest", "manifest", spec.Name)
	case errors.Is(err, errExists):
	default:
		return err
	}
	return nil
}

// shard drives one manifest to BOOTSTRAPPED, or refuses. Every decision is
// made from a fresh read of the manifest and the peers, so a run killed at
// any point is resumed by the next one.
func (b *bootstrapper) shard(ctx context.Context, name string) error {
	issued := false // this run's bootstrap call succeeded
	lastWhy := ""
	for {
		m, err := b.store.Get(ctx, name)
		if err != nil {
			return err
		}
		sts := b.statuses(ctx, m)
		act, why := decide(m, sts)
		logf := func(msg string, args ...any) {
			b.log.Info(fmt.Sprintf("%s: %s", m.GroupID, msg), append([]any{"phase", m.Phase}, args...)...)
		}
		switch act {
		case actRefuse:
			return fmt.Errorf("%w: %s (%s): %s", errRefused, m.GroupID, m.Name, why)

		case actWait:
			if why != lastWhy {
				logf("waiting", "reason", why)
				lastWhy = why
			}

		case actBootstrap:
			if m.Phase == phasePending {
				// Record the exact voters before anything can create the
				// group, so a retry re-issues the same configuration.
				m.Phase, m.PeerIDs = phaseBootstrapping, nil
				for _, p := range m.Peers {
					m.PeerIDs = append(m.PeerIDs, sts[p.PodName].ServerID)
				}
				if _, err := b.store.Update(ctx, m); errors.Is(err, errConflict) {
					continue
				} else if err != nil {
					return err
				}
				logf("recorded voters", "peerIds", m.PeerIDs)
				continue
			}
			voters := make([]peerVoter, len(m.Peers))
			for i, p := range m.Peers {
				voters[i] = peerVoter{ID: m.PeerIDs[i], Address: p.Address}
			}
			addr, err := mgmtAddr(m.Peers[0].Address, b.mgmtPort)
			if err != nil {
				return err
			}
			callCtx, cancel := context.WithTimeout(ctx, b.call)
			err = b.peers.Bootstrap(callCtx, addr, voters)
			cancel()
			switch {
			case err == nil:
				issued = true
				logf("bootstrapped", "pod", m.Peers[0].PodName, "voters", len(voters))
				continue
			case errors.Is(err, errHasState):
				logf("bootstrap target already has state", "err", err)
				continue
			case errors.Is(err, errRejected):
				return fmt.Errorf("%s: %w", m.GroupID, err)
			default:
				// The call may still have taken effect; the next status
				// round shows it as state on the target.
				logf("bootstrap outcome unknown; re-reading", "err", err)
			}

		case actFence:
			ok, fwhy := fenced(m, sts)
			if !ok {
				if fwhy != lastWhy {
					logf("waiting for the initialization fence", "reason", fwhy)
					lastWhy = fwhy
				}
				break
			}
			if m.Phase == phaseBootstrapped {
				logf("verified", "fence", fwhy)
				return nil
			}
			if issued && b.hold > 0 && !b.held {
				logf("holding before recording BOOTSTRAPPED", "seconds", b.hold.Seconds())
				b.held = true
				if err := sleep(ctx, b.hold); err != nil {
					return err
				}
				continue
			}
			m.Phase = phaseBootstrapped
			if _, err := b.store.Update(ctx, m); errors.Is(err, errConflict) {
				continue
			} else if err != nil {
				return err
			}
			if issued {
				logf("bootstrapped and fenced", "fence", fwhy)
			} else {
				logf("group already initialized; verified", "fence", fwhy)
			}
			return nil
		}
		if err := sleep(ctx, b.poll); err != nil {
			return fmt.Errorf("%s: %w (last: %s)", m.GroupID, err, why)
		}
	}
}

// statuses asks every peer for its status; unreachable peers are absent.
func (b *bootstrapper) statuses(ctx context.Context, m manifest) map[string]nodeStatus {
	sts := map[string]nodeStatus{}
	for _, p := range m.Peers {
		addr, err := mgmtAddr(p.Address, b.mgmtPort)
		if err != nil {
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, b.call)
		s, err := b.peers.Status(callCtx, addr)
		cancel()
		if err == nil {
			sts[p.PodName] = s
		}
	}
	return sts
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
