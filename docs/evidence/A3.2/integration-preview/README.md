# Reconciler integration preview

This is a local integration of #84, #86 and #90 with current main's #87.
`tested-source.txt` records its local commit. It was not pushed as another
stacked PR. These combined demo results predate #90's base change to #86.

Demo 5 passed the unchanged 120-second settle assertions for 500 creates,
50 reciprocal matches and 450 WAITING, with zero ready/unacked at the queue
check and strict clean/quiescent invcheck after confirmed drain. Demo 8 passed
with one actual interrupted create, original-key/payload recovery, both pod
replacements and strict invcheck. Compact checks/results/audits are retained.

The first integrated demo-8 preflight failed because per-pod `kubectl wait`
does not establish one simultaneous Ready snapshot. #84 now awaits the strict
snapshot itself within the same 90 seconds; the corrected run passed.

The tested wakeup delta is now applied to #90's real production source and
the patch artifact is removed. #90 stacks only on #86. The generic supervisor
retains the connection through both consumers' bounded drain. Current-branch
checks are recorded in ../finalized/; #84's demo scripts stay in their own PR.

The wakeup regression first failed with request PENDING at shutdown. With the
patch, the admitted bounded attempt commits WAITING and is acknowledged before
the channel closes. Subsequent failures retain the same unacked delivery while
admission remains open; a shutdown does not start another attempt. The A2.4
25-hop verifier still reports deliveryLimit20/businessRetryCount1 and ALL PASS.
Full check/gen and affected race count3 also passed in the integrated checkout.

Limits: these local combined results are not main post-merge verification or
the joint A3.3 gate. Both prerequisite merges and a final demo8 on the merged
source remain required to close A3.2. All five workers are retained in #90.
