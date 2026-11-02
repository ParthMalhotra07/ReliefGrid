# One saved S4.3 report -> chaos/judge Input. The report does not carry the
# post-grace counters added after it ran, so they are filled with lower
# bounds the report proves (see the comments); everything else is copied.
. as $r
| ($r.availability.fromMs + $r.availability.graceMs) as $inside
| ($r.availability.toMs) as $to
| (($r.availability.graceMs / $r.reads.intervalMs | floor) + 1) as $perLoop   # read starts one loop can make in the grace
| ($r.reads.readers + ($r.reads.probes | length)) as $loops
| ([$r.availability.shards[] | select(.errorsDuring > 0) | .shard]) as $errShards
| ([$r.errorsObserved[] | select(.source == "load" and .at >= $inside and .at <= $to)] | length) as $loadErrsInside
| {experiment: $r.experiment, shard: $r.target.shard, target: $r.target.pod, negative: ($r.control.negative // ""),
   bounds: {electionMaxSeconds: 10, recoveryMaxSeconds: 60},
   verify: {ok: ($r.ledger.exitCode == 0), incomplete: ($r.ledger.incomplete // ""),
     ledger: {acked: $r.load.acked, unknown: $r.load.unknown, rejected: $r.load.rejected, errorCount: ($r.errorsObserved | map(select(.source=="load")) | length)},
     resolution: $r.ledger.resolution,
     verdict: {checked: $r.ledger.checked, lost: $r.ledger.lost, staleReads: ([range(0; $r.reads.staleCount)] | map({reason: "saved"})), reads: $r.reads.verdict},
     availability: ($r.availability | .shards |= map(
       . as $w
       | . + {
         # acked after the grace and by the heal: each is an attempt that ended inside;
         # failed attempts inside, when this is the only shard with errors
         writeAttemptsInsideFault: ($w.acksInsideFault + (if $errShards == [$w.shard] then $loadErrsInside else 0 end)),
         # every read that started in [from, to] minus the most the loops could start in the grace
         readsInsideFault: ([$w.readsDuring - $perLoop * $loops, $w.readsServedInsideFault] | max | if . < 0 then 0 else . end),
         probeReadsInsideFault: ($w.probeReadsDuring | with_entries(.value = ([.value - $perLoop, ($w.probeServedInsideFault[.key] // 0)] | max | if . < 0 then 0 else . end)))
       }))},
   verifyExit: $r.ledger.exitCode,
   check: {ok: ($r.invcheck.exitCode == 0), verdict: $r.invcheck.verdict, incomplete: ($r.invcheck.incomplete // ""),
     report: {quiescent: $r.invcheck.quiescent, violations: $r.invcheck.violations}},
   checkExit: $r.invcheck.exitCode,
   harness: $r.harness}
