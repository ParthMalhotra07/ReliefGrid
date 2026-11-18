. "$R/stages/lib.sh"
SEED="$SEED" bash scripts/verify-S4.5.sh --kind
job="s45-check-seed$SEED"
$K wait --for=condition=complete "job/$job" --timeout=120s
$K logs "job/$job" > "$R/strict-invcheck.json"
jq -e '.ok == true and .verdict == "clean" and .report.quiescent == true and all(.report.outbox[]; .pending == 0)' "$R/strict-invcheck.json" >/dev/null &&
	ok "strict invcheck: ok, clean, quiescent, outbox pending 0 on every shard" || die "strict invcheck: $(jq -c '{ok, verdict, quiescent: .report.quiescent, outbox: .report.outbox}' "$R/strict-invcheck.json")"
$K get job "$job" -o json | jq -e '[.spec.template.spec.containers[0].env[]? | select(.name == "INTERNAL_AUTH_TOKEN_FILE")] | length == 1' >/dev/null &&
	ok "the invcheck Job ran with the app sender token mounted" || die "the invcheck Job had no INTERNAL_AUTH_TOKEN_FILE"
for d in reliefgrid-api reliefgrid-matcher; do $K rollout status "deployment/$d" --timeout=180s >/dev/null || die "$d not restored"; done
ok "app restored after verify-S4.5"
