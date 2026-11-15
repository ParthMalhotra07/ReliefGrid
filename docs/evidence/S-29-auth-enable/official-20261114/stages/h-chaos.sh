# The chaos suite with the app quiesced, as in the gate checklist (b3), with internal auth on.
. "$R/stages/lib.sh"
APP_SELECTOR='app.kubernetes.io/instance=reliefgrid,app.kubernetes.io/component in (api,matcher,ui)'
$K get deployment reliefgrid-api reliefgrid-matcher reliefgrid-ui -o jsonpath='{range .items[*]}{.metadata.name}={.spec.replicas}{"\n"}{end}' > "$R/app-replicas-before-chaos.txt"
cat "$R/app-replicas-before-chaos.txt"
test "$(wc -l < "$R/app-replicas-before-chaos.txt")" -eq 3
$K scale deployment reliefgrid-api reliefgrid-matcher reliefgrid-ui --replicas=0
deadline=$((SECONDS + 180))
until [ -z "$($K get pods -l "$APP_SELECTOR" -o name)" ]; do
	[ "$SECONDS" -lt "$deadline" ] || { $K get pods -l "$APP_SELECTOR"; die "app pods still present after 180 s"; }
	sleep 2
done
echo "app quiesced: no api/matcher/ui pods"
chaos_rc=0
make verify-S4.3 2>&1 | tee "$R/chaos-suite.log" || chaos_rc=$?
restore_rc=0
while IFS== read -r name replicas; do $K scale deployment "$name" --replicas="$replicas" || restore_rc=1; done < "$R/app-replicas-before-chaos.txt"
for d in reliefgrid-api reliefgrid-matcher reliefgrid-ui; do $K rollout status "deployment/$d" --timeout=300s || restore_rc=1; done
echo "chaos suite exit $chaos_rc; app restore $([ "$restore_rc" = 0 ] && echo OK || echo FAILED)"
test "$chaos_rc" = 0 && test "$restore_rc" = 0
