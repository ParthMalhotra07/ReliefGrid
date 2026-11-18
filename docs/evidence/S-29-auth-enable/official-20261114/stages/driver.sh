#!/usr/bin/env bash
# Official auth-run driver: each stage in order, timed; stops on the first failure.
R="$(cd "$(dirname "$0")" && pwd)"
export R
TL="$R/timeline.tsv"
printf 'stage\tstart\tend\tseconds\trc\n' > "$TL"
for stage in a-reset b-auth-on c-demos d-401 e-seams f-invcheck g-rotation h-chaos; do
	start="$(date -u +%FT%TZ)" t0=$SECONDS
	echo "=== $start START $stage" | tee -a "$R/driver.log"
	bash "$R/stages/$stage.sh" > "$R/$stage.log" 2>&1
	rc=$?
	end="$(date -u +%FT%TZ)"
	printf '%s\t%s\t%s\t%s\t%s\n' "$stage" "$start" "$end" "$((SECONDS - t0))" "$rc" >> "$TL"
	echo "=== $end END $stage rc=$rc ($((SECONDS - t0)) s)" | tee -a "$R/driver.log"
	if [ "$rc" != 0 ]; then echo "STOPPED at $stage" | tee -a "$R/driver.log"; exit "$rc"; fi
done
echo "ALL STAGES PASSED" | tee -a "$R/driver.log"
