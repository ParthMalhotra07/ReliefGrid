. "$R/stages/lib.sh"
kind get clusters 2>&1 | quiet_grep 'No kind clusters found' || { kind get clusters; die "a kind cluster exists before the reset"; }
make demo-reset INTERNAL_AUTH=true
