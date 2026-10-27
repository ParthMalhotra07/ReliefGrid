# shellcheck shell=bash
# Leaked port-forwards (#50). A script that runs `kubectl port-forward ... &` must background
# kubectl itself (so $! is the process to kill), never a shell function or a $(...) subshell,
# and kill every one it started on exit. repo_port_forwards finds any that outlived their
# script, so the Verify can fail on them and the gate can refuse to start.

# repo_port_forwards [root]: "<pid> <command line>" for every kubectl port-forward process whose
# working directory is <root> or below it (default: PF_REPO_ROOT, else this repository).
repo_port_forwards() {
	local root="${PF_REPO_ROOT:-${1:-$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)}}" pid cwd
	root="${root%/}"
	for pid in $(pgrep -f -- port-forward 2>/dev/null); do
		cwd="$(readlink "/proc/$pid/cwd" 2>/dev/null)" || continue
		[[ "$cwd/" == "$root/"* ]] || continue
		# argv, one element per line: a kubectl executable and a port-forward subcommand
		tr '\0' '\n' <"/proc/$pid/cmdline" 2>/dev/null |
			awk '/(^|\/)kubectl$/ { k = 1 } $0 == "port-forward" { p = 1 } END { exit !(k && p) }' || continue
		echo "$pid $(tr '\0' ' ' <"/proc/$pid/cmdline" 2>/dev/null | sed 's/ $//')"
	done
}
