# shellcheck shell=bash
# internal-auth-env.sh: internal HTTP credentials for the S-side cluster callers (#29),
# sourced by chaos/scripts/lib.sh, loadtest/run.sh, scripts/verify-S4.6b.sh and
# scripts/verify-storage-k8s.sh. The caller inventory is in the PR that added this file.
#
# With internalAuth.enabled=false (the default) nothing changes: no file is written,
# no variable is exported, the curl argument arrays are empty and a Job passes through
# unchanged, so every caller behaves exactly as before.
#
# With it on, the storage release owns two Secrets (deploy/helm/storage):
# reliefgrid-internal-auth (app: coordinator data, ring, scan, outbox) and
# reliefgrid-node-auth (node: storage /internal/* routes). A sender uses each Secret's
# sender-token key. The token goes from the Secret into a 0600 file in a 0700 temp dir,
# never into a variable, a command line or evidence.
#
#   internal_auth_detect <context> <namespace>
#       INTERNAL_AUTH_ON=true|false from the storage release's computed values
#       (helm get values -a; release INTERNAL_AUTH_RELEASE, default storage): only the
#       JSON booleans internalAuth.enabled true/false; an absent or null internalAuth key
#       (a chart from before #29) is off. Returns 1 if it cannot tell: an unreadable
#       release, or a string, number, null or missing enabled, is never taken as "off".
#   internal_auth_files
#       On: writes the app and node sender tokens and a curl header file for each,
#       exports INTERNAL_AUTH_TOKEN_FILE and NODE_AUTH_SENDER_TOKEN_FILE, and sets
#       INTERNAL_AUTH_CURL / NODE_AUTH_CURL to (-H @<header file>). Off: the arrays are
#       empty and nothing else happens. Call internal_auth_cleanup from the caller's EXIT
#       trap.
#   internal_auth_setup <context> <namespace>
#       detect, then files.
#   internal_auth_cleanup
#       Removes the temp dir and unexports what internal_auth_files exported. Safe to
#       call more than once, or when nothing was set up.
#   internal_auth_patch_job [--node] < job.json > job.json
#       On: adds the app sender-token mount and INTERNAL_AUTH_TOKEN_FILE to the Job's
#       single container (and, with --node, the node sender-token and
#       NODE_AUTH_SENDER_TOKEN_FILE for direct storage reads), with fsGroup 65532 so the
#       0440 files are readable by the non-root tools image. Off: the Job unchanged.
#       Needs internal_auth_detect first.
#   internal_auth_run_overrides <container name>
#       For `kubectl run`: on, prints the arguments
#       --override-type=strategic --overrides <json> (one per line) that add the same app
#       sender-token mount and variable to the named container; off, prints nothing.

INTERNAL_AUTH_HEADER=X-ReliefGrid-Internal-Token
INTERNAL_AUTH_APP_SECRET=reliefgrid-internal-auth
INTERNAL_AUTH_NODE_SECRET=reliefgrid-node-auth
INTERNAL_AUTH_APP_MOUNT=/var/run/reliefgrid/internal-auth
INTERNAL_AUTH_NODE_MOUNT=/var/run/reliefgrid/node-auth
INTERNAL_AUTH_ON="${INTERNAL_AUTH_ON:-}"
INTERNAL_AUTH_DIR=""
INTERNAL_AUTH_CURL=()
NODE_AUTH_CURL=()
_internal_auth_exported=false

_internal_auth_err() { printf 'internal-auth: %s\n' "$*" >&2; }

internal_auth_detect() {
	local ctx="$1" ns="$2" release="${INTERNAL_AUTH_RELEASE:-storage}" values enabled
	_internal_auth_ctx="$ctx" _internal_auth_ns="$ns"
	values="$(helm --kube-context "$ctx" -n "$ns" get values "$release" -a -o json 2>/dev/null)" || {
		_internal_auth_err "cannot read the values of release $release in $ns; refusing to guess whether internal auth is on"
		return 1
	}
	# Only the JSON booleans choose a mode. An absent or null internalAuth key (a release of a
	# chart from before #29, read successfully) is off; anything else malformed fails closed.
	enabled="$(jq -r '
		if type != "object" then "error values are not a JSON object"
		elif .internalAuth == null then "off"
		elif (.internalAuth | type) != "object" then
			"error internalAuth is \(.internalAuth | tojson) (\(.internalAuth | type)), not an object"
		elif (.internalAuth | has("enabled") | not) then "error internalAuth has no enabled"
		elif .internalAuth.enabled == true then "on"
		elif .internalAuth.enabled == false then "off"
		else "error internalAuth.enabled is \(.internalAuth.enabled | tojson) (\(.internalAuth.enabled | type)), not a boolean"
		end' <<<"$values" 2>/dev/null)" || {
		_internal_auth_err "release $release: values are not JSON"
		return 1
	}
	case "$enabled" in
	on) INTERNAL_AUTH_ON=true ;;
	off) INTERNAL_AUTH_ON=false ;;
	*)
		_internal_auth_err "release $release: ${enabled#error }"
		return 1
		;;
	esac
}

# _internal_auth_valid <file>: 1 to 4096 bytes, each printable ASCII 33-126 except ","
# (the receiver's rule; a trailing newline is an invalid credential, never trimmed).
_internal_auth_valid() {
	local size rest
	size="$(wc -c <"$1")" || return 1
	[ "$size" -ge 1 ] && [ "$size" -le 4096 ] || return 1
	rest="$(LC_ALL=C tr -d '!-+\055-~' <"$1" | wc -c)" || return 1
	[ "$rest" -eq 0 ]
}

# _internal_auth_secret <secret> <file>: its sender-token key, decoded, into <file>.
_internal_auth_secret() {
	# pipefail here, whatever the caller set: a missing Secret is never an empty token.
	(
		set -o pipefail
		kubectl --context "$_internal_auth_ctx" -n "$_internal_auth_ns" get secret "$1" -o json 2>/dev/null |
			jq -er '.data["sender-token"] // empty' 2>/dev/null | base64 -d >"$2" 2>/dev/null
	) || {
		_internal_auth_err "cannot read $1/sender-token"
		return 1
	}
	_internal_auth_valid "$2" || {
		_internal_auth_err "$1/sender-token is not a valid credential"
		return 1
	}
}

internal_auth_files() {
	INTERNAL_AUTH_CURL=()
	NODE_AUTH_CURL=()
	case "$INTERNAL_AUTH_ON" in
	false) return 0 ;;
	true) ;;
	*)
		_internal_auth_err "internal_auth_files before internal_auth_detect"
		return 1
		;;
	esac
	local kind secret
	INTERNAL_AUTH_DIR="$(umask 077 && mktemp -d "${TMPDIR:-/tmp}/reliefgrid-internal-auth.XXXXXX")" || return 1
	chmod 700 "$INTERNAL_AUTH_DIR" || return 1
	for kind in app node; do
		secret="$INTERNAL_AUTH_APP_SECRET"
		[ "$kind" = node ] && secret="$INTERNAL_AUTH_NODE_SECRET"
		(umask 077 && _internal_auth_secret "$secret" "$INTERNAL_AUTH_DIR/$kind-sender-token") || {
			internal_auth_cleanup
			return 1
		}
		(umask 077 && { printf '%s: ' "$INTERNAL_AUTH_HEADER"; cat "$INTERNAL_AUTH_DIR/$kind-sender-token"; printf '\n'; } >"$INTERNAL_AUTH_DIR/$kind-header") || {
			internal_auth_cleanup
			return 1
		}
	done
	export INTERNAL_AUTH_TOKEN_FILE="$INTERNAL_AUTH_DIR/app-sender-token"
	export NODE_AUTH_SENDER_TOKEN_FILE="$INTERNAL_AUTH_DIR/node-sender-token"
	_internal_auth_exported=true
	INTERNAL_AUTH_CURL=(-H "@$INTERNAL_AUTH_DIR/app-header")
	NODE_AUTH_CURL=(-H "@$INTERNAL_AUTH_DIR/node-header")
}

internal_auth_setup() {
	internal_auth_detect "$1" "$2" && internal_auth_files
}

internal_auth_cleanup() {
	if [ -n "$INTERNAL_AUTH_DIR" ]; then
		rm -rf -- "$INTERNAL_AUTH_DIR"
		INTERNAL_AUTH_DIR=""
	fi
	if [ "$_internal_auth_exported" = true ]; then
		unset INTERNAL_AUTH_TOKEN_FILE NODE_AUTH_SENDER_TOKEN_FILE
		_internal_auth_exported=false
	fi
	INTERNAL_AUTH_CURL=()
	NODE_AUTH_CURL=()
}

internal_auth_patch_job() {
	local node=false
	[ "${1:-}" = --node ] && node=true
	case "$INTERNAL_AUTH_ON" in
	false)
		cat
		return
		;;
	true) ;;
	*)
		_internal_auth_err "internal_auth_patch_job before internal_auth_detect"
		return 1
		;;
	esac
	jq -e --argjson node "$node" \
		--arg app "$INTERNAL_AUTH_APP_SECRET" --arg nodeSecret "$INTERNAL_AUTH_NODE_SECRET" \
		--arg appMount "$INTERNAL_AUTH_APP_MOUNT" --arg nodeMount "$INTERNAL_AUTH_NODE_MOUNT" '
		def mount($name; $secret; $path; $var):
			.spec.template.spec.volumes += [{name: $name, secret: {secretName: $secret, defaultMode: 288,
				items: [{key: "sender-token", path: "sender-token"}]}}]
			| .spec.template.spec.containers[0].volumeMounts += [{name: $name, mountPath: $path, readOnly: true}]
			| .spec.template.spec.containers[0].env += [{name: $var, value: ($path + "/sender-token")}];
		if (.spec.template.spec.containers | length) != 1 then error("internal-auth: want exactly one container")
		elif ((.spec.template.spec.securityContext.fsGroup // 65532) != 65532) then error("internal-auth: conflicting fsGroup")
		else
			.spec.template.spec.securityContext.fsGroup = 65532
			| mount("internal-auth"; $app; $appMount; "INTERNAL_AUTH_TOKEN_FILE")
			| if $node then mount("node-auth"; $nodeSecret; $nodeMount; "NODE_AUTH_SENDER_TOKEN_FILE") else . end
		end'
}

internal_auth_run_overrides() {
	case "$INTERNAL_AUTH_ON" in
	false) return 0 ;;
	true) ;;
	*)
		_internal_auth_err "internal_auth_run_overrides before internal_auth_detect"
		return 1
		;;
	esac
	printf '%s\n' --override-type=strategic --overrides
	jq -cn --arg name "$1" --arg secret "$INTERNAL_AUTH_APP_SECRET" --arg mount "$INTERNAL_AUTH_APP_MOUNT" '{
		apiVersion: "v1", spec: {securityContext: {fsGroup: 65532},
			volumes: [{name: "internal-auth", secret: {secretName: $secret, defaultMode: 288,
				items: [{key: "sender-token", path: "sender-token"}]}}],
			containers: [{name: $name,
				env: [{name: "INTERNAL_AUTH_TOKEN_FILE", value: ($mount + "/sender-token")}],
				volumeMounts: [{name: "internal-auth", mountPath: $mount, readOnly: true}]}]}}'
}
