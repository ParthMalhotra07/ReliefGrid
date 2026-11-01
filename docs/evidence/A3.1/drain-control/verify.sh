set -euo pipefail
export PATH="$HOME/.local/go/bin:$HOME/.local/bin:$PATH"
export PYTHONDONTWRITEBYTECODE=1
cd "$(git rev-parse --show-toplevel)"
export DEMO_RUN_ID="${DEMO_RUN_ID:-A3.1-drain-control-$(date -u +%Y%m%dT%H%M%SZ)}"
. scripts/demos/lib.sh
demo_begin 5 'Controlled pending-handoff regression (separate from burst fixture)'
. scripts/demos/kube.sh
require_preflight
export DEMO_COORD_URL="$(coord)" DEMO_OPERATOR_TOKEN="$OPTOK"
forward svc/reliefgrid-rabbitmq 5672
export DEMO_AMQP_ENDPOINT="amqp://127.0.0.1:$FWD_PORT"
export DEMO_OUTBOX_DRAIN="$DEMO_TMP/drain-outbox"
go build -o "$DEMO_OUTBOX_DRAIN" ./scripts/demos/drain-outbox
out="$PWD/docs/evidence/A3.1/drain-control/$DEMO_RUN_ID"
mkdir -p "$out"
if run 'seed one pending event with workers stopped, drain and audit' python3 "$PWD/docs/evidence/A3.1/drain-control/verify.py" "$out"; then
 pass 'pending event drained and invariants pass; workers restored'
else
 fail 'controlled pending handoff failed'
fi
demo_end
