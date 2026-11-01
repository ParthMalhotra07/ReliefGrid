set -euo pipefail
export PATH="$HOME/.local/go/bin:$HOME/.local/bin:$PATH"
export PYTHONDONTWRITEBYTECODE=1
cd /home/parthmalhotra/reliefgrid-a31
export DEMO_RUN_ID=A3.1-20261101-drain-control
. scripts/demos/lib.sh
demo_begin 5 'Controlled pending-handoff regression (separate from burst fixture)'
. scripts/demos/kube.sh
require_preflight
export DEMO_COORD_URL="$(coord)" DEMO_OPERATOR_TOKEN="$OPTOK"
forward svc/reliefgrid-rabbitmq 5672
export DEMO_AMQP_ENDPOINT="amqp://127.0.0.1:$FWD_PORT"
export DEMO_OUTBOX_DRAIN="$DEMO_TMP/drain-outbox"
go build -o "$DEMO_OUTBOX_DRAIN" ./scripts/demos/drain-outbox
out="$PWD/docs/evidence/A3.1/drain-control"
mkdir -p "$out"
cp /mnt/c/Users/am/Desktop/CC/.tools/resume-nov1/drain-control.py "$out/verify.py"
cp /mnt/c/Users/am/Desktop/CC/.tools/resume-nov1/drain-control.sh "$out/verify.sh"
if run 'seed one pending event with workers stopped, drain and audit' python3 /mnt/c/Users/am/Desktop/CC/.tools/resume-nov1/drain-control.py "$out"; then
 pass 'pending event drained and invariants pass; workers restored'
else
 fail 'controlled pending handoff failed'
fi
demo_end
