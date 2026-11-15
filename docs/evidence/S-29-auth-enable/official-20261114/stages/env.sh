export REPO=~/code/ReliefGrid-auth-official
export R=~/.cache/reliefgrid/auth-official-20261114
export CLUSTER=reliefgrid NAMESPACE=reliefgrid KUBE_CONTEXT=kind-reliefgrid
export APP_RELEASE=reliefgrid APP_WITH_STORAGE=false APP_BUILD_PROVENANCE=0
export STORAGE_IMAGE=reliefgrid/storagenode:dev COORDINATOR_IMAGE=reliefgrid/coordinator:dev TOOLS_IMAGE=reliefgrid/tools:dev
export INTERNAL_AUTH=true RESET_CONFIRM=reliefgrid
export DEBUG_IMAGE=gcr.io/distroless/static-debian12@sha256:d5563cc7f2f44313f332e91138cc8c6a158899afeeeab2fce3b0f9ccdb3cf9ee
export SEED=20261114
unset GITHUB_ACTIONS ONLY SKIP_INSTALL SEEDS DEMOS_DIR DEMO_RUN_ID APP_IMAGE_TAG A15_EVIDENCE_DIR
unset MQ_AMQP_URL MQ_MANAGEMENT_URL DEMO_AMQP_ENDPOINT INTERNAL_AUTH_TOKEN_FILE NODE_AUTH_SENDER_TOKEN_FILE
K="kubectl --context kind-reliefgrid -n reliefgrid"
