# Operational internal-call sender verification

Permanent tests initially failed: scratch/coordinator calls sent no app token,
demo clients had no internal sender, and audit Jobs had no sender mount. The
current suite covers credential validation/redaction, credentialed coordinator
versus anonymous public/telemetry traffic, redirect/foreign-route refusal,
legacy disabled mode and actual emitted demo/recovery/audit Pod manifests.
Three disposable mutations (missing HTTP header, following a redirect, missing
Pod sender-file environment) are rejected by the regression suite.

Focused tests, the existing scratch recovery/fault and demo transport tests,
full make check and make gen-check passed; raw outputs are adjacent. This is
local HTTP and manifest fixture verification. No Kubernetes/RabbitMQ or live
authentication/rotation run occurred. Runtime use still requires accepted #163,
#171 and the complete operational inventory. Auth remains disabled by default.

A follow-up compatibility regression catches coordinator origins ending in `/`:
previously concatenation produced `//internal/`, rejected by the strict sender.
The three host callers normalize that root slash before joining paths, without
following redirects or changing retry IDs/bodies. The suite now has 12 cases;
root-url-red.txt retains the pre-fix assertion failure.
