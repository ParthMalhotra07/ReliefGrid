# PR139 host timeout and reproduction follow-up

The six first lifecycle regressions failed against the original attached
`docker run` path (timeout-red.txt). The implementation now creates a uniquely
named/labelled runner first, records its exact ID, then starts attached. Host
cleanup stops that exact owned ID, waits, confirms State.Running=false and only
then removes it. Original timeout/nonzero errors remain failures; cleanup errors
and an unconfirmed/nonzero runner exit cannot become acceptance. An additional
red control covers a nonzero daemon-side exit after a successful attach client.

Eight permanent lifecycle cases cover timeout, normal/nonzero completion,
stop failure, still-running state, unparseable status and failed creation. Three
additional tests cover future named-node evidence and rejection of incomplete or
duplicate node inventories. The full offline suite has21 cases; named probes in
its output are mocked fixtures, not fresh cluster observations.
Preparation/offline-host prerequisites and the named-branch/controller/archive
transport requirements are documented in docs/operations/offline-kit.md.

## Historical isolation evidence qualification

Original `nov7-offline-forward-install.log` and
`nov7-offline-success-summary.log` were recovered from the local session logs.
Their unchanged relevant outputs are retained here. They contain five generic
external-HTTPS-blocked PASS lines, not a command/result mapping for each named
node. The accepted runner-network.txt separately retains the runner probe and
five Ready node names. The original verify_node_isolation loop ran `docker exec
<node> bash -ec` with a curl --noproxy '*' request to https://1.1.1.1/ bounded by
2-second connect and 4-second total timeouts, but it did not print each node name.

No original named per-node probe log was found in the artifact evidence or the
session logs. Names have not been retrospectively assigned to generic lines,
and no fresh probes have been presented as historical evidence. The retained
9/9 reports continue to support the recorded artifact only. This follow-up
does not claim a new offline nine-demo run or acceptance of current main.

## Current lightweight physical host-timeout control

Docker Desktop was started by the coordinator after the initial WSL Docker
integration was unavailable. `live-host-timeout.txt` records actual Docker
create/start/stop/wait/inspect/remove calls for one uniquely named disposable
runner using the cached historical runner image and a trapped shell command.
The container had network=none, no bind mounts and no Docker socket; kit
verification and kind lookup were stubbed so no cluster or demos were touched.
The attached host client timed out after1s, host cleanup confirmed exit and
removed the exact owned ID, and a subsequent inspect confirmed absence. This
proves the timeout lifecycle control, not a new nine-demo acceptance run.
Docker CLI/server were29.7.2; Python was3.14.4.
