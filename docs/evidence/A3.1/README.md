# A3.1 application demos

Demos 2/3/5/8/9 exercise public API work, current reciprocal claims, pod
replacement and API scaling. Demo 5 retains its fixed 120-second 500/50 bound.

The original two clean rounds passed locally; the independent second round
caught a non-quiescent demo-8 audit with WAITING backlog. Final audits for
demos 2/3/8 now use demo 5's pause, confirmed drain, strict invcheck and restore.

Demo 8 crashes the selected API pod with a real create body in flight through
a private loopback forward. It requires a measured transport interruption,
retries the frozen full payload with the same key and checks its original
result on replay. Every failed call must have a matching successful later
attempt. Ordinary traffic continues through the Service. Graceful shutdown
is tested separately by A3.2's HTTP/broker barriers.

Retained evidence contains assertions/results and one representative round
per demo under ../A3.1-20261101-resume1/round-1/, plus compact red/green and
final logs here. Repetitive HTTP/polling/node/Deployment dumps and failed
intermediate deployment directories were removed. Host home paths are stripped.

Review-fix validation: make check, verify-A3.1-static and generation pass.
Corrected live results are added after the clean deployment run completes.
The joint all-nine gate remains pending required merges and coordination.
