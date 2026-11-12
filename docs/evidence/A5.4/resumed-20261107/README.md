# Resumed offline kit verification — INCOMPLETE

Eight static safety tests pass. Docker saves and inspects one explicit linux/amd64 platform; recursive OCI archive validation checks referenced content existence, digest and size before installation. This fixes the original missing-blob/import defect without bypassing identity checks.

Three physical attempts in this resumed batch:
1. Complete archive loaded, but pre-export index ID differed from loaded platform manifest ID. Fixed explicit-platform inspection, pinned by a failing-first regression.
2. Five Ready nodes with external HTTPS blocked, but Docker internal-network filtering dropped routed pod traffic outside its bridge subnet. Same-node coordinator access worked, cross-node access timed out. Fixed internal bridge envelope 10.240.0.0/12 with disjoint node pool10.240.0.0/24, covering fixed pod CIDR10.244.0.0/16; external egress stays denied.
3. Image import, all five external-block probes and standalone storage bootstrap/install passed. All nine storage pods Ready. App install failed pinned Traefik schema validation because generated image.digest was an empty string. Nine offline demos were NOT started.

Three-attempt cap reached; PR139 remains draft/NOT READY. Next batch must reproduce schema rejection against generated offline values, remove the inherited digest in a way the pinned chart accepts while retaining the exact exported local image identity, then prepare a new immutable kit and run all nine demos with external access blocked. Do not weaken schema validation or reuse a modified immutable artifact as acceptance evidence.

Kit revisionebeadd0, local artifact outside Git. Temporary offline clusters are removed after preserving evidence; original development cluster/PVCs retained. No binary images, credentials or kubeconfigs committed. This checkpoint is not offline acceptance or completion of A5.4.
