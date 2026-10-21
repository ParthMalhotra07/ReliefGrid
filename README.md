# ReliefGrid

Distributed emergency resource matching: three Raft-replicated shards with a deterministic SQLite
state machine, a coordinator that owns the hash ring and leader discovery, and RabbitMQ-driven
matching workers, deployed on Kubernetes.

Getting started: install the pinned tools (see [CONTRIBUTING.md](CONTRIBUTING.md#tools)), then run
`make doctor` and `make check`.
