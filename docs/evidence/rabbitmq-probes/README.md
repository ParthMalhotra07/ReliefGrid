# RabbitMQ probes: idle cost before and after

Kind (`make demo-reset`), the broker idle (no load), 120 s per window after 60 s
settling. CPU and CFS throttling come from the broker pod's cgroup on its kind
node (`measure.sh`, one JSON line each in `idle-cpu.jsonl`). Host: Intel Core
Ultra 7 258V, 8 logical CPUs, 30 GiB; kind v0.33.0, `kindest/node:v1.37.0`;
`rabbitmq:4.3.7-management`.

| Probes | CPU limit | Idle CPU | Throttled CFS periods | Probe failures |
| --- | --- | --- | --- | --- |
| before: `rabbitmq-diagnostics` exec (startup, readiness 5 s, liveness 10 s) | none (chart default) | 648m | n/a | 0 |
| before | 750m (S5.2 load budget) | 456m | 71.7% | 0 in this window; 4 liveness timeouts earlier on an idle cluster |
| after: TCP startup and readiness on `amqp`, HTTP liveness on `management` | 750m | 5m | 0% | 0 |
| after | none (chart default) | 6m | n/a | 0 |

Each `rabbitmq-diagnostics` call boots a full Erlang CLI VM. Under the 750m limit
one call cost about 1.45 CPU-seconds and took about 5 s of wall time, equal to
the probe timeout. Inside the broker's own VM the schedulers were about 99.4%
asleep (`rabbitmq-diagnostics runtime_thread_stats`).

Hung node (the broker's `beam.smp` frozen with SIGSTOP from the kind node):

```
running:              tcp 5672: connected   http 15672: 200
VM frozen (state T):  tcp 5672: connected   http 15672: 000 (timed out)
resumed:              tcp 5672: connected   http 15672: 200
```

The kernel completes the handshake on a listening socket while the VM is
frozen, so a TCP liveness check never restarts a hung broker. With the HTTP
liveness probe (15 s period, 10 s timeout, 4 failures), a frozen broker was
restarted 99 s after the freeze: `Liveness probe failed: ... context deadline
exceeded` x4, then `Container rabbitmq failed liveness probe, will be restarted`.

`demo-5.txt`: demo 5 on a clean deployment with the new probes, PASS (500
creates, 50 reciprocal matches, 450 waiting, broker queues drained, invcheck
clean).

Not changed here: on a fresh install, the postStart hook (`rabbitmqctl wait`,
`import_definitions`) failed once and the container restarted. That happened
before and after this change.
