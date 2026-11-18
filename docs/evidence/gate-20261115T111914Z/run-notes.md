# Run notes — gate-20261115T111914Z

Gate SHA c8b2b609d384d84a80e85d85aee9f6946eae9380, tree d35c3e2e57476432d2486fd51a7325e95b81c501. Started 2026-11-15T11:19:15Z.
- 2026-11-15T11:31:52Z reset wrapper (app-reset-UiR8ecBb): core reset.sh 103 s, wrapper total 104 s
- 2026-11-15T11:37:28Z reset wrapper (app-reset-2YPKoXOE): core reset.sh 96 s, wrapper total 97 s
- reset wrapper: /home/hrishabh-sharrma/.cache/reliefgrid/gate-20261115T111914Z/reset-scoped.sh (sha256 1637f7b43273fd4462a2548a74ad500b64e834d3b251172a8c7055f1d255290c)
- gate.txt reset   OK (104 s)             OK (97 s)
- gate.txt result: PASS (18/18 passed)
- 2026-11-15T11:44:13Z STOPPED at section 3 step 1 (verify-S4.2 replacement): FAILED (1 of 16 checks failed) — "FAIL  control:  reaches storage-shard-1-2:7000 and :8080 before the wipe". peer0 was empty (leader_pod found no Leader at that instant); abort, no retry. Later section 3 checks, section 4 and section 5 NOT RUN.
- 2026-11-15T12:21:44Z cluster torn down (make cluster-down) after read-only diagnosis; this run is attempt 1 of the final gate.
- 2026-11-15T12:31:00Z harness fix opened as PR #185 (s/S4.2-replace-wait).
