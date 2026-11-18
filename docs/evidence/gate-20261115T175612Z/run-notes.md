# Run notes — gate-20261115T175612Z

Gate SHA 636b265084fc2f855a0d3b17ec8ca6634209bef2, tree fa967e0db5c92bbe26b0afdb5fb715f9cb14b491. Started 2026-11-15T17:56:13Z.
- 2026-11-15T18:25:57Z reset wrapper (app-reset-kaJZIEjW): core reset.sh 104 s, wrapper total 105 s
- 2026-11-15T18:31:51Z reset wrapper (app-reset-efrtvA1x): core reset.sh 107 s, wrapper total 108 s
- reset wrapper: /home/hrishabh-sharrma/.cache/reliefgrid/gate-20261115T175612Z/reset-scoped.sh (sha256 1637f7b43273fd4462a2548a74ad500b64e834d3b251172a8c7055f1d255290c)
- gate.txt reset   OK (105 s)             OK (108 s)
- gate.txt result: PASS (18/18 passed)
- 2026-11-15T19:17:22Z STOPPED at section 3 (metrics/dashboards, verify-A4.2-live.py --mode full): FAIL — RuntimeError: HTTP 401: GET /internal/ring at the start of the leader-fault proof (metrics_fault.run_fault), before its traffic, cordon or leader delete. Seven earlier full-mode checks passed. Coordinator log: one refusal, credential=app method=GET route=/internal/ring reason=missing. M6 study, internal-auth checks, deployed UI/forms, PS aliases, trace, CI evidence, section 4 (#157) and section 5 NOT RUN. No retry.
- 2026-11-15T19:20:56Z cluster torn down (make cluster-down) after the stop observations; this run is attempt 2 of the final gate.
