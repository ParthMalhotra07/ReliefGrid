# A4.4 public contract proposal

Base main b454dc0. Proposes opt-in If-Match, strong ETag response headers and
opaque list continuations without changing unconditional clients or domain/FSM
rules. Both owners must approve before the implementation merges.

Red test: TestPublicRefinementsContract reported missing If-Match/412/ETag and
cursor/nextCursor (red.txt). After the schema change and regeneration:
make check PASS; focused contract race/count3 PASS (1.399s); OpenAPI validator
reports `contracts/public.yaml: OK`; gen-check PASS against the staged intended
generated output (the first run correctly detected the un-staged intentional
generated change). Logs are retained here. No internal contract changed.

PRE-PR REVIEW: READY for owner review. Headers and pagination remain opt-in;
all existing response/input contracts still validate. No FSM/claim/ack behavior
changes, no S edits, no local-only files or credentials. Generated types are
included. Runtime ETag/CAS and exact cursor-walk tests belong to the separate
implementation PR; this contract PR does not claim those behaviors already ship.
