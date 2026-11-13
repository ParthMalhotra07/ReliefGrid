# Valid older-version regression

Permanent negative cases cover both kinds: a valid v3 create receipt followed
by a valid v2 current record must retain the v3 receipt, original body/key and
block another creation. A later v4 read completes confirmation without a second
POST. Removing the BigInt minimum-version comparison fails this new test in a
disposable copy; version-mutation-red.txt retains its actual output.

Integrated main ed108ea. Frontend unit/browser checks, make check and generation
passed; actual outputs are adjacent. No contract or production behavior changed.
No live kind/demo acceptance was performed for this test-only follow-up.
