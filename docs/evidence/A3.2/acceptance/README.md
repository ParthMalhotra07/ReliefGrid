# Current A3.2 demo acceptance

Run a32-acceptance-20261102 passed demo8 after demo-reset/app-deploy from a
clean local kind deployment. One actual interrupted create recovered with its
original key/payload. API and matcher replacements became Ready, successful
calls continued, and strict clean/quiescent invcheck passed after confirmed
drain. The JSON assertions, result, audit and exact combined source identities
are retained. This is one recovery acceptance run, not the joint A3.3 gate.

The local combined checkout merges current #90 production head3b7acb6 and
#84 demo head3fed0d7; provenance.json identifies its commit/file hashes. Merge
resolution preserves both Makefile verifiers and API response metadata plus
the bounded shutdown/health handling. This merge is not pushed as another PR
stack. Production #90 remains based only on #86.

The first combined default make check hit unchanged S-owned coordinator
identity_test.go:106 (leader address never tried), tracked in #91. The exact
failure is retained. GOFLAGS=-p=1 make check passed with the same tests,
deadlines and within-package concurrency. Generation passed. Standard CI on
the unchanged #90 production head also passed. No S source or assertion was
changed to obtain a green result. Main post-merge checks remain required.
