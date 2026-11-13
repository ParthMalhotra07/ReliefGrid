# Nine-page report assembly review

The assembled source integrates the A fragment and supplied S v3 text/figures.
The handout source is ca0fd99a6080d26f1d44512d605a1a46fb83b9e4; all 15 originals
are preserved with SHA-256 hashes. The working F2 corrects illegal request
edges against the committed transition table; compact print adaptations retain
the publication/claim/ack authority boundaries. The original contribution is
editorial source, including its provisional Phase 2 text, not accepted results.

Actual checks:

- assembly-qa.json: nine pages including references, all original hashes match,
  text fits page bounds and no unresolved submission citation placeholders.
- All nine pages rendered and visually inspected: no clipped labels, table
  overflow or unreadable figure text observed. Temporary page PNGs are excluded
  from the PR; the 36 KB vector PDF and editable source are retained.
- ../submission-draft/: full make check, generation drift check, 14-artifact
  report verifier (293 local links), three link regressions, exact nine-page
  editing-budget arithmetic, Phase 1 aggregate replay and M6 replay passed.

The PDF is a review draft with source baseline f72715e, not a final submission.
Phase 2 published evidence, final gate/source pin and owner acceptance remain.
No live kind/load/auth/aging run was performed for report assembly.
