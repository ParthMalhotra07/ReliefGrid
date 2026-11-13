# Review draft assembly

submission-draft.md integrates the A fragments and supplied S v3 contribution
into nine sections. Its explicit page breaks are the review layout, not evidence
of final scientific acceptance. Build with Python and pdf-requirements.txt:

    python -m pip install -r docs/report/pdf-requirements.txt
    python scripts/build_submission_pdf.py

The script uses compact native vector adaptations of F1-F4 for readable print
labels and emits F1-print.svg through F4-print.svg from the same drawing data.
Original .mmd/SVG/PNG files remain preserved with source hashes under
contributions/original-figures/. The working F2 .mmd also corrects the supplied
illegal MATCHED/IN_PROGRESS -> RETRYING edges against the FSM transition table.

The exact supplied allocation is S5.55/A3.45 pages, still nine inclusive; the
previous 5.5/3.5 figures were rounded editing envelopes. Layout is checked on the
actual PDF, including references and captions, not inferred from word count.
No additional cover/contents/appendix is outside the count.

This remains a review draft: Phase 2 evidence must be published/reviewed before
its numbers are accepted. Supplied provisional numbers remain in the preserved
editorial contribution but not this draft's accepted table/abstract/conclusion.
Final cohort checks, final source pin and report owner review remain required.
The current draft has no {FINAL_SHA}/TODO citation placeholders; source links
use its declared baseline and historical runs retain their own identities.

The retained PDF was built with Python 3.12.10, reportlab 5.0.1. Page rendering and
text/hash checks used pypdf 6.21.2 and pymupdf 2.1; these are optional review
dependencies rather than runtime application dependencies. Recheck a rebuilt
PDF before replacing the retained artifact:

    python docs/evidence/A5.1/assembly-review/assembly-qa.py --root .

That replay checks source-file hashes, page count, text bounds and unresolved
citation placeholders; visual inspection of every rendered page remains manual.
