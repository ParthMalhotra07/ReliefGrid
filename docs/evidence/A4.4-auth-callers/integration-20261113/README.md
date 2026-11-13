# Local combined-source integration

Input heads are recorded in inputs.txt: A caller support #171 at 153503b,
linearizable reads #166 at c7eb9d3, and operational callers #172 at 2927b41.
The A caller head contains auth parent #163 at 79783cd and main 7fa4a7d.

This was a disposable uncommitted merge plus the A operational patch, not an
accepted immutable deployment revision. Go source merged automatically; only
generated ui/static files conflicted with the already-merged forms. The pinned
Node 24.16.0/npm 11.13.0 build regenerated the full static set from merged source.
Two builds have identical complete file hashes (empty rebuild.txt diff), revision
28110e86fb709ae1. Seventeen unit and seventeen browser tests, formatting,
full make check and make gen-check pass. The emitted asset set is not copied into
this caller PR; #166's owner must regenerate/check it after updating that branch.

The combined check protects against individually green PRs failing together.
It is local fixture/build verification, not a physical kind/auth enablement or
rotation run, final gate, image/source binding, or acceptance of load capacity.
No container/cluster was started. Final branch heads still require CI/review.
