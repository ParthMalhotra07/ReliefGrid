# PS alias review follow-up

Rebased onto integrated main 15ab5f7, retaining autoscaling, fixed-replica defaults
and all existing verification targets. The legacy test accepted a temporary
POST /requestz mutation (legacy-gap.txt). The permanent table now requires each
of the seven exact method/path rules exactly once, and the same mutation fails
the request-create row (red-path-mutation.txt). Every alias still requires one
API backend and one AddPrefix middleware. The original chart is unchanged.

Added requestType/resourceType versus shared type filter/index mapping, and
corrected the requested wording typos. Static Helm/API verification, make check
and generation checks passed; outputs are retained here. No new kind run was
performed and no historical run is relabelled as current deployment acceptance.
