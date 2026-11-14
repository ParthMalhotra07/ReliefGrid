# Review follow-up: exact enabled app sender projection

The old chart suite passed when the configured sender filename was changed to
an unmounted file. That survivor is retained. The API/matcher enabled contract
now asserts Secret name, exactly one sender key/filename, 0440 file mode,
fsGroup 65532, the full INTERNAL_AUTH_TOKEN_FILE value and exactly one matching
read-only volume mount at /var/run/reliefgrid/internal-auth. UI is forbidden
from receiving app/node Secret references or the sender env/path.

With the corrected test, corrupt env filename, mount directory and projected
filename mutations each fail an assertion. Unmodified baseline passes. No chart
production values or startup invariants were weakened; the existing binary
fail-closed tests still apply. No physical mount/enablement run is claimed.
