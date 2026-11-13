# API and matcher sender-file startup

Before implementation, the permanent startup tests ran red: valid sender files
were ignored and invalid/missing configured files still issued anonymous ring
requests. Both production startup paths now pass WithInternalAuthFile to the
shared client. Cases cover valid/disabled/missing/empty/newline/comma credentials,
first-request identity, zero I/O on invalid configuration and error redaction.

This refresh keeps only the A caller patch on merged #163, main f72715e.
The previous stacked parent and its unrelated history are no longer part of the
PR diff. Startup regressions remain unchanged. The enabled app-chart check now
pins Secret name/key/projected filename, full env path, matching read-only mount,
file mode/group, and UI exclusion. Previously a wrong env filename survived;
after strengthening, env-path, mount-directory and projected-filename mutations
all fail actual assertions. Baseline and mutant output is in review-followup/.

No chart is enabled, no Secret is provisioned and no kind/broker/rotation run
occurred. #172 supplies separate A operational HTTP and Pod/Job support. The
complete owner invocation inventory and joint live enablement/rotation run remain
prerequisites. This is caller support, not closure of #29 or A4.4 acceptance.
