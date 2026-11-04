# Restore-only cache for the seams job

Issue #102: integrating #94 and #97 added a Go job without the cache setup
required by TestCIGoCache. Main 741cb0e failed that check in Actions run
41790561681. The unmodified test is captured in red.log; focused.log records
the same test suite after applying the workflow change.

The seams job now disables setup-go's default cache and restores the newest
matching Go cache written by main. Only the check job saves it. Test assertions,
test selection, deadlines and production code are unchanged.
