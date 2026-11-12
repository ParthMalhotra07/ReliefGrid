# Two keyed creation forms

Create request and register resource use existing public endpoints; matching
remains automatic. There is no contract, storage or authentication change.
The exact body/key is saved before network I/O and retained across uncertain
results and reloads. Acknowledged creates require current-record confirmation
and a successful view refresh; their confirmation retry does not repeat POST.

Ten new unit cases cover reentry, frozen retries/reload, sticky uncertainty,
known rejection editing, failed confirmation/refresh, storage failure, paginated
resource confirmation, incomplete observations, identity/version and validation.
All sixteen frontend unit tests and seventeen browser tests passed. The browser
run initially found a fetch receiver-binding defect before POST; wrapping the
native fetch in an arrow function fixed it without changing assertions. Browser
cases retain actual commit/lost-reply/retry behavior rather than optimistic UI.
The mobile screenshot was visually inspected with no horizontal clipping.

Full checks/generation outputs are retained when complete. Browser tests use the
local deterministic HTTP fixture and real Chromium, not a live kind deployment
or broker. No gate or auth enablement is claimed. Final demo acceptance still
requires the deployed cohort. See ui/README.md for session lifetime and bounded
resource-walk limits; unresolved entries are not silently discarded.
