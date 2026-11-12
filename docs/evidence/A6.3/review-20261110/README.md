# Clean-machine README review corrections

Review of 6a40f47 identified missing auxiliary prerequisites/network preparation
and incomplete submission wording. The README now lists jq, curl, GNU timeout,
a C compiler for race tests, Bash/make/Python3 and the pinned CONTRIBUTING tools
before any destructive reset command. It distinguishes online uncached tool,
module/image/Traefik preparation from a separately prepared offline kit.

Source comparison used scripts/demos/reset.sh (jq nineUP check), Makefile's race
tests and scripts/app-chart-deps.sh (pinned Traefik chart fetch). Submission now
explicitly requires a public GitHubURL, mandatory local-kind live demonstration,
deployment-from-scratch backup recording with playback verification, and records
report length/format as pending professor confirmation.

No clean-machine deployment, demo, recording or submission was executed or
claimed. This documentation correction preserves the earlier combined14-PR
compatibility evidence and its historical limitations. Fenced shell examples
were syntax checked and local links resolve; no prose-mirroring test was added.
