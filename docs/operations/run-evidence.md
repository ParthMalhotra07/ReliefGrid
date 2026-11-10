# Application run evidence

`make app-images`, `make app-deploy` (including `make demo-reset`) and
`make e2e-smoke` create separate run folders by default:

```text
${XDG_CACHE_HOME:-$HOME/.cache}/reliefgrid/evidence/a15-<UTC>-<unique suffix>/
```

The command prints its folder before invoking external tools. Failed runs keep
their diagnostics there too. Running a deployment exports the chosen folder to
its image-build subprocess, so image inspections and deployment evidence stay
together. Separate top-level commands get separate folders by default.

Existing committed `docs/evidence/A1.5` files are historical acceptance evidence;
ordinary builds, deployments and smoke tests do not overwrite them.

For a named acceptance run, choose one directory explicitly and pass it to both
deployment and smoke commands. This also retains immutable retry input for an
intentional smoke retry with the same `E2E_RUN_ID`:

```sh
export A15_EVIDENCE_DIR="$PWD/docs/evidence/my-named-acceptance/deployment"
make app-deploy
E2E_RUN_ID=round-1 make e2e-smoke
```

CI and schedule verification already provide explicit per-run directories;
their layout is unchanged. Review selected summaries before committing evidence.
Do not add cache directories, private values, Secret manifests or kubeconfigs.
Reproducing A1.5 into its old directory requires explicitly selecting that path;
prefer a newly named run to preserve the original accepted artifacts.
