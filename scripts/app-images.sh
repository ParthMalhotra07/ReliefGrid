#!/usr/bin/env bash
# Build and inspect application images without changing the cluster or its PVCs.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
tag="${APP_IMAGE_TAG:-a15-$(date -u +%Y%m%d%H%M%S)}"
[[ "$tag" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]]
evidence="$(python3 scripts/app_evidence.py)"
printf 'application image evidence: %s\n' "$evidence"
case "${APP_BUILD_PROVENANCE:-0}" in
  1)
    python3 scripts/build_provenance.py build --source "$ROOT" --output "$evidence/provenance" --tag "$tag"
    for component in api matcher ui; do cp "$evidence/provenance/image-$component.json" "$evidence/image-$component.json"; done
    printf 'APP_IMAGE_TAG=%s\n' "$tag" > "$evidence/image-tag.txt"
    printf 'A1.5 APPLICATION IMAGES: PASS (strict source snapshot)\n'
    exit 0 ;;
  0) ;;
  *) echo 'APP_BUILD_PROVENANCE must be 0 or 1' >&2; exit 1 ;;
esac
record="$(mktemp)"
trap 'rm -f "$record"' EXIT
for component in api matcher ui; do
  dockerfile="cmd/$component/Dockerfile"
  if [ "$component" = ui ]; then dockerfile=ui/Dockerfile; fi
  image="reliefgrid/$component:$tag"
  docker build --progress=plain --provenance=false -f "$dockerfile" -t "$image" .
  docker image inspect "$image" --format '{{json .}}' > "$record"
  python3 - "$record" "$component" "$image" "$evidence" <<'PY'
import json
import pathlib
import sys

source, component, image, evidence = sys.argv[1:]
data = json.loads(pathlib.Path(source).read_text())
config = data['Config']
assert config['User'] == '65532:65532', (component, config['User'])
assert config['Entrypoint'] == ['/' + component], (component, config['Entrypoint'])
result = dict(image=image, imageId=data['Id'], user=config['User'],
              entrypoint=config['Entrypoint'], architecture=data['Architecture'],
              sourceBinding='NOT_ESTABLISHED: development build from working checkout')
pathlib.Path(evidence, 'image-' + component + '.json').write_text(json.dumps(result, indent=2) + '\n')
print('verified image:', image, data['Id'], 'user=65532:65532')
PY
done
printf 'APP_IMAGE_TAG=%s\n' "$tag" > "$evidence/image-tag.txt"
printf 'A1.5 APPLICATION IMAGES: PASS\n'
