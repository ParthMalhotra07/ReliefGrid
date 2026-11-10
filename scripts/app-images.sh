#!/usr/bin/env bash
# Build and inspect application images without changing the cluster or its PVCs.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
tag="${APP_IMAGE_TAG:-a15-$(date -u +%Y%m%d%H%M%S)}"
[[ "$tag" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]]
evidence="$(python3 scripts/app_evidence.py)"
printf 'application image evidence: %s\n' "$evidence"
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
              entrypoint=config['Entrypoint'], architecture=data['Architecture'])
pathlib.Path(evidence, 'image-' + component + '.json').write_text(json.dumps(result, indent=2) + '\n')
print('verified image:', image, data['Id'], 'user=65532:65532')
PY
done
printf 'APP_IMAGE_TAG=%s\n' "$tag" > "$evidence/image-tag.txt"
printf 'A1.5 APPLICATION IMAGES: PASS\n'
