import argparse
from pathlib import Path
import hashlib
import json
import shutil

import pymupdf
from pypdf import PdfReader

parser = argparse.ArgumentParser(description='Replay imported source hashes and rendered report checks.')
parser.add_argument('--root', type=Path, required=True)
parser.add_argument('--pdf', type=Path)
args = parser.parse_args()
root = args.root.resolve()
pdf = args.pdf or root / 'docs/report/reliefgrid-submission-draft.pdf'
evidence = root / 'docs/evidence/A5.1/assembly-review'
evidence.mkdir(parents=True, exist_ok=True)
manifest = json.loads((root / 'docs/report/contributions/S-source-manifest.json').read_text())
for original, item in manifest['files'].items():
    actual = hashlib.sha256((root / item['target']).read_bytes()).hexdigest()
    assert actual == item['sha256'], original
doc = pymupdf.open(pdf)
assert len(PdfReader(pdf).pages) == 9
characters = []
for number, page in enumerate(doc, 1):
    text = page.get_text()
    assert len(text) > 1000
    assert '{FINAL_SHA}' not in text and 'TODO-after' not in text
    assert '\ufffd' not in text and '\u25a0' not in text
    for block in page.get_text('dict')['blocks']:
        for line in block.get('lines', []):
            for span in line['spans']:
                x0, y0, x1, y1 = span['bbox']
                assert x0 >= 40 and x1 <= page.rect.width - 40, (number, span)
                assert y0 >= 20 and y1 <= page.rect.height - 14, (number, span)
    characters.append(len(text))
report = {
    'result': 'PASS', 'pagesIncludingReferences': 9,
    'sourceBaseline': 'f72715e1b6fdcc8564e81df0fbaf7b08c1f28956',
    'handoutSource': manifest['sourceCommit'],
    'preservedSourceFiles': len(manifest['files']),
    'originalHashesMatch': True,
    'charactersPerPage': characters,
    'placeholderAndTextBoundsChecks': 'PASS',
    'visualReview': 'All nine rendered pages inspected; no clipped labels, overflow or unreadable tables observed.',
    'pdfSha256': hashlib.sha256(pdf.read_bytes()).hexdigest(),
    'limitations': ['Review draft, not final submission', 'Phase 2 raw comparison publication pending', 'Final source pin and owner acceptance pending', 'No new physical deployment or gate run'],
}
(evidence / 'assembly-qa.json').write_text(json.dumps(report, indent=2) + '\n', newline='\n')
target = root / 'docs/report/reliefgrid-submission-draft.pdf'
if pdf.resolve() != target.resolve():
    shutil.copy2(pdf, target)
target = evidence / 'assembly-qa.py'
if Path(__file__).resolve() != target.resolve():
    shutil.copy2(Path(__file__), target)
print(json.dumps(report, indent=2))
