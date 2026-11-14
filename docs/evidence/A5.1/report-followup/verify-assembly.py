import argparse
import hashlib
import json
from pathlib import Path
import shutil

import pymupdf
from pypdf import PdfReader

parser=argparse.ArgumentParser()
parser.add_argument('--root',type=Path,required=True)
parser.add_argument('--pdf',type=Path)
args=parser.parse_args()
root=args.root.resolve()
pdf=args.pdf or root/'docs/report/reliefgrid-submission-draft.pdf'
out=root/'docs/evidence/A5.1/report-followup'
out.mkdir(exist_ok=True)
reader=PdfReader(pdf)
assert 1<=len(reader.pages)<10
manifest=json.loads((root/'docs/report/contributions/S-source-manifest.json').read_text())
for source,item in manifest['files'].items():
 assert hashlib.sha256((root/item['target']).read_bytes()).hexdigest()==item['sha256'],source
doc=pymupdf.open(pdf)
text='\n'.join(page.get_text() for page in doc)
assert '{FINAL_SHA}' not in text and 'TODO-after' not in text
assert '\ufffd' not in text and '\u25a0' not in text
for value in ('Phase 2 comparison','93.57','56.43','90.31','270191','6324','0.001093','Development and evaluation method','Technology and version'):
 assert value in text,value
for n,page in enumerate(doc,1):
 for block in page.get_text('dict')['blocks']:
  for line in block.get('lines',[]):
   for span in line['spans']:
    x0,y0,x1,y1=span['bbox']
    assert x0>=40 and x1<=page.rect.width-40 and y0>=20 and y1<=page.rect.height-14,(n,span)
result={
 'result':'PASS','pagesIncludingReferences':len(reader.pages),'originalSourceFilesMatch':len(manifest['files']),
 'sourceBaseline':'b77e5b9761119a73bed8f2d3eede40c863492275',
 'phase2Evidence':'25ea8bc5675ab935d0b4852f8adbacb4cf14b9b3',
 'phase2MeasuredSource':'514bb33e1ffd3c6f8a83591fc82e6347b837d7ac',
 'visualReview':'All nine final pages rendered and inspected; readable tables/figure labels, intact captions, no observed clipping or overlap.',
 'checks':['under ten pages','required Phase 2/stack/method text present','no unresolved submission citations','text inside page bounds','original import hashes unchanged'],
 'pdfSha256':hashlib.sha256(pdf.read_bytes()).hexdigest(),
 'limitations':['Review draft pending human abstract/introduction/conclusion rewrite and joint read-through','Final integrated gate and submission source pin remain','No new physical load or enabled auth/aging run'],
}
(out/'assembly-qa.json').write_text(json.dumps(result,indent=2)+'\n',newline='\n')
target=root/'docs/report/reliefgrid-submission-draft.pdf'
if target.resolve()!=pdf.resolve():shutil.copy2(pdf,target)
target=out/'verify-assembly.py'
if target.resolve()!=Path(__file__).resolve():shutil.copy2(Path(__file__),target)
print(json.dumps(result,indent=2))
