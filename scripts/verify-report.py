#!/usr/bin/env python3
"""Check the report's source/evidence links and mandatory chapter coverage."""
from pathlib import Path
import re
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = ['docs/report/README.md', 'docs/report/architecture.md',
            'docs/report/workflow-and-messaging.md', 'docs/report/requirements-and-results.md',
            'docs/report/deployment-and-demo.md',
            'docs/adr/004-attempt-closure.md', 'docs/adr/005-outbox-handoff.md']


def verify():
    errors, links = [], 0
    for name in REQUIRED:
        source = ROOT / name
        if not source.is_file():
            errors.append('missing chapter: ' + name)
            continue
        text = source.read_text()
        for href in re.findall(r'\[[^\]]*\]\(([^)\s]+)\)', text):
            parsed = urlsplit(href)
            if parsed.scheme in ('https', 'http', 'mailto'):
                continue
            target = (source.parent / unquote(parsed.path)).resolve() if parsed.path else source
            if not target.is_relative_to(ROOT) or not target.exists():
                errors.append(name + ': broken local link ' + href)
            links += 1
    if errors:
        raise SystemExit('REPORT VERIFY FAIL\n' + '\n'.join(errors))
    print(f'REPORT VERIFY PASS: {len(REQUIRED)} chapters/ADRs, {links} local source/evidence links')


if __name__ == '__main__':
    verify()
