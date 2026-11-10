"""Choose a new run directory unless the caller explicitly selects evidence."""
import datetime
import os
from pathlib import Path
import tempfile


def evidence_dir():
    selected = os.environ.get('A15_EVIDENCE_DIR')
    if selected:
        path = Path(selected)
        path.mkdir(parents=True, exist_ok=True)
        return path
    cache = Path(os.environ.get('XDG_CACHE_HOME') or Path.home() / '.cache')
    parent = cache / 'reliefgrid' / 'evidence'
    parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    return Path(tempfile.mkdtemp(prefix='a15-' + stamp + '-', dir=parent))


if __name__ == '__main__':
    print(evidence_dir())
