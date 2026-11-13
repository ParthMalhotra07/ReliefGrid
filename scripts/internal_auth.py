"""Optional app sender for operational coordinator calls, not node/operator APIs."""
import os
from pathlib import Path
import urllib.parse
import urllib.request

HEADER = 'X-ReliefGrid-Internal-Token'
MOUNT = '/var/run/reliefgrid/internal-auth'

def _token(path):
    if not path:
        return None
    try:
        raw = Path(path).read_bytes()
    except OSError:
        raise ValueError('internal auth: cannot read sender file') from None
    if not 1 <= len(raw) <= 4096 or any(b < 33 or b > 126 or b == 44 for b in raw):
        raise ValueError('internal auth: invalid sender credential')
    return raw.decode('ascii')

def _origin(url):
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ('http', 'https') or not parts.hostname or parts.username or parts.password or parts.fragment:
        raise ValueError('internal auth: invalid coordinator URL')
    return (parts.scheme, parts.hostname, parts.port or (443 if parts.scheme == 'https' else 80))

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None

class Sender:
    def __init__(self, coordinator, path=None):
        self.origin = _origin(coordinator)
        parts = urllib.parse.urlsplit(coordinator)
        if parts.path not in ('', '/') or parts.query:
            raise ValueError('internal auth: coordinator must be an origin URL')
        self._token = _token(os.environ.get('INTERNAL_AUTH_TOKEN_FILE', '') if path is None else path)
        handlers = [urllib.request.ProxyHandler({})]
        if self._token is not None:
            handlers.append(_NoRedirect())
        self._opener = urllib.request.build_opener(*handlers)

    def __repr__(self):
        return 'Sender([REDACTED])'

    def open(self, request, *, timeout):
        if not isinstance(request, urllib.request.Request):
            request = urllib.request.Request(request)
        if self._token is None:
            # Disabled auth preserves the existing anonymous HTTP transport,
            # including redirects and caller-supplied operator headers.
            return self._opener.open(request, timeout=timeout)
        parts = urllib.parse.urlsplit(request.full_url)
        if _origin(request.full_url) != self.origin or not parts.path.startswith('/internal/'):
            raise ValueError('internal auth: request outside coordinator internal routes')
        headers = {key:value for key,value in request.header_items() if key.lower() != HEADER.lower()}
        if self._token is not None:
            headers = {key:value for key,value in headers.items() if key.lower() not in ('authorization','proxy-authorization')}
            headers[HEADER] = self._token
        forwarded = urllib.request.Request(request.full_url, data=request.data, headers=headers, method=request.get_method())
        return self._opener.open(forwarded, timeout=timeout)

def mount_app_sender(pod):
    """Configure our single-container audit/recovery Pod only when opted in.

    Read the host sender file first, but never serialize it. Kubernetes mounts
    only the separately staged app sender key, not current/previous/node keys.
    """
    if _token(os.environ.get('INTERNAL_AUTH_TOKEN_FILE', '')) is None:
        return
    spec = pod['spec']
    containers = spec['containers']
    if len(containers) != 1:
        raise ValueError('internal auth: expected one operational container')
    if any(v.get('name') == 'internal-auth' for v in spec.get('volumes', [])):
        raise ValueError('internal auth: volume already configured')
    security = spec.get('securityContext', {})
    if security.get('fsGroup',65532) != 65532:
        raise ValueError('internal auth: conflicting file group')
    container = containers[0]
    if any(e.get('name') == 'INTERNAL_AUTH_TOKEN_FILE' for e in container.get('env', [])):
        raise ValueError('internal auth: sender already configured')
    if any(m.get('mountPath') == MOUNT for m in container.get('volumeMounts', [])):
        raise ValueError('internal auth: mount already configured')
    spec.setdefault('securityContext', {})['fsGroup'] = 65532
    spec.setdefault('volumes', []).append({'name':'internal-auth','secret':{
        'secretName':'reliefgrid-internal-auth','defaultMode':0o440,
        'items':[{'key':'sender-token','path':'sender-token'}]}})
    container.setdefault('env', []).append({'name':'INTERNAL_AUTH_TOKEN_FILE','value':MOUNT+'/sender-token'})
    container.setdefault('volumeMounts', []).append({'name':'internal-auth','mountPath':MOUNT,'readOnly':True})
