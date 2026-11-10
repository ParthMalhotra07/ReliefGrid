#!/usr/bin/env python3
"""Prepare immutable local artifacts online; install/run them without downloads."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[1]
# Internal bridge filtering must include routed pod addresses, not just nodes.
# Allocate node addresses outside the fixed kind pod CIDR within this envelope.
OFFLINE_SUBNET = '10.240.0.0/12'
OFFLINE_NODE_POOL = '10.240.0.0/24'


def run(*args, cwd=ROOT, timeout=600, env=None, capture=False):
    result = subprocess.run(args, cwd=cwd, timeout=timeout, check=True, text=True,
                            stdout=subprocess.PIPE if capture else None, env=env)
    return result.stdout.strip() if capture else None


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()



def verify_image_archive(path):
    """Reject incomplete Docker/OCI exports before an offline deployment."""
    with tarfile.open(path) as archive:
        members={member.name:member for member in archive.getmembers()}
        def read(name):
            member=members.get(name)
            if member is None or not member.isfile():
                raise ValueError('image archive is missing a regular file: '+name)
            return archive.extractfile(member)
        seen=set()
        def descriptor(value):
            digest=value.get('digest','')
            if not re.fullmatch(r'sha256:[0-9a-f]{64}',digest):
                raise ValueError('unsupported image descriptor digest')
            if digest in seen: return
            seen.add(digest)
            name='blobs/sha256/'+digest.split(':')[1]
            checksum=hashlib.sha256();size=0
            with read(name) as stream:
                for block in iter(lambda:stream.read(1024*1024),b''):
                    checksum.update(block);size+=len(block)
            if checksum.hexdigest()!=digest.split(':')[1] or size!=value.get('size'):
                raise ValueError('image descriptor content mismatch: '+name)
            media=value.get('mediaType','')
            if 'manifest' in media or 'index' in media:
                with read(name) as stream: document=json.load(stream)
                for child in document.get('manifests',[]): descriptor(child)
                if 'config' in document: descriptor(document['config'])
                for child in document.get('layers',[]): descriptor(child)
        found=False
        if 'index.json' in members:
            with read('index.json') as stream: index=json.load(stream)
            if not index.get('manifests'): raise ValueError('empty OCI image archive')
            for value in index['manifests']: descriptor(value)
            found=True
        if 'manifest.json' in members:
            with read('manifest.json') as stream: images=json.load(stream)
            if not images: raise ValueError('empty Docker image archive')
            for image in images:
                for name in [image['Config'],*image['Layers']]:
                    with read(name): pass
            found=True
        if not found: raise ValueError('no image manifest in archive')


def verify_files(root):
    root = root.resolve()
    manifest = json.loads((root / 'manifest.json').read_text())
    files = manifest.get('files')
    if manifest.get('version') != 1 or not isinstance(files, dict) or not files:
        raise ValueError('missing/versioned artifact inventory')
    for name, checksum in files.items():
        path = root / name
        if (Path(name).is_absolute() or '..' in Path(name).parts or path.is_symlink()
                or not path.resolve().is_relative_to(root) or not path.is_file()
                or not re.fullmatch('[0-9a-f]{64}', str(checksum))):
            raise ValueError('missing or escaping asset: ' + name)
        if digest(path) != checksum:
            raise ValueError('corrupt asset: ' + name)
    for name in ("runtime-images.tar", "host-images.tar"):
        if name in files: verify_image_archive(root/name)
    return manifest


def external_export_unavailable(error):
    return all(fragment in error for fragment in
        ('failed to get api server port', '.NetworkSettings.Ports', 'slice index out of range'))


def check_cluster_name(name):
    if not re.fullmatch(r'reliefgrid-offline(?:-[a-z0-9][a-z0-9-]{0,24})?', name):
        raise ValueError('only new reliefgrid-offline[-suffix] clusters are permitted')


def inspect(image):
    return json.loads(run('docker', 'image', 'inspect', '--platform=linux/amd64', image, capture=True))[0]


def pin(name):
    return run('make', '-s', 'print-' + name, capture=True)


def offline_app_values(roles, tag):
    app_values = {'app': {'enabled': True}, 'storage': {'enabled': False},
        'rabbitmq': {'image': roles['rabbitmq']},
        'traefik': {'image': {'registry': 'docker.io', 'repository': 'reliefgrid-offline/traefik', 'tag': tag, 'digest': None, 'pullPolicy': 'Never'},
            'ingressClass': {'name': 'traefik'}, 'service': {'spec': {'type': 'NodePort'}},
            'ports': {'web': {'nodePort': 30080}, 'websecure': {'nodePort': 30443}}}}
    for component in ('api', 'matcher', 'ui'):
        app_values['app'][component] = {'image': {'tag': tag, 'pullPolicy': 'Never'}}
    return app_values


def prepare(output):
    # Preparation downloads/builds; acceptance never calls this path offline.
    import yaml  # host preparation dependency only; runner uses stdlib
    output = output.resolve()
    if output.is_relative_to(ROOT) or output.exists():
        raise ValueError('choose a new output directory outside the source checkout')
    if run('git', 'status', '--porcelain', '--untracked-files=no', capture=True):
        raise ValueError('commit source changes before preparing a revisioned kit')
    output.mkdir(parents=True)
    for directory in ('bin', 'charts', 'values'):
        (output / directory).mkdir()
    revision = run('git', 'rev-parse', 'HEAD', capture=True)
    tag = 'offline-' + revision[:12]
    images, roles = {}, {}
    def record(role, image, source):
        data = inspect(image)
        if data['Architecture'] != 'amd64':
            raise ValueError('kit currently supports Linux/amd64 only')
        images[image] = dict(id=data['Id'], source=source)
        roles[role] = image
    # Build only; never install or reset the existing cluster during preparation.
    for component in ('api', 'matcher', 'ui', 'storagenode', 'coordinator', 'tools'):
        dockerfile = 'ui/Dockerfile' if component == 'ui' else 'cmd/' + ('invcheck' if component == 'tools' else component) + '/Dockerfile'
        image = 'reliefgrid/' + component + ':' + tag
        run('docker', 'build', '--provenance=false', '--label', 'org.opencontainers.image.revision=' + revision,
            '-f', dockerfile, '-t', image, '.', timeout=900)
        record(component, image, revision)
    run('bash', 'scripts/app-chart-deps.sh')
    app = yaml.safe_load((ROOT / 'deploy/helm/reliefgrid/values.yaml').read_text())
    render = run('helm', 'template', 'reliefgrid', 'deploy/helm/reliefgrid', capture=True)
    documents = list(yaml.safe_load_all(render))
    traefik = next(d for d in documents if d and d.get('kind') == 'Deployment' and d['metadata']['name'] == 'reliefgrid-traefik')
    external = {'rabbitmq': app['rabbitmq']['image'], 'traefik': traefik['spec']['template']['spec']['containers'][0]['image'], 'node': pin('KIND_NODE_IMAGE')}
    for component, source in external.items():
        try:
            inspect(source)
        except subprocess.CalledProcessError:
            run('docker', 'pull', source, timeout=900)
        image = 'reliefgrid-offline/' + component + ':' + tag
        run('docker', 'tag', source, image)
        record(component, image, source)
    with tempfile.TemporaryDirectory(prefix='reliefgrid-offline-build-') as temp:
        context = Path(temp)
        for tool in ('kubectl', 'helm', 'kind', 'docker'):
            executable = shutil.which(tool)
            if not executable:
                raise ValueError('missing tool: ' + tool)
            shutil.copy2(executable, context / (tool + '-real' if tool == 'docker' else tool))
        for name in ('Dockerfile', 'runner.sh', 'docker-guard.sh'):
            shutil.copy2(ROOT / 'deploy/offline' / name, context / name)
        image = 'reliefgrid-offline/runner:' + tag
        run('docker', 'build', '--provenance=false', '-t', image, str(context), timeout=900)
        record('runner', image, revision)
    run('go', 'build', '-trimpath', '-o', str(output / 'bin/drain-outbox'), './scripts/demos/drain-outbox',
        env=dict(os.environ, CGO_ENABLED='0', GOTOOLCHAIN='local'))
    for chart in ('storage', 'reliefgrid'):
        run('helm', 'package', 'deploy/helm/' + chart, '-d', str(output / 'charts'))
    app_values = offline_app_values(roles, tag)
    storage_values = {'image': {'tag': tag, 'pullPolicy': 'Never'}, 'coordinator': {'image': {'tag': tag, 'pullPolicy': 'Never'}}}
    (output / 'values/app.json').write_text(json.dumps(app_values))
    (output / 'values/storage.json').write_text(json.dumps(storage_values))
    config = yaml.safe_load((ROOT / 'deploy/kind/cluster.yaml').read_text())
    config.pop('name', None); config['networking'].pop('apiServerPort', None)
    for node in config['nodes']:
        node.pop('extraPortMappings', None)
    (output / 'kind.json').write_text(json.dumps(config))
    with tempfile.TemporaryDirectory(prefix='reliefgrid-offline-source-') as temp:
        source = Path(temp) / 'source'
        branch = run('git', 'branch', '--show-current', capture=True)
        run('git', 'clone', '--quiet', '--no-local', '--depth', '1', '--single-branch', '--branch', branch, ROOT.as_uri(), str(source))
        run('git', '-C', str(source), 'remote', 'set-url', 'origin', 'https://github.com/ParthMalhotra07/ReliefGrid.git')
        with tarfile.open(output / 'source.tar.gz', 'w:gz') as archive:
            archive.add(source, arcname='source')
    runtime = [image for image in images if image not in (roles['node'], roles['runner'])]
    run('docker', 'save', '--platform=linux/amd64', '-o', str(output / 'runtime-images.tar'), *runtime, timeout=600)
    run('docker', 'save', '--platform=linux/amd64', '-o', str(output / 'host-images.tar'), roles['node'], roles['runner'], timeout=600)
    manifest = dict(version=1, source=revision, roles=roles, images=images,
                    runtime=runtime, files={})
    manifest['files'] = {f.relative_to(output).as_posix(): digest(f) for f in sorted(output.rglob('*')) if f.is_file()}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    verify_files(output)
    print('OFFLINE KIT PREPARED: ' + str(output), flush=True)


def verify_node_isolation(cluster, evidence=None):
    nodes = run('kind', 'get', 'nodes', '--name', cluster, capture=True).splitlines()
    if len(nodes) != 5 or len(set(nodes)) != 5:
        raise ValueError('isolation requires five distinct kind nodes')
    script = """set -eu
command -v curl >/dev/null
if curl --noproxy '*' -ks --connect-timeout 2 --max-time 4 https://1.1.1.1/ -o /dev/null; then exit 1; fi
echo 'PASS: external HTTPS blocked'
"""
    probes = []
    for node in nodes:
        command = ['docker', 'exec', node, 'bash', '-ec', script]
        print('Node isolation probe: ' + json.dumps(command), flush=True)
        try:
            output = run(*command, capture=True)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as error:
            output = getattr(error, 'stdout', '') or ''
            if isinstance(output, bytes):
                output = output.decode('utf-8', errors='replace')
            probes.append({'node': node, 'command': command, 'result': 'FAIL',
                           'stdout': output, 'errorType': type(error).__name__})
            if evidence is not None:
                evidence.write_text(json.dumps(probes, indent=2) + '\n')
            raise
        probes.append({'node': node, 'command': command, 'result': 'PASS',
                       'stdout': output})
        if evidence is not None:
            evidence.write_text(json.dumps(probes, indent=2) + '\n')
        print(node + ': ' + output, flush=True)


def install(output, cluster):
    check_cluster_name(cluster)
    manifest = verify_files(output)
    if cluster in run('kind', 'get', 'clusters', capture=True).splitlines():
        raise ValueError('cluster exists; refuse to overwrite or delete it')
    if (output / 'source').exists():
        raise ValueError('source extraction already exists; refuse to overwrite it')
    for archive in ('host-images.tar', 'runtime-images.tar'):
        run('docker', 'load', '-i', str(output / archive))
    for image, metadata in manifest['images'].items():
        if inspect(image)['Id'] != metadata['id']:
            raise ValueError('loaded image identity mismatch: ' + image)
    with tarfile.open(output / 'source.tar.gz') as archive:
        archive.extractall(output, filter='data')
    network = cluster + '-net'
    run('docker', 'network', 'create', '--internal', '--subnet', OFFLINE_SUBNET, '--ip-range', OFFLINE_NODE_POOL, '--label', 'reliefgrid.io/offline-cluster=' + cluster, network)
    net = json.loads(run('docker', 'network', 'inspect', network, capture=True))[0]
    if not net['Internal']:
        raise ValueError('offline network has external connectivity')
    (output / 'network-proof.json').write_text(json.dumps({'network': network, 'internal': net['Internal']}) + '\n')
    # kind's entrypoint needs a non-loopback host.docker.internal address for
    # its embedded-DNS NAT rules. An internal network has no default route and
    # does not resolve Desktop's external host alias. This local alias supplies
    # only an address; DNS is still Docker's embedded resolver, with no egress.
    run('docker', 'run', '--detach', '--pull=never', '--network', network,
        '--network-alias', 'host.docker.internal', '--name', cluster + '-dns-anchor',
        '--label', 'reliefgrid.io/offline-cluster=' + cluster, '--entrypoint', '/bin/sleep',
        manifest['roles']['runner'], 'infinity')
    with tempfile.TemporaryDirectory(prefix='reliefgrid-offline-install-') as temp:
        external = Path(temp) / 'external-config'
        command = ['kind','create','cluster','--name',cluster,'--image',manifest['roles']['node'],
                   '--config',str(output/'kind.json'),'--wait','120s','--kubeconfig',str(external)]
        created = subprocess.run(command, text=True, capture_output=True, timeout=300,
            env=dict(os.environ, KIND_EXPERIMENTAL_DOCKER_NETWORK=network))
        print(created.stdout, end=''); print(created.stderr, end='')
        if created.returncode and not external_export_unavailable(created.stderr):
            raise RuntimeError('kind bootstrap failed; inspect the retained output')
        # --internal deliberately has no host-published API port on Desktop.
        # Recover that specific export failure by using the internal endpoint,
        # then independently require all five nodes Ready; never ignore a
        # bootstrap failure or assume that a Ready-looking log was sufficient.
        config = run('kind','get','kubeconfig','--name',cluster,'--internal',capture=True)
        kubeconfig = Path(temp)/'internal-config'; kubeconfig.write_text(config); kubeconfig.chmod(0o600)
        def inside(tool, *args, capture=False):
            return run('docker','run','--rm','--pull=never','--network',network,
                '--mount','type=bind,src='+str(output)+',dst=/kit,readonly',
                '--mount','type=bind,src='+str(kubeconfig)+',dst=/kubeconfig,readonly',
                '-e','KUBECONFIG=/kubeconfig','--entrypoint','/usr/local/bin/'+tool,
                manifest['roles']['runner'],*args,capture=capture,timeout=480)
        inside('kubectl','wait','--for=condition=Ready','nodes','--all','--timeout=120s')
        nodes = json.loads(inside('kubectl','get','nodes','-o','json',capture=True))['items']
        if len(nodes) != 5 or not all(any(c['type']=='Ready' and c['status']=='True' for c in n['status']['conditions']) for n in nodes):
            raise ValueError('isolated Kubernetes bootstrap is not fully Ready')
        print('PASS: all five nodes Ready through the private API',flush=True)
        run('kind','load','image-archive','--name',cluster,str(output/'runtime-images.tar'),timeout=900)
        verify_node_isolation(cluster, output / 'node-isolation.json')
        for release, chart, values in [('storage','storage','storage'),('reliefgrid','reliefgrid','app')]:
            packages = list((output/'charts').glob(chart+'-*.tgz'))
            if len(packages)!=1:
                raise ValueError('ambiguous packaged chart: '+chart)
            inside('helm','install',release,'/kit/charts/'+packages[0].name,'-n','reliefgrid',
                '--create-namespace','-f','/kit/values/'+values+'.json',
                '--wait','--wait-for-jobs','--timeout','6m')
        print('OFFLINE KIT INSTALLED: kind-'+cluster,flush=True)


def acceptance(output, cluster):
    check_cluster_name(cluster)
    manifest = verify_files(output)
    evidence = output / 'evidence'
    evidence.mkdir(exist_ok=True)
    config = run('kind', 'get', 'kubeconfig', '--name', cluster, '--internal', capture=True)
    with tempfile.TemporaryDirectory(prefix='reliefgrid-offline-kube-') as temp:
        kubeconfig = Path(temp) / 'config'; kubeconfig.write_text(config); kubeconfig.chmod(0o600)
        # Create separately so a timed-out attached Docker client cannot hide
        # the daemon-side runner identity. Cleanup uses only the ID returned by
        # our successful create, never a pre-existing container with a name.
        name = cluster + '-runner-' + uuid.uuid4().hex
        identifier = run('docker', 'create', '--name', name,
            '--label', 'reliefgrid.io/offline-cluster=' + cluster,
            '--pull=never', '--network=' + cluster + '-net',
            '--mount', 'type=bind,src=' + str(output) + ',dst=/kit,readonly',
            '--mount', 'type=bind,src=' + str(evidence) + ',dst=/evidence',
            '--mount', 'type=bind,src=' + str(kubeconfig) + ',dst=/kubeconfig,readonly',
            '--mount', 'type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock',
            manifest['roles']['runner'], cluster, capture=True)
        if not re.fullmatch('[0-9a-f]{64}', identifier):
            raise RuntimeError('Docker did not return a full runner container ID')
        lifecycle = {'containerName': name, 'containerId': identifier,
                     'exitConfirmed': False}
        record = evidence / ('runner-host-' + identifier[:12] + '.json')
        record.write_text(json.dumps(lifecycle, indent=2) + '\n')
        try:
            run('docker', 'start', '--attach', identifier, timeout=2400)
        finally:
            # Stopping an already-exited runner is harmless. Every command has
            # a host deadline; daemon errors or unconfirmed exit fail closed.
            run('docker', 'stop', '--time', '10', identifier, timeout=30)
            status = run('docker', 'wait', identifier, capture=True, timeout=15)
            running = run('docker', 'inspect', '--format', '{{.State.Running}}',
                          identifier, capture=True, timeout=15)
            if running != 'false' or not re.fullmatch('[0-9]+', status):
                raise RuntimeError('offline runner exit not confirmed: ' + identifier)
            lifecycle.update(exitConfirmed=True, exitCode=int(status))
            record.write_text(json.dumps(lifecycle, indent=2) + '\n')
            run('docker', 'rm', identifier, timeout=15)
        if int(status) != 0:
            raise RuntimeError('offline runner exited with status ' + status)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'verify', 'install', 'run'))
    parser.add_argument('--kit', type=Path, required=True)
    parser.add_argument('--cluster', default='reliefgrid-offline')
    args = parser.parse_args(); output = args.kit.resolve()
    if args.action == 'prepare': prepare(output)
    elif args.action == 'verify': verify_files(output); print('OFFLINE KIT INTEGRITY PASS')
    elif args.action == 'install': install(output, args.cluster)
    else: acceptance(output, args.cluster)


if __name__ == '__main__':
    main()
