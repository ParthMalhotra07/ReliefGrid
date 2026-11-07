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

ROOT = Path(__file__).resolve().parents[1]


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
    return manifest


def check_cluster_name(name):
    if not re.fullmatch(r'reliefgrid-offline(?:-[a-z0-9][a-z0-9-]{0,24})?', name):
        raise ValueError('only new reliefgrid-offline[-suffix] clusters are permitted')


def inspect(image):
    return json.loads(run('docker', 'image', 'inspect', image, capture=True))[0]


def pin(name):
    return run('make', '-s', 'print-' + name, capture=True)


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
    app_values = {'app': {'enabled': True}, 'storage': {'enabled': False},
        'rabbitmq': {'image': roles['rabbitmq']},
        'traefik': {'image': {'registry': '', 'repository': 'reliefgrid-offline/traefik', 'tag': tag, 'digest': '', 'pullPolicy': 'Never'},
            'ingressClass': {'name': 'traefik'}, 'service': {'spec': {'type': 'NodePort'}},
            'ports': {'web': {'nodePort': 30080}, 'websecure': {'nodePort': 30443}}}}
    for component in ('api', 'matcher', 'ui'):
        app_values['app'][component] = {'image': {'tag': tag, 'pullPolicy': 'Never'}}
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
    run('docker', 'save', '-o', str(output / 'runtime-images.tar'), *runtime, timeout=600)
    run('docker', 'save', '-o', str(output / 'host-images.tar'), roles['node'], roles['runner'], timeout=600)
    manifest = dict(version=1, source=revision, roles=roles, images=images,
                    runtime=runtime, files={})
    manifest['files'] = {f.relative_to(output).as_posix(): digest(f) for f in sorted(output.rglob('*')) if f.is_file()}
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    verify_files(output)
    print('OFFLINE KIT PREPARED: ' + str(output), flush=True)


def block_nodes(cluster):
    nodes = run('kind', 'get', 'nodes', '--name', cluster, capture=True).splitlines()
    script = """set -eu
iptables -N RELIEFGRID_OFFLINE
for net in 127.0.0.0/8 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16; do iptables -A RELIEFGRID_OFFLINE -d "$net" -j RETURN; done
iptables -A RELIEFGRID_OFFLINE -j REJECT
iptables -I OUTPUT 1 -j RELIEFGRID_OFFLINE
iptables -I FORWARD 1 -j RELIEFGRID_OFFLINE
ip6tables -N RELIEFGRID_OFFLINE
for net in ::1/128 fc00::/7 fe80::/10; do ip6tables -A RELIEFGRID_OFFLINE -d "$net" -j RETURN; done
ip6tables -A RELIEFGRID_OFFLINE -j REJECT
ip6tables -I OUTPUT 1 -j RELIEFGRID_OFFLINE
ip6tables -I FORWARD 1 -j RELIEFGRID_OFFLINE
if curl --noproxy '*' -ks --connect-timeout 2 --max-time 4 https://1.1.1.1/ -o /dev/null; then exit 1; fi
echo 'PASS: external HTTPS blocked'
"""
    for node in nodes:
        run('docker', 'exec', node, 'bash', '-ec', script)


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
    previous = run('kubectl', 'config', 'current-context', capture=True)
    try:
        run('kind', 'create', 'cluster', '--name', cluster, '--image', manifest['roles']['node'],
            '--config', str(output / 'kind.json'), '--wait', '120s', timeout=300)
        run('kind', 'load', 'image-archive', '--name', cluster, str(output / 'runtime-images.tar'), timeout=900)
        block_nodes(cluster)
        context = 'kind-' + cluster
        for release, chart, values in [('storage','storage','storage'), ('reliefgrid','reliefgrid','app')]:
            packages = list((output / 'charts').glob(chart + '-*.tgz'))
            if len(packages) != 1:
                raise ValueError('ambiguous packaged chart: ' + chart)
            run('helm', '--kube-context', context, 'install', release, str(packages[0]),
                '-n', 'reliefgrid', '--create-namespace', '-f', str(output / ('values/' + values + '.json')),
                '--wait', '--wait-for-jobs', '--timeout', '6m', timeout=420)
        print('OFFLINE KIT INSTALLED: ' + context, flush=True)
    finally:
        run('kubectl', 'config', 'use-context', previous)


def acceptance(output, cluster):
    check_cluster_name(cluster)
    manifest = verify_files(output)
    evidence = output / 'evidence'
    evidence.mkdir(exist_ok=True)
    config = run('kind', 'get', 'kubeconfig', '--name', cluster, '--internal', capture=True)
    with tempfile.TemporaryDirectory(prefix='reliefgrid-offline-kube-') as temp:
        kubeconfig = Path(temp) / 'config'; kubeconfig.write_text(config); kubeconfig.chmod(0o600)
        run('docker', 'run', '--rm', '--pull=never', '--cap-add=NET_ADMIN', '--network=kind',
            '--mount', 'type=bind,src=' + str(output) + ',dst=/kit,readonly',
            '--mount', 'type=bind,src=' + str(evidence) + ',dst=/evidence',
            '--mount', 'type=bind,src=' + str(kubeconfig) + ',dst=/kubeconfig,readonly',
            '--mount', 'type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock',
            manifest['roles']['runner'], cluster, timeout=2400)


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
