#!/usr/bin/env python3
"""Two-lockfile RustSec evaluator. Exit 0=PASS, 1=FAIL, 2=HOLD.

No builds, lock generation, native ignores, or implicit database fallback.
Run directories are exclusive evidence; never reuse one after an uncertain run.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tarfile
import tomllib
import uuid

LOCKS = ('Cargo.lock', 'nilket/Cargo.lock')
HERE = Path(__file__).resolve().parent
UTC = dt.timezone.utc


class Hold(Exception):
    pass


def require(ok, message):
    if not ok:
        raise Hold(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def timestamp(value):
    require(isinstance(value, (str, dt.datetime)), 'timestamp must be UTC')
    if isinstance(value, str):
        value = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(value.tzinfo is not None and value.utcoffset() == dt.timedelta(0), 'timestamp must be UTC')
    return value


def save(path, value):
    """Atomic durable receipt. A pre-existing run itself is never overwritten."""
    temp = path.with_name(path.name + '.tmp-' + uuid.uuid4().hex)
    with temp.open('x') as f:
        json.dump(value, f, indent=2, default=str)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)
    fd = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def command(argv, cwd=None, timeout=120, env=None):
    # Bound the entire subprocess group, including a stuck fetch child.
    p = subprocess.Popen(argv, cwd=cwd, env=env if env is not None else {'PATH': os.defpath}, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, start_new_session=True)
    try:
        out, err = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(p.pid, signal.SIGKILL)
        out, err = p.communicate()
        raise Hold(f'command timed out: {argv[0]}')
    return p.returncode, out, err


def checked(argv, **kwargs):
    rc, out, err = command(argv, **kwargs)
    require(rc == 0, f'command failed ({rc}): {argv[0]}: {err.decode(errors="replace")[:1500]}')
    return out


def git(repo, *args):
    return checked(['git', '-C', str(repo), *args])


def allowlist(data, when):
    obj = tomllib.loads(data.decode())
    require(set(obj) == {'entries'} and isinstance(obj['entries'], list), 'unknown allowlist schema')
    result = {}
    fields = {'id', 'crate', 'reason', 'reviewer', 'approved_at', 'expires_at'}
    for e in obj['entries']:
        require(isinstance(e, dict) and set(e) == fields, 'allowlist fields missing/unknown')
        require(all(isinstance(e[k], str) and e[k].strip() for k in fields - {'approved_at', 'expires_at'}), 'empty allowlist field')
        require(re.fullmatch(r'RUSTSEC-\d{4}-\d{4}', e['id']) is not None, 'invalid advisory id')
        require(re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_-]*', e['crate']) is not None, 'invalid crate')
        key = (e['id'], e['crate'])
        require(key not in result, 'duplicate allowlist key')
        approved, expires = timestamp(e['approved_at']), timestamp(e['expires_at'])
        require(approved <= when, 'future approval')
        require(dt.timedelta(0) < expires - approved <= dt.timedelta(days=30), 'invalid approval span')
        result[key] = {**e, 'valid': approved <= when < expires}
    return result


def parse_report(raw, rc, db_commit, lock_bytes):
    """Validate pinned 0.22.2 schema and unfiltered report settings."""
    require(rc in (0, 1), f'audit execution error: {rc}')
    obj = json.loads(raw)
    require(isinstance(obj, dict) and set(obj) == {'database', 'lockfile', 'settings', 'vulnerabilities', 'warnings'}, 'unknown report schema')
    db, settings, vulns = obj['database'], obj['settings'], obj['vulnerabilities']
    require(set(db) == {'advisory-count', 'last-commit', 'last-updated'}, 'database schema')
    require(type(db['advisory-count']) is int and db['advisory-count'] > 0 and db['last-commit'] in (None, db_commit), 'database identity/empty')
    expected = {'target_arch': [], 'target_os': [], 'severity': None, 'ignore': [],
                'informational_warnings': ['unmaintained', 'unsound', 'notice']}
    require(settings == expected, 'filtered/unknown report settings')
    packages = tomllib.loads(lock_bytes.decode())['package']
    require(obj['lockfile'] == {'dependency-count': len(packages)}, 'lock coverage mismatch')
    require(set(vulns) == {'found', 'count', 'list'}, 'vulnerability schema')
    require(isinstance(vulns['list'], list) and type(vulns['count']) is int and type(vulns['found']) is bool, 'vulnerability types')
    require(vulns['count'] == len(vulns['list']) and vulns['found'] == bool(vulns['list']), 'vulnerability count mismatch')
    require(rc == int(bool(vulns['list'])), 'report/exit mismatch')
    require(isinstance(obj['warnings'], dict), 'warnings schema')
    require(set(obj['warnings']) <= {'unmaintained', 'unsound', 'notice'}, 'unknown warning kind')
    findings = []
    for v in vulns['list']:
        require(isinstance(v, dict) and set(v) == {'advisory', 'versions', 'affected', 'package'}, 'finding schema')
        a, p = v['advisory'], v['package']
        require(re.fullmatch(r'RUSTSEC-\d{4}-\d{4}', a['id']) is not None, 'finding id')
        require(a['informational'] is None and a['withdrawn'] is None, 'non-active vulnerability')
        require(a['package'] == p['name'] and any(all(q.get(k) == p.get(k) for k in ('name', 'version', 'source', 'checksum')) for q in packages), 'finding/package mismatch')
        findings.append({'id': a['id'], 'crate': p['name'], 'version': p['version'],
                         'source': p.get('source'), 'checksum': p.get('checksum')})
    return findings, obj['warnings']


def evaluate(args, receipt):
    repo, run, tool = Path(args.repo).resolve(), Path(args.run).resolve(), Path(args.tool).resolve()
    require(repo != run and repo not in run.parents, 'run directory must be outside source checkout')
    home, cargo_home, tmp = run / 'home', run / 'cargo-home', run / 'tmp'
    for directory in (home, cargo_home, tmp):
        directory.mkdir(mode=0o700)
    clean_env = {'PATH': os.defpath, 'HOME': str(home), 'CARGO_HOME': str(cargo_home), 'TMPDIR': str(tmp)}
    receipt['environment'] = clean_env
    policy_bytes, config = (HERE / 'policy.json').read_bytes(), (HERE / 'audit.toml').read_bytes()
    policy = json.loads(policy_bytes)
    require(policy['schema'] == 1 and tuple(policy['lockfiles']) == LOCKS, 'policy lock coverage')
    require(policy['version'] == '0.22.2', 'unreviewed tool version')
    require(sha(tool.read_bytes()) == policy['binary_sha256'], 'tool sha mismatch')
    require(sha(config) == policy['config_sha256'], 'config sha mismatch')
    require(checked([str(tool), '--version'], env=clean_env).decode().strip() == 'cargo-audit 0.22.2', 'tool version mismatch')
    commit = git(repo, 'rev-parse', '--verify', args.commit + '^{commit}').decode().strip()
    require(re.fullmatch(r'[0-9a-f]{40}', commit) is not None, 'invalid commit')
    when = timestamp(args.at) if args.at else dt.datetime.now(UTC)
    require(args.mode == 'replay' or args.at is None, 'live evaluation cannot override clock')
    receipt.update(source_tree=git(repo, 'rev-parse', commit + '^{tree}').decode().strip(), commit=commit, evaluation_utc=when.isoformat(), policy_sha256=sha(policy_bytes),
                   script_sha256=sha(Path(__file__).read_bytes()), tool=policy, mode=args.mode,
                   yanked='NOT CHECKED', locks={}, findings=[], warnings={})
    # Policy/allowlist is Git-pinned, not ambient working-tree configuration.
    rc, a, _ = command(['git', '-C', str(repo), 'show', commit + ':ci/advisory-allowlist.toml'])
    if rc != 0:
        require(args.mode == 'replay' and args.historical_empty_allowlist,
                'allowlist missing at source commit')
        a = b'entries = []\n'
        receipt['historical_empty_allowlist'] = True
    entries = allowlist(a, when)
    receipt['allowlist_sha256'] = sha(a)
    (run / 'allowlist.toml').write_bytes(a)
    originals = {}
    for lock in LOCKS:
        data = git(repo, 'show', commit + ':' + lock)
        path = repo / lock
        # The checkout is the candidate, not a silently substituted clean copy.
        require(path.is_file() and path.read_bytes() == data, 'lock missing/changed: ' + lock)
        require(isinstance(tomllib.loads(data.decode()).get('package'), list), 'lock schema')
        originals[lock] = data
        dest = run / 'locks' / lock
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        receipt['locks'][lock] = sha(data)
    (run / '.cargo').mkdir()
    (run / '.cargo/audit.toml').write_bytes(config)
    db = run / 'advisory-db'
    if args.mode == 'live':
        # New owned clone and explicit fetch: never fall back to a shared cache.
        checked(['git', 'init', '--quiet', str(db)], env=clean_env)
        checked(['git', '-C', str(db), 'remote', 'add', 'origin', policy['database_url']], env=clean_env)
        checked(['git', '-C', str(db), 'fetch', '--quiet', '--depth=1', 'origin', 'HEAD'], env=clean_env)
        checked(['git', '-C', str(db), 'checkout', '--quiet', '--detach', 'FETCH_HEAD'], env=clean_env)
        receipt['fetch'] = {'success': True, 'origin_url': policy['database_url'],
                            'fetched_at': dt.datetime.now(UTC).isoformat()}
    else:
        require(args.database is not None, 'replay database required')
        source = Path(args.database).resolve()
        require(not git(source, 'status', '--porcelain=v1', '--untracked-files=all'), 'dirty replay database')
        # Independent checkout preserves exact snapshot without sharing mutable refs.
        checked(['git', 'clone', '--quiet', '--no-hardlinks', '--no-checkout', str(source), str(db)])
        wanted = git(source, 'rev-parse', 'HEAD').decode().strip()
        checked(['git', '-C', str(db), 'checkout', '--quiet', '--detach', wanted])
    db_commit = git(db, 'rev-parse', 'HEAD').decode().strip()
    require(not git(db, 'status', '--porcelain=v1', '--untracked-files=all'), 'dirty database')
    require(any((db / 'crates').glob('*/RUSTSEC-*.md')), 'empty/malformed database')
    receipt['database_commit'] = db_commit
    # Archive includes Git identity so the same d can be replayed without network.
    with tarfile.open(run / 'advisory-db.tar.gz', 'w:gz') as archive:
        archive.add(db, arcname='advisory-db')
    receipt['database_archive_sha256'] = sha((run / 'advisory-db.tar.gz').read_bytes())
    failed = False
    errors = []
    for index, lock in enumerate(LOCKS):
        argv = [str(tool), 'audit', '--file', str(run / 'locks' / lock), '--json',
                '--no-fetch', '--db', str(db), '--no-yanked']
        if args.mode == 'replay':
            argv.append('--stale')
        try:
            rc, raw, err = command(argv, cwd=run, env=clean_env)
            (run / f'lock-{index}.raw.json').write_bytes(raw)
            (run / f'lock-{index}.stderr').write_bytes(err)
            receipt.setdefault('reports', {})[lock] = {'argv': argv, 'returncode': rc, 'sha256': sha(raw)}
            require((run / 'locks' / lock).read_bytes() == originals[lock] and (repo / lock).read_bytes() == originals[lock], 'lock mutated: ' + lock)
            fs, warnings = parse_report(raw, rc, db_commit, originals[lock])
            receipt['warnings'][lock] = warnings
            for f in fs:
                e = entries.get((f['id'], f['crate']))
                authorized = e is not None and e['valid']
                receipt['findings'].append({**f, 'lockfile': lock,
                    'classification': 'authorized' if authorized else 'active', 'entry': e})
                failed |= not authorized
        except (Hold, ValueError, KeyError, TypeError, OSError) as e:
            errors.append(f'{lock}: {e}')
    for lock, data in originals.items():
        require((repo / lock).read_bytes() == data, 'source lock changed: ' + lock)
    require(not git(db, 'status', '--porcelain=v1', '--untracked-files=all') and git(db, 'rev-parse', 'HEAD').decode().strip() == db_commit, 'database changed during evaluation')
    receipt['unused_expired_entries'] = [e for key, e in entries.items() if not e['valid'] and not any((f['id'], f['crate']) == key for f in receipt['findings'])]
    require(not errors, '; '.join(errors))
    return 'FAIL' if failed else 'PASS'


def watch_age(path):
    """Advisory warning only; never infer a successful scan from missing state."""
    record = {'path': str(path), 'status': 'UNKNOWN', 'last_success': None, 'age_seconds': None}
    try:
        state = json.loads(Path(path).read_text())
        gate_time = dt.datetime.now(UTC)
        last = state['last_success']
        if last is not None:
            age = (gate_time - timestamp(last)).total_seconds()
            record.update(last_success=last, age_seconds=age,
                          status='FRESH' if 0 <= age <= 26 * 3600 else 'STALE_OR_CLOCK_SKEW')
    except (OSError, ValueError, KeyError, TypeError):
        pass
    if record['status'] != 'FRESH':
        record['warning'] = 'G2 has no recent confirmed successful scan; timer/user-manager availability is limited'
    return record


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', required=True)
    p.add_argument('--commit', required=True)
    p.add_argument('--run', required=True)
    p.add_argument('--tool', required=True)
    p.add_argument('--mode', choices=('live', 'replay'), required=True)
    p.add_argument('--database')
    p.add_argument('--at')
    p.add_argument('--watch-state', default=str(Path.home() / '.local/state/nilket-advisory-watch/state.json'))
    p.add_argument('--historical-empty-allowlist', action='store_true', help='replay only: explicit empty policy for pre-gate history')
    args = p.parse_args(argv)
    run = Path(args.run).resolve()
    # Exclusive before any I/O: an uncertain submission is inspected, not rerun.
    try:
        run.mkdir(mode=0o700, parents=False, exist_ok=False)
    except OSError as e:
        print(json.dumps({'verdict': 'HOLD', 'error': str(e)}))
        return 2
    receipt = {'schema': 1, 'started_at': dt.datetime.now(UTC).isoformat(), 'verdict': 'HOLD',
               'g2_watch': watch_age(args.watch_state)}
    save(run / 'started.json', receipt)
    try:
        require(args.mode != 'live' or not args.historical_empty_allowlist, 'historical exception is replay-only')
        receipt['verdict'] = evaluate(args, receipt)
    except Exception as e:
        # Any unexpected schema/process failure is HOLD, never a clean report.
        receipt['error'] = f'{type(e).__name__}: {e}'
    receipt['completed_at'] = dt.datetime.now(UTC).isoformat()
    save(run / 'receipt.json', receipt)
    print(json.dumps({'verdict': receipt['verdict'], 'receipt': str(run / 'receipt.json'), 'error': receipt.get('error')}))
    return {'PASS': 0, 'FAIL': 1, 'HOLD': 2}[receipt['verdict']]


if __name__ == '__main__':
    raise SystemExit(main())
