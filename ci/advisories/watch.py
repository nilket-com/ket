#!/usr/bin/env python3
"""Owned daily G1 scanner and durable incident outbox. Never auto-fixes/pushes.

All writers (checker, failure handler, delivery receipts) serialize on state.lock.
Only a complete PASS/FAIL scan may resolve exposure or advance last_success.
"""
import argparse
from contextlib import contextmanager
import datetime as dt
import fcntl
import json
import shutil
import os
from pathlib import Path
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gate


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def initial():
    return {'schema': 1, 'event_seq': 0, 'incidents': {}, 'health': {}, 'outbox': {},
            'last_success': None, 'last_started': None, 'last_completed': None}


@contextmanager
def transaction(directory):
    directory = Path(directory)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (directory / 'state.lock').open('a') as lock:
        deadline = time.monotonic() + 15
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise gate.Hold('state writer busy for 15 seconds')
                time.sleep(0.05)
        path = directory / 'state.json'
        state = json.loads(path.read_text()) if path.exists() else initial()
        gate.require(state['schema'] == 1, 'unknown watch state schema')
        try:
            yield state
            gate.save(path, state)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def event(state, kind, identity, transition, details, context, test=False):
    state['event_seq'] += 1
    eid = str(uuid.uuid4())
    record = {'event_uuid': eid, 'event_seq': state['event_seq'], 'kind': kind,
              'identity': identity, 'transition': transition, 'details': details,
              'observed_at': now(), 'context': context}
    prefix = 'TEST/old-snapshot ' if test else ''
    body = prefix + f'advisory-watch {kind} {transition}\n' + json.dumps(record, indent=2)
    for recipient in ('claude', 'codex'):
        # Stable per-event UUID => distinct repeated transitions, stable replays.
        key = str(uuid.uuid5(uuid.UUID(eid), recipient))
        state['outbox'][key] = {'event': record, 'recipient': recipient, 'sender': 'advisory-watch', 'key': key,
            'conversation': 'advisory-watch-TEST' if test else 'advisory-watch',
            'body': body, 'state': 'stored', 'message_id': None, 'attempts': 0}
    return record


def health(state, reason, context, test=False):
    # All acquisition/execution/coverage failures form one durable outage.
    previous = state['health'].get('scan')
    state['health']['scan'] = {'open': True,
        'reason': previous['reason'] if previous and previous['open'] else reason,
        'notes': list(previous.get('notes', [])) if previous and previous['open'] else [],
        'since': previous['since'] if previous and previous['open'] else now()}
    if previous and previous['open'] and reason != previous['reason'] and reason not in state['health']['scan']['notes']:
        state['health']['scan']['notes'].append(reason)
    if not previous or not previous['open']:
        event(state, 'health', 'scan', 'OPEN', {'reason': reason}, context, test)


def scan_result(state, receipt, context, test=False):
    """Pure transition logic; serialization and durable outbox commit are external."""
    state['last_completed'] = now()
    if receipt.get('verdict') not in ('PASS', 'FAIL'):
        health(state, receipt.get('error', 'incomplete scan'), context, test)
        return
    gate.require(set(receipt['locks']) == set(gate.LOCKS), 'incomplete lock coverage')
    gate.require(set(receipt['reports']) == set(gate.LOCKS), 'incomplete report coverage')
    if state['last_success'] is not None:
        elapsed = gate.timestamp(now()) - gate.timestamp(state['last_success'])
        if elapsed > dt.timedelta(hours=26):
            health(state, 'last successful scan older than 26h', context, test)
    if state['health'].get('scan', {}).get('open'):
        event(state, 'health', 'scan', 'RECOVERED', {}, context, test)
        state['health']['scan']['open'] = False
    current = {}
    for finding in receipt['findings']:
        gate.require(finding['lockfile'] in gate.LOCKS, 'unexpected finding lockfile')
        identity = json.dumps([finding['id'], finding['crate'], finding['lockfile']], separators=(',', ':'))
        row = current.setdefault(identity, {'versions': [], 'unpermitted': False})
        row['versions'].append({'version': finding['version'], 'source': finding['source'], 'checksum': finding['checksum']})
        row['unpermitted'] |= finding['classification'] != 'authorized'
    for row in current.values():
        row['versions'].sort(key=lambda v: (v['version'], v['source'] or '', v['checksum'] or ''))
    for identity, row in current.items():
        previous = state['incidents'].get(identity)
        generation = previous['generation'] if previous else 0
        if not previous or previous['state'] == 'RESOLVED':
            generation += 1
            transition = 'OPEN' if previous is None else 'REINTRODUCED'
        elif row['versions'] != previous['versions']:
            transition = ('AFFECTED_VERSIONS_CHANGED_AND_AUTHORIZATION_LOST'
                          if row['unpermitted'] and not previous['unpermitted'] else 'AFFECTED_VERSIONS_CHANGED')
        elif row['unpermitted'] and not previous['unpermitted']:
            transition = 'AUTHORIZATION_LOST'
        else:
            transition = None
        state['incidents'][identity] = {**row, 'state': 'OPEN', 'generation': generation,
            'first_seen': previous['first_seen'] if previous else now(), 'last_seen': now()}
        if transition:
            event(state, 'exposure', identity, transition, state['incidents'][identity], context, test)
    for identity, previous in state['incidents'].items():
        if identity not in current and previous['state'] == 'OPEN':
            previous['state'] = 'RESOLVED'
            event(state, 'exposure', identity, 'RESOLVED', previous, context, test)
    state['last_success'] = now()
    state['last_commit'] = receipt['commit']
    state['last_database'] = receipt['database_commit']


def chat_environment():
    # The user's chatd socket/SSH agent may need the standard runtime directory.
    allowed = ('PATH', 'HOME', 'XDG_RUNTIME_DIR', 'SSH_AUTH_SOCK')
    return {k: os.environ[k] for k in allowed if k in os.environ}


def chat_json(chatctl, argv, body=None, *, timeout=15):
    # Bounded process group, no shell, no inherited Git/Cargo/RustSec overrides.
    gate.require(timeout > 0, 'delivery budget exhausted')
    if body is None:
        rc, out, err = gate.command([str(chatctl), '--json', *argv], env=chat_environment(), timeout=timeout)
    else:
        # gate.command deliberately has no stdin; send from a durable body file.
        import subprocess
        p = subprocess.Popen([str(chatctl), '--json', *argv], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             env=chat_environment(), start_new_session=True)
        try:
            out, err = p.communicate(body.encode(), timeout=timeout)
        except subprocess.TimeoutExpired:
            import signal
            os.killpg(p.pid, signal.SIGKILL); p.communicate()
            raise gate.Hold('uncertain chatd submission; saved key retained')
        rc = p.returncode
    if body is None and argv[0] == 'lookup' and rc == 4 and err.startswith(b'chatctl: NotFound:'):
        return {'r': 'not_found'}
    gate.require(rc == 0, 'chatd command failed: ' + err.decode(errors='replace')[:500])
    return json.loads(out)


def drain(directory, chatctl, *, budget=90):
    """Network outside writer lock; atomic fresh-state updates never lose rows.

    Concurrent drainers use identical saved keys, so chatd deduplicates them.
    """
    deadline = time.monotonic() + budget
    with transaction(directory) as state:
        keys = [k for k, row in state['outbox'].items() if row['state'] != 'acknowledged']
    for key in keys:
        if time.monotonic() >= deadline:
            break
        with transaction(directory) as state:
            row = dict(state['outbox'][key])
            if row['state'] == 'acknowledged':
                continue
            state['outbox'][key]['attempts'] += 1
        try:
            if row['message_id']:
                found = {'r': 'found', 'id': row['message_id']}
            else:
                found = chat_json(chatctl, ['lookup', '--sender', row['sender'], '--idempotency-key', key], timeout=min(15, deadline - time.monotonic()))
            if found['r'] == 'found':
                message_id = found['id']
            elif found['r'] == 'not_found':
                sent = chat_json(chatctl, ['send', '--as', row['sender'], '--to', row['recipient'],
                    '--conversation', row['conversation'], '--kind', 'request',
                    '--idempotency-key', key], row['body'], timeout=min(15, deadline - time.monotonic()))
                message_id = sent['id']
            else:
                raise gate.Hold('unknown lookup schema')
            # receive is read-only. Never acknowledge on behalf of the recipient.
            message = chat_json(chatctl, ['receive', '--as', row['sender'], '--id', message_id], timeout=min(15, deadline - time.monotonic()))
            gate.require(message['id'] == message_id and message['recipient'] == row['recipient'] and message['idempotency_key'] == key, 'chatd identity mismatch')
            with transaction(directory) as state:
                target = state['outbox'][key]
                target['message_id'] = message_id
                if target['state'] != 'acknowledged':
                    target['state'] = 'acknowledged' if message.get('received_at_ms') is not None else 'submitted'
                target['last_error'] = None
        except Exception as e:
            with transaction(directory) as state:
                state['outbox'][key]['last_error'] = str(e)
                # Keep exact event/key/body even after uncertain delivery.


def prune_runs(directory, keep, *, protect):
    """Bound artifact storage; outbox/state lives independently and is never pruned."""
    directory = Path(directory).resolve()
    gate.require(type(keep) is int and 1 <= keep <= 30, 'invalid retention bound')
    runs = []
    for path in directory.glob('run-*'):
        try:
            uuid.UUID(path.name[4:])
        except ValueError:
            continue
        gate.require(not path.is_symlink() and path.is_dir(), 'unsafe run retention path')
        runs.append(path)
    runs.sort(key=lambda p: p.stat().st_mtime_ns, reverse=True)
    retained = {protect, *runs[:keep]}
    # Protect counts as one of the bounded slots, even after an unusual clock jump.
    if protect not in runs[:keep]:
        retained = {protect, *runs[:max(0, keep - 1)]}
    for path in runs:
        if path not in retained:
            shutil.rmtree(path)


def check(config):
    directory = Path(config['state_dir']).resolve()
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (directory / 'scan.lock').open('a') as lease:
        try:
            fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0  # Another owned checker is normal, not a health outage.
        run = directory / ('run-' + str(uuid.uuid4())); run.mkdir(mode=0o700)
        context = {'run': str(run), 'mode': 'live', 'started_at': now()}
        with transaction(directory) as state:
            state['last_started'] = context['started_at']
        try:
            source = run / 'source'
            cache = directory / 'source.git'
            env = chat_environment()
            if not cache.exists():
                gate.checked(['git', 'init', '--bare', '--quiet', str(cache)], env=env)
            url = gate.checked(['git', '-C', config['repo'], 'remote', 'get-url', 'origin'], env=env).decode().strip()
            # Exact main ref, confirmed fresh fetch. Never scan a cached ref on failure.
            gate.checked(['git', '--git-dir', str(cache), 'fetch', '--quiet', '--depth=1',
                          url, '+refs/heads/main:refs/heads/main'], env=env, timeout=180)
            commit = gate.checked(['git', '--git-dir', str(cache), 'rev-parse', 'refs/heads/main'], env=env).decode().strip()
            advertised = gate.checked(['git', 'ls-remote', '--exit-code', url, 'refs/heads/main'], env=env).decode().split()
            gate.require(len(advertised) == 2 and advertised[0] == commit and advertised[1] == 'refs/heads/main', 'origin main advanced during acquisition; no cached scan')
            context.update(commit=commit, origin_fetch={'success': True, 'fetched_at': now(), 'ls_remote_commit': advertised[0]})
            gate.checked(['git', '--git-dir', str(cache), '-c', 'pack.threads=1', '-c', 'pack.windowMemory=16m', '-c', 'pack.deltaCacheSize=8m', 'gc', '--prune=now', '--quiet'], env=env)
            gate.checked(['git', 'clone', '--no-hardlinks', '--quiet', '--no-checkout', str(cache), str(source)], env=env)
            gate.checked(['git', '-C', str(source), 'checkout', '--quiet', '--detach', commit], env=env)
            result = gate.main(['--repo', str(source), '--commit', commit, '--run', str(run / 'g1'),
                               '--tool', config['tool'], '--mode', 'live'])
            receipt = json.loads((run / 'g1/receipt.json').read_text())
            context['database_commit'] = receipt.get('database_commit')
            with transaction(directory) as state:
                scan_result(state, receipt, context, test=config.get('test_mode', False))
            rc = 2 if result == 2 else 0
        except Exception as e:
            with transaction(directory) as state:
                state['last_completed'] = now()
                health(state, str(e), context, test=config.get('test_mode', False))
            rc = 2
        drain(directory, config['chatctl'])
        prune_runs(directory, config.get('retain_runs', 7), protect=run)
        return rc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('action', choices=['check', 'health', 'deliver', 'age'])
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    gate.require(set(config) <= {'repo', 'state_dir', 'tool', 'chatctl', 'test_mode', 'retain_runs'} and {'repo', 'state_dir', 'tool', 'chatctl'} <= set(config), 'unknown watch config')
    gate.require(type(config.get('test_mode', False)) is bool, 'test_mode must be boolean')
    gate.require(type(config.get('retain_runs', 7)) is int and 1 <= config.get('retain_runs', 7) <= 30, 'retain_runs must be 1..30')
    if args.action == 'check':
        return check(config)
    if args.action == 'health':
        with transaction(config['state_dir']) as state:
            health(state, 'checker unit failed; inspect journal and retained run', {'unit_failure_at': now()}, test=config.get('test_mode', False))
        drain(config['state_dir'], config['chatctl'], budget=60)
    elif args.action == 'deliver':
        drain(config['state_dir'], config['chatctl'])
    else:
        with transaction(config['state_dir']) as state:
            last = state['last_success']
            print(json.dumps({'last_success': last, 'age_seconds': (gate.timestamp(now()) - gate.timestamp(last)).total_seconds() if last else None,
                              'warning': 'No successful scan recorded' if last is None else 'Watch availability depends on the enabled user timer and manager'}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
