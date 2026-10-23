#!/usr/bin/env python3
"""Watch recovery/outbox controls. All sends are mocked; no chatd messages."""
import concurrent.futures
import copy
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import gate
import watch


def finding(version='1.0.0', authorized=False):
    return {'id': 'RUSTSEC-2026-0001', 'crate': 'fixture', 'version': version,
            'source': 'registry+https://github.com/rust-lang/crates.io-index',
            'checksum': 'a' * 64, 'lockfile': 'Cargo.lock',
            'classification': 'authorized' if authorized else 'active'}


def receipt(findings=None):
    return {'verdict': 'FAIL' if findings else 'PASS', 'findings': findings or [],
            'locks': {k: 'hash' for k in gate.LOCKS}, 'reports': {k: {} for k in gate.LOCKS},
            'commit': 'b' * 40, 'database_commit': 'd' * 40}


def events(state):
    return {row['event']['event_uuid']: row['event'] for row in state['outbox'].values()}


class WatchControls(unittest.TestCase):
    def test_first_repeat_two_recipients_and_db_churn(self):
        s = watch.initial(); watch.scan_result(s, receipt([finding()]), {}, test=True)
        self.assertEqual(len(events(s)), 1); self.assertEqual(len(s['outbox']), 2)
        self.assertEqual({r['recipient'] for r in s['outbox'].values()}, {'claude', 'codex'})
        self.assertTrue(all(r['body'].startswith('TEST/old-snapshot') and r['conversation'] == 'advisory-watch-TEST' for r in s['outbox'].values()))
        r = receipt([finding()]); r['database_commit'] = 'e' * 40
        watch.scan_result(s, r, {}, test=True); self.assertEqual(len(events(s)), 1)

    def test_resolve_reintroduce_and_successive_versions(self):
        s = watch.initial()
        for r in [receipt([finding()]), receipt([finding('1.0.1')]), receipt([finding('1.0.2')]), receipt(), receipt([finding()])]:
            watch.scan_result(s, r, {}, test=True)
        self.assertEqual(len(events(s)), 5)
        self.assertEqual(s['event_seq'], 5)
        self.assertEqual(next(iter(s['incidents'].values()))['generation'], 2)

    def test_expiry_renewal_expiry_distinct_events(self):
        s = watch.initial()
        for authorized in [True, False, True, False]:
            watch.scan_result(s, receipt([finding(authorized=authorized)]), {}, test=True)
        self.assertEqual([e['transition'] for e in events(s).values()], ['OPEN', 'AUTHORIZATION_LOST', 'AUTHORIZATION_LOST'])
        self.assertEqual(len(s['outbox']), 6)

    def test_health_outages_recover_without_resolving_on_hold(self):
        s = watch.initial(); watch.scan_result(s, receipt([finding()]), {}, test=True)
        for n in range(2):
            watch.scan_result(s, {'verdict': 'HOLD', 'error': 'fetch failure'}, {}, test=True)
            watch.scan_result(s, {'verdict': 'HOLD', 'error': 'fetch failure'}, {}, test=True)
            self.assertEqual(next(iter(s['incidents'].values()))['state'], 'OPEN')
            watch.scan_result(s, receipt([finding()]), {}, test=True)
        self.assertEqual(len([e for e in events(s).values() if e['kind'] == 'health']), 4)

    def test_missed_run_recovers_and_incomplete_scan_cannot_resolve(self):
        s = watch.initial(); watch.scan_result(s, receipt([finding()]), {}, test=True)
        s['last_success'] = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=27)).isoformat()
        watch.scan_result(s, receipt([finding()]), {}, test=True)
        self.assertTrue(any(e['kind'] == 'health' for e in events(s).values()))
        broken = receipt(); broken['reports'].pop('nilket/Cargo.lock')
        with self.assertRaises(gate.Hold):
            watch.scan_result(s, broken, {}, test=True)
        self.assertEqual(next(iter(s['incidents'].values()))['state'], 'OPEN')

    def test_serialized_writers_no_lost_events(self):
        with tempfile.TemporaryDirectory() as temp:
            def writer(i):
                with watch.transaction(temp) as s:
                    watch.event(s, 'test', str(i), 'OPEN', {}, {}, test=True)
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(writer, range(20)))
            state = json.loads((Path(temp) / 'state.json').read_text())
            self.assertEqual(state['event_seq'], 20); self.assertEqual(len(state['outbox']), 40)

    def test_lost_receipt_restart_one_recipient_failure_and_ack(self):
        with tempfile.TemporaryDirectory() as temp:
            with watch.transaction(temp) as s:
                watch.scan_result(s, receipt([finding()]), {}, test=True)
            initial = json.loads((Path(temp) / 'state.json').read_text())
            keys = set(initial['outbox']); server = {}; sends = []; lost = [False]; ack = [False]; fail_codex = [True]
            def chat(exe, argv, body=None, **kwargs):
                if argv[0] == 'lookup':
                    key = argv[-1]
                    return {'r': 'found', 'id': server[key]['id']} if key in server else {'r': 'not_found'}
                if argv[0] == 'send':
                    key = argv[-1]; row = initial['outbox'][key]
                    if row['recipient'] == 'codex' and fail_codex[0]:
                        raise gate.Hold('controlled one recipient failure')
                    sends.append(key)
                    server[key] = {'id': str(len(server) + 1), 'recipient': row['recipient'], 'idempotency_key': key, 'received_at_ms': None}
                    if not lost[0]:
                        lost[0] = True; raise gate.Hold('stored but receipt lost')
                    return {'id': server[key]['id']}
                if argv[0] == 'receive':
                    row = next(copy.deepcopy(v) for v in server.values() if v['id'] == argv[-1]); row['received_at_ms'] = 1 if ack[0] else None; return row
                raise AssertionError(argv)
            with patch.object(watch, 'chat_json', chat):
                watch.drain(temp, '/fake/chatctl')
                watch.drain(temp, '/fake/chatctl')
                self.assertEqual(len(sends), 1)  # lost receipt found by SAME key
                fail_codex[0] = False; watch.drain(temp, '/fake/chatctl')
                self.assertEqual(len(sends), 2)
                ack[0] = True; watch.drain(temp, '/fake/chatctl')
                watch.drain(temp, '/fake/chatctl'); self.assertEqual(len(sends), 2)
            end = json.loads((Path(temp) / 'state.json').read_text())
            self.assertEqual(set(end['outbox']), keys)
            self.assertTrue(all(r['state'] == 'acknowledged' for r in end['outbox'].values()))

    def test_interrupted_temp_write_does_not_block_restart(self):
        with tempfile.TemporaryDirectory() as temp:
            p = Path(temp)
            with watch.transaction(p) as state:
                watch.health(state, 'first outage', {}, test=True)
            before = json.loads((p / 'state.json').read_text())
            (p / 'state.json.tmp').write_text('interrupted partial write')
            (p / 'state.json.tmp-orphan').write_text('interrupted partial write')
            with watch.transaction(p) as state:
                watch.scan_result(state, receipt(), {}, test=True)
            after = json.loads((p / 'state.json').read_text())
            self.assertEqual(set(before['outbox']) - set(after['outbox']), set())
            self.assertEqual(len(events(after)), 2)

    def test_never_fired_disabled_watch_warns_next_g1(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'state.json'
            self.assertEqual(gate.watch_age(path)['status'], 'UNKNOWN')
            path.write_text(json.dumps({'last_success': (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=27)).isoformat()}))
            self.assertEqual(gate.watch_age(path)['status'], 'STALE_OR_CLOCK_SKEW')

    def test_first_reason_overlap_combined_event(self):
        s = watch.initial()
        watch.health(s, 'original fetch failure', {}, test=True)
        watch.health(s, 'checker unit failed', {}, test=True)
        self.assertEqual(s['health']['scan']['reason'], 'original fetch failure')
        self.assertEqual(s['health']['scan']['notes'], ['checker unit failed'])
        s = watch.initial()
        watch.scan_result(s, receipt([finding(authorized=True)]), {}, test=True)
        watch.scan_result(s, receipt([finding('1.0.1', authorized=False)]), {}, test=True)
        self.assertIn('AFFECTED_VERSIONS_CHANGED_AND_AUTHORIZATION_LOST', [e['transition'] for e in events(s).values()])
        with tempfile.TemporaryDirectory() as temp:
            with (Path(temp) / 'scan.lock').open('a') as lease:
                import fcntl
                fcntl.flock(lease, fcntl.LOCK_EX)
                self.assertEqual(watch.check({'state_dir':temp}), 0)
            self.assertFalse((Path(temp) / 'state.json').exists())

    def test_retention_preserves_outbox_and_bounds_owned_runs(self):
        import uuid
        with tempfile.TemporaryDirectory() as temp:
            p = Path(temp)
            with watch.transaction(p) as s:
                watch.health(s, 'outage', {}, test=True)
            for _ in range(5):
                run = p / ('run-' + str(uuid.uuid4())); run.mkdir()
                (run / 'artifact').write_text('fixture')
            protect = run
            watch.prune_runs(p, 2, protect=protect)
            self.assertEqual(len(list(p.glob('run-*'))), 2)
            self.assertTrue(protect.exists())
            self.assertEqual(len(json.loads((p/'state.json').read_text())['outbox']), 2)

    def test_concurrent_ack_cannot_be_downgraded(self):
        with tempfile.TemporaryDirectory() as temp:
            with watch.transaction(temp) as s:
                watch.scan_result(s, receipt([finding()]), {}, test=True)
                for k,r in s['outbox'].items():r['message_id']=k
            def chat(exe, argv, body=None, **kw):
                self.assertGreater(kw['timeout'], 0)
                key=argv[-1]
                with watch.transaction(temp) as s:
                    s['outbox'][key]['state']='acknowledged'
                    recipient=s['outbox'][key]['recipient']
                return {'id':key,'recipient':recipient,'idempotency_key':key,'received_at_ms':None}
            with patch.object(watch,'chat_json',chat):watch.drain(temp,'/fixture/chatctl')
            s=json.loads((Path(temp)/'state.json').read_text())
            self.assertTrue(all(r['state']=='acknowledged' for r in s['outbox'].values()))

    def test_failed_origin_fetch_never_scans_cache(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = {'state_dir': temp, 'repo': '/fixture/source', 'tool': '/fake/tool', 'chatctl': '/fake/chatctl'}
            def checked(argv, **kw):
                if 'get-url' in argv: return b'https://example.invalid/repo.git\n'
                if 'fetch' in argv: raise gate.Hold('controlled origin fetch failure')
                if 'init' in argv: return b''
                raise AssertionError('cached scan attempted: ' + str(argv))
            with patch.object(gate, 'checked', checked), patch.object(gate, 'main') as evaluate, patch.object(watch, 'drain'):
                self.assertEqual(watch.check(cfg), 2); evaluate.assert_not_called()
            s = json.loads((Path(temp) / 'state.json').read_text())
            self.assertIsNone(s['last_success']); self.assertEqual(len(events(s)), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
