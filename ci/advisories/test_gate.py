#!/usr/bin/env python3
"""Contract controls; no network and no chatd delivery.

Set ADVISORY_TEST_TOOL to the pinned executable for real-tool controls.
"""
import copy
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('gate', Path(__file__).with_name('gate.py'))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
T = dt.datetime(2026, 10, 23, tzinfo=dt.timezone.utc)
LOCK = b'''version = 4
[[package]]
name = "advisory-fixture"
version = "0.1.0"
[[package]]
name = "fixture-vulnerable"
version = "1.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
'''


def entry(**overrides):
    return {'id': 'RUSTSEC-2026-0001', 'crate': 'fixture-vulnerable',
            'reason': 'controlled test-only exception', 'reviewer': 'fixture reviewer',
            'approved_at': '2026-10-01T00:00:00Z', 'expires_at': '2026-10-31T00:00:00Z', **overrides}


def encode_entries(entries):
    # JSON string encoding is also valid for these TOML basic strings.
    if not entries:
        return b'entries = []\n'
    return '\n'.join('[[entries]]\n' + '\n'.join(k + ' = ' + json.dumps(v) for k, v in e.items()) for e in entries).encode()


def report(findings=False):
    r = {'database': {'advisory-count': 1, 'last-commit': None, 'last-updated': None},
         'lockfile': {'dependency-count': 2},
         'settings': {'target_arch': [], 'target_os': [], 'severity': None, 'ignore': [],
                      'informational_warnings': ['unmaintained', 'unsound', 'notice']},
         'vulnerabilities': {'found': findings, 'count': int(findings), 'list': []}, 'warnings': {}}
    if findings:
        r['vulnerabilities']['list'] = [{'advisory': {'id': 'RUSTSEC-2026-0001', 'package': 'fixture-vulnerable', 'informational': None, 'withdrawn': None},
                                        'versions': {'patched': ['>=1.0.1'], 'unaffected': []}, 'affected': None,
                                        'package': {'name': 'fixture-vulnerable', 'version': '1.0.0', 'source': 'registry+https://github.com/rust-lang/crates.io-index', 'checksum': 'a' * 64}}]
    return r


class PolicyControls(unittest.TestCase):
    def test_exact_thirty_days_and_one_second_before_expiry(self):
        e = entry(); when = gate.timestamp(e['expires_at']) - dt.timedelta(seconds=1)
        self.assertTrue(gate.allowlist(encode_entries([e]), when)[(e['id'], e['crate'])]['valid'])

    def test_expiry_boundary_authorizes_nothing(self):
        e = entry(); self.assertFalse(gate.allowlist(encode_entries([e]), gate.timestamp(e['expires_at']))[(e['id'], e['crate'])]['valid'])

    def test_invalid_entries_fail_closed(self):
        cases = [entry(id='RUSTSEC-*'), entry(crate='*'), entry(reason=''), entry(reviewer=''),
                 entry(approved_at='2026-10-24T00:00:00Z'), entry(expires_at='2026-10-31T00:00:01Z'),
                 entry(expires_at='2026-10-01T00:00:00Z'), entry(approved_at='2026-10-01T00:00:00'),
                 {k: v for k, v in entry().items() if k != 'crate'}, entry(unknown='field')]
        for e in cases:
            with self.subTest(e=e), self.assertRaises((gate.Hold, ValueError)):
                gate.allowlist(encode_entries([e]), T)

    def test_duplicate_even_identical_is_invalid(self):
        with self.assertRaises(gate.Hold):
            gate.allowlist(encode_entries([entry(), entry()]), T)

    def test_wrong_crate_cannot_authorize(self):
        entries = gate.allowlist(encode_entries([entry(crate='another-crate')]), T)
        self.assertNotIn(('RUSTSEC-2026-0001', 'fixture-vulnerable'), entries)

    def test_empty_allowlist(self):
        self.assertEqual(gate.allowlist(b'entries = []', T), {})


class ReportControls(unittest.TestCase):
    def parse(self, r, rc):
        return gate.parse_report(json.dumps(r).encode(), rc, 'b' * 40, LOCK)

    def test_clean_and_findings_exit(self):
        self.assertEqual(self.parse(report(), 0)[0], [])
        self.assertEqual(len(self.parse(report(True), 1)[0]), 1)

    def test_report_exit_mismatches_and_tool_error_hold(self):
        for r, rc in [(report(), 1), (report(True), 0), (report(), 2), (report(), -9), (report(), 127)]:
            with self.subTest(rc=rc), self.assertRaises(gate.Hold):
                self.parse(r, rc)

    def test_schema_and_coverage_controls(self):
        bad = []
        for key in report():
            r = report(); del r[key]; bad.append(r)
        r = report(); r['new-schema'] = {}; bad.append(r)
        for key, value in [('ignore', ['RUSTSEC-2026-0001']), ('target_os', ['linux']), ('target_arch', ['x86_64']), ('severity', 'high')]:
            r = report(); r['settings'][key] = value; bad.append(r)
        r = report(); r['database']['advisory-count'] = 0; bad.append(r)
        r = report(); r['database']['last-commit'] = 'c' * 40; bad.append(r)
        r = report(); r['lockfile']['dependency-count'] = 1; bad.append(r)
        r = report(True); r['vulnerabilities']['count'] = 0; bad.append(r)
        r = report(True); r['vulnerabilities']['list'][0]['package']['version'] = '2.0.0'; bad.append(r)
        r = report(True); r['vulnerabilities']['list'][0]['advisory']['withdrawn'] = '2026-10-22'; bad.append(r)
        for r in bad:
            with self.subTest(r=r), self.assertRaises((gate.Hold, KeyError)):
                self.parse(r, int(bool(r.get('vulnerabilities', {}).get('list'))))

    def test_unparseable_json(self):
        with self.assertRaises(ValueError):
            gate.parse_report(b'not json', 0, 'b' * 40, LOCK)


@unittest.skipUnless(os.environ.get('ADVISORY_TEST_TOOL'), 'set ADVISORY_TEST_TOOL for pinned-binary controls')
class RealToolControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tool = str(Path(os.environ['ADVISORY_TEST_TOOL']).resolve())
        policy = json.loads((gate.HERE / 'policy.json').read_text())
        assert gate.sha(Path(cls.tool).read_bytes()) == policy['binary_sha256']

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.r = Path(self.temp.name)
        (self.r / '.cargo').mkdir()
        (self.r / '.cargo/audit.toml').write_bytes((gate.HERE / 'audit.toml').read_bytes())
        self.db = self.r / 'db'; self.db.mkdir()
        (self.db / 'crates/fixture-vulnerable').mkdir(parents=True)
        (self.db / 'crates/fixture-vulnerable/RUSTSEC-2026-0001.md').write_text('''```toml
[advisory]
id = "RUSTSEC-2026-0001"
package = "fixture-vulnerable"
date = "2026-01-01"
[versions]
patched = [">=1.0.1"]
```
# Synthetic test advisory
Fixture only, never a live database.
''')
        gate.checked(['git', 'init', '--quiet', str(self.db)])
        gate.checked(['git', '-C', str(self.db), 'add', '.'])
        gate.checked(['git', '-C', str(self.db), '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', '-c', 'commit.gpgsign=false', 'commit', '--quiet', '-m', 'fixture'])
        self.d = gate.git(self.db, 'rev-parse', 'HEAD').decode().strip()
        self.lock = self.r / 'Cargo.lock'; self.lock.write_bytes(LOCK)

    def invoke(self, **kwargs):
        return gate.command([self.tool, 'audit', '--file', str(self.lock), '--json', '--no-fetch', '--db', str(self.db), '--no-yanked', '--stale'], cwd=self.r, **kwargs)

    def test_real_findings_authorization_and_clean(self):
        rc, raw, _ = self.invoke(); self.assertEqual(rc, 1)
        findings, _ = gate.parse_report(raw, rc, self.d, LOCK)
        self.assertEqual(findings[0]['id'], 'RUSTSEC-2026-0001')
        allowed = gate.allowlist(encode_entries([entry()]), T)
        self.assertTrue(all(allowed[(f['id'], f['crate'])]['valid'] for f in findings))
        clean = LOCK.replace(b'version = "1.0.0"', b'version = "1.0.1"')
        self.lock.write_bytes(clean); rc, raw, _ = self.invoke(); self.assertEqual(rc, 0)
        self.assertEqual(gate.parse_report(raw, rc, self.d, clean)[0], [])

    def test_real_lock_error(self):
        self.lock.write_text('malformed lock')
        rc, raw, _ = self.invoke(); self.assertEqual(rc, 2)
        with self.assertRaises(gate.Hold):
            gate.parse_report(raw, rc, self.d, LOCK)

    def test_ambient_home_ignore_has_no_effect(self):
        home = self.r / 'ambient'; (home / '.cargo').mkdir(parents=True)
        (home / '.cargo/audit.toml').write_text('[advisories]\nignore=["RUSTSEC-2026-0001"]\n')
        env = {**os.environ, 'HOME': str(home), 'CARGO_HOME': str(home / '.cargo')}
        rc, raw, _ = self.invoke(env=env); self.assertEqual(rc, 1)
        self.assertEqual(len(gate.parse_report(raw, rc, self.d, LOCK)[0]), 1)

    def test_no_graph_narrowing(self):
        # An orphan lock package (could be optional/dev/target-only) remains visible.
        self.assertNotIn(b'dependencies', LOCK)
        rc, raw, _ = self.invoke()
        self.assertEqual(len(gate.parse_report(raw, rc, self.d, LOCK)[0]), 1)




@unittest.skipUnless(os.environ.get('ADVISORY_TEST_TOOL'), 'set ADVISORY_TEST_TOOL for evaluator controls')
class EvaluatorControls(RealToolControls):
    # Inherits the actual binary/schema controls, and adds whole-run failure paths.
    def setUp(self):
        super().setUp()
        self.repo = self.r / 'source'; self.repo.mkdir()
        (self.repo / 'nilket').mkdir(); (self.repo / 'ci').mkdir()
        for path in gate.LOCKS:
            (self.repo / path).write_bytes(LOCK)
        (self.repo / 'ci/advisory-allowlist.toml').write_bytes(b'entries = []\n')
        gate.checked(['git', 'init', '--quiet', str(self.repo)])
        self.commit()

    def commit(self):
        gate.git(self.repo, 'add', '.')
        gate.git(self.repo, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', '-c', 'commit.gpgsign=false', 'commit', '--quiet', '--allow-empty', '-m', 'fixture')
        self.c = gate.git(self.repo, 'rev-parse', 'HEAD').decode().strip()

    def run_gate(self, *, tool=None, mode='replay', **extra):
        run = self.r / ('run-' + str(len(list(self.r.glob('run-*')))))
        argv = ['--repo', str(self.repo), '--commit', self.c, '--run', str(run), '--tool', tool or self.tool, '--mode', mode]
        if mode == 'replay':
            argv += ['--database', str(self.db), '--at', T.isoformat()]
        for key, value in extra.items():
            argv += ['--' + key.replace('_', '-'), value]
        rc = gate.main(argv)
        return rc, json.loads((run / 'receipt.json').read_text())

    def test_two_lock_findings_no_short_circuit(self):
        rc, r = self.run_gate()
        self.assertEqual(rc, 1)
        self.assertEqual({f['lockfile'] for f in r['findings']}, set(gate.LOCKS))
        self.assertEqual(len(r['reports']), 2)

    def test_authorized_findings_exit_one_can_pass(self):
        (self.repo / 'ci/advisory-allowlist.toml').write_bytes(encode_entries([entry()]))
        self.commit(); rc, r = self.run_gate()
        self.assertEqual(rc, 0)
        self.assertEqual({f['classification'] for f in r['findings']}, {'authorized'})

    def test_matching_expiry_and_wrong_crate_fail(self):
        for e in [entry(expires_at=T.isoformat()), entry(crate='another-crate')]:
            (self.repo / 'ci/advisory-allowlist.toml').write_bytes(encode_entries([e]))
            self.commit(); rc, r = self.run_gate()
            self.assertEqual(rc, 1)

    def test_missing_changed_or_omitted_lock_hold(self):
        from unittest.mock import patch
        path = self.repo / 'nilket/Cargo.lock'
        path.unlink(); self.assertEqual(self.run_gate()[0], 2)
        path.write_bytes(LOCK + b'\n'); self.assertEqual(self.run_gate()[0], 2)
        path.write_bytes(LOCK)
        # A policy omitting a lock must not silently narrow the graph.
        policy_dir = self.r / 'bad-policy'; policy_dir.mkdir()
        policy = json.loads((gate.HERE / 'policy.json').read_text()); policy['lockfiles'] = ['Cargo.lock']
        (policy_dir / 'policy.json').write_text(json.dumps(policy))
        (policy_dir / 'audit.toml').write_bytes((gate.HERE / 'audit.toml').read_bytes())
        with patch.object(gate, 'HERE', policy_dir):
            self.assertEqual(self.run_gate()[0], 2)

    def test_tool_config_and_allowlist_errors_hold(self):
        from unittest.mock import patch
        wrong = self.r / 'wrong-tool'; wrong.write_text('wrong bytes')
        self.assertEqual(self.run_gate(tool=str(wrong))[0], 2)
        policy_dir = self.r / 'bad-config'; policy_dir.mkdir()
        (policy_dir / 'policy.json').write_bytes((gate.HERE / 'policy.json').read_bytes())
        (policy_dir / 'audit.toml').write_text('[advisories]\nignore=["RUSTSEC-2026-0001"]')
        with patch.object(gate, 'HERE', policy_dir):
            self.assertEqual(self.run_gate()[0], 2)
        (self.repo / 'ci/advisory-allowlist.toml').write_bytes(encode_entries([entry(reason='')]))
        self.commit(); self.assertEqual(self.run_gate()[0], 2)

    def test_empty_and_malformed_database_hold(self):
        advisory = self.db / 'crates/fixture-vulnerable/RUSTSEC-2026-0001.md'
        for contents in ['malformed advisory', None]:
            if contents is None:
                advisory.unlink()
            else:
                advisory.write_text(contents)
            gate.git(self.db, 'add', '.')
            gate.git(self.db, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', '-c', 'commit.gpgsign=false', 'commit', '--quiet', '-m', 'bad database')
            self.assertEqual(self.run_gate()[0], 2)

    def test_scrubbed_audit_and_fetch_environments(self):
        from unittest.mock import patch
        home = self.r / 'ambient-cargo'; home.mkdir()
        (home / 'audit.toml').write_text('[advisories]\nignore=["RUSTSEC-2026-0001"]\n')
        real = gate.command
        audited = []
        def observe(argv, **kwargs):
            if argv[0] == self.tool and 'audit' in argv or 'fetch' in argv:
                env = kwargs['env']; audited.append(env)
                self.assertEqual(set(env), {'PATH', 'HOME', 'CARGO_HOME', 'TMPDIR'})
                self.assertNotEqual(env['CARGO_HOME'], str(home))
                self.assertEqual(list(Path(env['CARGO_HOME']).iterdir()), [])
                if 'fetch' in argv:
                    return 128, b'', b'controlled fetch after env capture'
            return real(argv, **kwargs)
        poison = {'CARGO_HOME': str(home), 'RUSTSEC_IGNORE': '*', 'GIT_DIR': '/no/such/gitdir', 'GIT_CONFIG_COUNT': '1', 'GIT_CONFIG_KEY_0': 'url.bad.insteadOf', 'GIT_CONFIG_VALUE_0': 'https://'}
        with patch.dict(os.environ, poison), patch.object(gate, 'command', observe):
            rc, r = self.run_gate()
            self.assertEqual(rc, 1)
            self.assertTrue(all(f['source'] == 'registry+https://github.com/rust-lang/crates.io-index' and f['checksum'] == 'a' * 64 for f in r['findings']))
            self.assertEqual(self.run_gate(mode='live')[0], 2)
        self.assertEqual(len(audited), 3)

    def test_live_fetch_failure_holds_without_cached_scan(self):
        from unittest.mock import patch
        real = gate.command
        def no_fetch(argv, **kwargs):
            if 'fetch' in argv:
                return 128, b'', b'controlled upstream outage'
            return real(argv, **kwargs)
        with patch.object(gate, 'command', no_fetch):
            rc, r = self.run_gate(mode='live')
        self.assertEqual(rc, 2); self.assertNotIn('reports', r)
        self.assertIn('controlled upstream outage', r['error'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
