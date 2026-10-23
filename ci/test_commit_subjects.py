#!/usr/bin/env python3
"""Isolated history and advisory-order controls; no production mutation."""
import contextlib
import io
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import commit_subjects as lint
import gate_preflight as preflight


class HistoryControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="commit-history-control-")
        self.repo = Path(self.temp.name)
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.name", lint.IDENTITY[0])
        self.git("config", "user.email", lint.IDENTITY[1])
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "core.hooksPath", "/dev/null")
        self.base = self.commit("legacy published subject is not relinted")
        self.git("update-ref", "refs/remotes/origin/main", self.base)

    def tearDown(self):
        self.temp.cleanup()

    def git(self, *args, env=None):
        q = subprocess.run(["git", "-C", str(self.repo), *args],
                           capture_output=True, text=True, check=True,
                           env=os.environ | {"GIT_AUTHOR_NAME": lint.IDENTITY[0], "GIT_AUTHOR_EMAIL": lint.IDENTITY[1],
                                             "GIT_COMMITTER_NAME": lint.IDENTITY[0], "GIT_COMMITTER_EMAIL": lint.IDENTITY[1]} | (env or {}))
        return q.stdout.strip()

    def commit(self, subject, env=None):
        self.git("commit", "--allow-empty", "--cleanup=verbatim", "-qm", subject, env=env)
        return self.git("rev-parse", "HEAD")

    def plan(self, number="0639", summary="gate dependency advisories"):
        p = self.repo / "plans/product/adr" / (number + "_gate_dependency_advisories.md")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("# Decision\n")
        self.git("add", str(p.relative_to(self.repo)))
        return self.commit("plan: adr " + number + " " + summary)

    def report(self):
        return lint.evaluate(self.repo, "HEAD")

    def test_plan_impl_and_patch_subjects_pass(self):
        self.plan()
        self.commit("impl: adr 0639 gate dependency advisories")
        for subject in ["fix: reject an invalid certificate", "test: cover the empty range", "docs: explain installation"]:
            self.commit(subject)
        self.assertEqual(self.report()["verdict"], "PASS")

    def test_empty_range_does_not_rewrite_published_history(self):
        self.assertEqual(self.report()["commits"], [])
        self.assertEqual(self.report()["verdict"], "PASS")

    def test_invalid_subject_classes_fail(self):
        for subject in [
            "fix(node): close startup", "test(storage): close startup",
            "feat: close startup", "ci: check source", "chore: update source",
            "style: format source", "Fix: close startup", "fix: Close startup",
            "fix: change source (r1)", "fix: change source )",
            "impl: gate history", "fix: naïve text", "fix: finish cut c",
            "docs: record evidence", "fix: update e1.1", "plan: adr 177 gate history",
        ]:
            with self.subTest(subject=subject):
                self.git("reset", "--hard", self.base)
                self.commit(subject)
                self.assertEqual(self.report()["verdict"], "FAIL")

    def test_both_identity_fields_are_checked(self):
        for role in ["AUTHOR", "COMMITTER"]:
            for field in ["NAME", "EMAIL"]:
                with self.subTest(role=role, field=field):
                    self.git("reset", "--hard", self.base)
                    self.commit("fix: reject invalid identity", {"GIT_" + role + "_" + field: "someone" if field == "NAME" else "someone@example.invalid"})
                    self.assertEqual(self.report()["verdict"], "FAIL")

    def test_orphan_impl_fails(self):
        self.commit("impl: adr 0639 gate dependency advisories")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_multiple_impls_may_have_different_summaries(self):
        self.plan()
        self.commit("impl: adr 0639 gate another policy")
        self.commit("impl: adr 0639 check delivery identity")
        self.assertEqual(self.report()["verdict"], "PASS")

    def test_intervening_lanes_are_accepted(self):
        self.plan()
        self.plan("0640", "trust operator authorities")
        self.commit("impl: adr 0640 load operator roots")
        self.commit("impl: adr 0639 gate dependency advisories")
        self.assertEqual(self.report()["verdict"], "PASS")

    def test_plan_requires_its_product_adr_document(self):
        self.commit("plan: adr 0639 gate dependency advisories")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_plan_cannot_change_a_second_file(self):
        p = self.repo / "extra.txt"
        p.write_text("not an ADR")
        self.git("add", "extra.txt")
        self.plan()
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_plan_can_amend_existing_adr(self):
        self.plan()
        p = self.repo / "plans/product/adr/0639_gate_dependency_advisories.md"
        p.write_text("# Amended decision\n")
        self.git("add", str(p.relative_to(self.repo)))
        self.commit("plan: adr 0639 amend delivery policy")
        self.commit("impl: adr 0639 persist delivery keys")
        self.assertEqual(self.report()["verdict"], "PASS")

    def test_message_body_is_refused(self):
        self.commit("fix: change a check\n\nAn otherwise hidden body")
        report = self.report()
        self.assertEqual(report["verdict"], "FAIL")
        self.assertIn("commit messages must have no body", report["commits"][-1]["errors"])

    def test_missing_adr_at_earlier_plan_cannot_authorize(self):
        p = self.repo / "plans/product/adr/0639_gate_dependency_advisories.md"
        p.parent.mkdir(parents=True)
        p.write_text("# Earlier decision\n")
        self.git("add", str(p.relative_to(self.repo)))
        self.commit("fix: add an old document")
        self.git("rm", str(p.relative_to(self.repo)))
        self.commit("plan: adr 0639 delete the document")
        self.commit("impl: adr 0639 gate dependency advisories")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_second_parent_plan_cannot_authorize_impl(self):
        self.git("checkout", "-qb", "side")
        self.plan()
        self.git("checkout", "main")
        self.commit("fix: update startup")
        self.git("merge", "--no-ff", "-m", "impl: adr 0639 gate dependency advisories", "side")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_nonconforming_side_branch_commits_are_checked(self):
        self.git("checkout", "-qb", "side")
        self.commit("ci: bypass through a merge")
        self.git("checkout", "main")
        self.commit("fix: update startup")
        self.git("merge", "--no-ff", "-m", "fix: join approved work", "side")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_nonancestor_base_holds(self):
        self.git("checkout", "-qb", "side")
        other = self.commit("fix: diverge")
        self.git("checkout", "main")
        self.commit("fix: another branch")
        self.git("update-ref", "refs/remotes/origin/main", other)
        with self.assertRaises(lint.HistoryError):
            self.report()

    def test_missing_base_or_candidate_holds(self):
        with self.assertRaises(lint.HistoryError):
            lint.evaluate(self.repo, "missing-ref")
        self.git("update-ref", "-d", "refs/remotes/origin/main")
        with self.assertRaises(lint.HistoryError):
            self.report()

    def test_shallow_history_holds(self):
        (self.repo / ".git/shallow").write_text(self.base + "\n")
        with self.assertRaises(lint.HistoryError):
            self.report()

    def test_history_refusal_prevents_advisory_start(self):
        self.commit("ci: bypass the gate")
        with patch.object(preflight.subprocess, "run", wraps=subprocess.run) as run:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(preflight.run(self.repo, "HEAD", []), 1)
        self.assertFalse(any("advisories/gate.py" in str(call.args[0]) for call in run.call_args_list))

    def test_history_hold_prevents_advisory_start(self):
        self.git("update-ref", "-d", "refs/remotes/origin/main")
        with patch.object(preflight.subprocess, "run", wraps=subprocess.run) as run:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(preflight.run(self.repo, "HEAD", []), 2)
        self.assertFalse(any("advisories/gate.py" in str(call.args[0]) for call in run.call_args_list))

    def test_replacement_refs_cannot_hide_bad_history(self):
        bad = self.commit("ci: hide the actual subject")
        self.git("reset", "--hard", self.base)
        replacement = self.commit("fix: show a different subject")
        self.git("reset", "--hard", bad)
        self.git("replace", bad, replacement)
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_advisory_receives_one_frozen_candidate(self):
        head = self.commit("fix: accept a certificate")
        real = subprocess.run
        calls = []

        def dispatch(argv, **kwargs):
            if "advisories/gate.py" in str(argv):
                calls.append(argv)
                return subprocess.CompletedProcess(argv, 7)
            return real(argv, **kwargs)

        with patch.object(preflight.subprocess, "run", side_effect=dispatch):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(preflight.run(self.repo, "HEAD", ["--mode", "replay"]), 7)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][calls[0].index("--commit") + 1], head)

    def policy(self, oversize=None, counts=None):
        p = self.repo / lint.LIMITS
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"schema": 1, "oversize": oversize or [], "impl_limits": counts or []}))
        self.git("add", lint.LIMITS)
        self.commit("fix: review decision size exceptions")

    def test_operator_product_noun_and_rnx_case_examples(self):
        self.assertIsNone(lint.subject_error("impl: adr 0175 polariton source is rustfmt clean"))
        self.assertIsNone(lint.subject_error("impl: adr 0101 import the wendelon-key"))
        rnx = "plan: rnx 0185 migrate to the Rune fork"
        self.assertIsNone(lint.summary_error(rnx.split("0185 ", 1)[1]))
        self.assertIsNone(lint.subject_error(rnx.replace("rnx", "adr", 1)))
        self.assertIsNone(lint.subject_error("impl: adr 0639 validate VISAUT03 and R2"))
        self.assertIsNotNone(lint.subject_error("impl: adr 0639 Validate the code"))
        self.assertIsNotNone(lint.subject_error("impl(polariton): adr 0175 source is clean"))

    def test_bare_cut_verb_passes_and_delivery_label_fails(self):
        self.assertIsNone(lint.subject_error("fix: cut the redundant copy"))
        self.assertIsNotNone(lint.subject_error("fix: finish Cut C"))
        self.assertIsNotNone(lint.subject_error("fix: finish CRYPTO-01"))
        self.assertIsNone(lint.subject_error("docs: explain the evidence policy"))

    def test_numbered_docs_require_current_document(self):
        self.commit("docs: adr 0639 explain the boundary")
        self.assertEqual(self.report()["verdict"], "FAIL")
        self.git("reset", "--hard", self.base)
        self.plan()
        self.commit("docs: adr 0639 explain the boundary")
        self.assertEqual(self.report()["verdict"], "PASS")

    def test_fifth_impl_passes_sixth_fails(self):
        self.plan()
        for i in range(5):
            self.commit("impl: adr 0639 implement step " + str(i))
        self.assertEqual(self.report()["verdict"], "PASS")
        self.commit("impl: adr 0639 implement the sixth step")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_reviewed_count_override_is_exact_and_reasoned(self):
        self.plan()
        for i in range(6):
            self.commit("impl: adr 0639 implement step " + str(i))
        self.policy(counts=[{"adr": "0639", "max_impl": 6, "reason": "Reviewed bounded follow-up", "reviewer": "fixture reviewer"}])
        self.assertEqual(self.report()["verdict"], "PASS")
        self.policy(counts=[{"adr": "0640", "max_impl": 6, "reason": "Other decision", "reviewer": "fixture reviewer"}])
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_plan_line_boundary_and_exact_blob_grandfather(self):
        self.plan()
        p = self.repo / "plans/product/adr/0639_gate_dependency_advisories.md"
        p.write_text("decision\n" * 400)
        self.git("add", str(p.relative_to(self.repo)))
        self.commit("plan: adr 0639 clarify the boundary")
        self.assertEqual(self.report()["verdict"], "PASS")
        p.write_text("decision\n" * 401)
        self.git("add", str(p.relative_to(self.repo)))
        self.commit("plan: adr 0639 correct the longer document")
        self.assertEqual(self.report()["verdict"], "FAIL")
        blob = self.git("rev-parse", "HEAD:" + str(p.relative_to(self.repo)))
        self.policy(oversize=[{"path": str(p.relative_to(self.repo)), "blob": blob,
                              "reason": "Reviewed unchanged legacy document", "reviewer": "fixture reviewer"}])
        self.assertEqual(self.report()["verdict"], "PASS")
        p.write_text("different\n" * 401)
        self.git("add", str(p.relative_to(self.repo)))
        self.commit("plan: adr 0639 change the oversized document")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_new_adr_number_must_be_global_and_unique(self):
        self.plan("0629")
        self.assertEqual(self.report()["verdict"], "FAIL")
        self.git("reset", "--hard", self.base)
        p = self.repo / "nilket/crates/toron/plans/adr/0639_other.md"
        p.parent.mkdir(parents=True)
        p.write_text("# Existing different namespace\n")
        self.git("add", str(p.relative_to(self.repo)))
        self.commit("fix: retain an existing decision")
        self.base = self.git("rev-parse", "HEAD")
        self.git("update-ref", "refs/remotes/origin/main", self.base)
        self.plan()
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_existing_legacy_adr_can_be_amended(self):
        p = self.repo / "plans/product/adr/0175_legacy.md"
        p.parent.mkdir(parents=True)
        p.write_text("# Existing legacy decision\n")
        self.git("add", str(p.relative_to(self.repo)))
        self.commit("legacy published plan seed")
        self.base = self.git("rev-parse", "HEAD")
        self.git("update-ref", "refs/remotes/origin/main", self.base)
        p.write_text("# Clarified legacy decision\n")
        self.git("add", str(p.relative_to(self.repo)))
        self.commit("plan: adr 0175 polariton source is rustfmt clean")
        self.commit("impl: adr 0175 polariton source is rustfmt clean")
        self.assertEqual(self.report()["verdict"], "PASS")

    def test_impl_requires_document_at_impl_commit(self):
        self.plan()
        self.git("rm", "plans/product/adr/0639_gate_dependency_advisories.md")
        self.commit("fix: remove the document")
        self.commit("impl: adr 0639 implement the missing decision")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_malformed_or_unreasoned_limit_policy_holds(self):
        self.policy(counts=[{"adr": "0639", "max_impl": 6, "reason": "", "reviewer": "fixture reviewer"}])
        with self.assertRaises(lint.HistoryError):
            self.report()
        p = self.repo / lint.LIMITS
        p.write_text('{"schema":1,"schema":1,"oversize":[],"impl_limits":[]}')
        self.git("add", lint.LIMITS)
        self.commit("fix: reject ambiguous policy")
        with self.assertRaises(lint.HistoryError):
            self.report()

    def test_short_summary_can_accompany_code_but_not_stand_alone(self):
        self.plan()
        p = self.repo / "plans/product/impl/0639_policy_evidence.md"
        p.parent.mkdir(parents=True)
        p.write_text("Before/after summary\n")
        self.git("add", str(p.relative_to(self.repo)))
        self.commit("impl: adr 0639 record the summary")
        self.assertEqual(self.report()["verdict"], "FAIL")
        self.git("reset", "--hard", "HEAD^")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("Before/after summary\n")
        code = self.repo / "parser.py"
        code.write_text("# implementation fixture\n")
        self.git("add", str(p.relative_to(self.repo)), "parser.py")
        self.commit("impl: adr 0639 implement the parser with a summary")
        self.assertEqual(self.report()["verdict"], "PASS")

    def test_source_overrides_abbreviations_and_help_are_refused(self):
        for option in ["--repo", "--repo=/other", "--commit", "--com=other", "--rep=/other", "--r", "-h", "--help"]:
            with self.subTest(option=option):
                real = subprocess.run
                starts = []

                def dispatch(argv, **kwargs):
                    if "advisories/gate.py" in str(argv):
                        starts.append(argv)
                        return subprocess.CompletedProcess(argv, 0)
                    return real(argv, **kwargs)

                with patch.object(preflight.subprocess, "run", side_effect=dispatch):
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                        self.assertEqual(preflight.run(self.repo, "HEAD", [option]), 2)
                self.assertEqual(starts, [])


if __name__ == "__main__":
    unittest.main()
