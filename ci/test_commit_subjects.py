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
        self.write("plans/product/adr/0638_previous_decision.md")
        self.publish("legacy published subject is not relinted")

    def write(self, path, text="# Decision\n"):
        p = self.repo / path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
        self.git("add", path)

    def publish(self, subject):
        self.base = self.commit(subject)
        self.git("update-ref", "refs/remotes/origin/main", self.base)
        return self.base

    def adr(self, path, number, summary="decide the boundary"):
        self.write(path)
        return self.commit("plan: adr " + number + " " + summary)

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
        p.parent.mkdir(parents=True, exist_ok=True)
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

    def policy(self, oversize=None, counts=None, schema=2):
        p = self.repo / lint.LIMITS
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"schema": schema, "oversize": oversize or [], "impl_limits": counts or []}))
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
        self.policy(counts=[{"directory": "plans/product/adr", "adr": "0639", "max_impl": 6, "reason": "Reviewed bounded follow-up", "reviewer": "fixture reviewer"}])
        self.assertEqual(self.report()["verdict"], "PASS")
        self.policy(counts=[{"directory": "plans/product/adr", "adr": "0640", "max_impl": 6, "reason": "Other decision", "reviewer": "fixture reviewer"}])
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

    def test_new_adr_number_is_the_next_in_its_directory(self):
        for number, verdict in [("0629", "FAIL"), ("0637", "FAIL"), ("0638", "FAIL"), ("0639", "PASS"), ("0640", "FAIL")]:
            with self.subTest(number=number):
                self.git("reset", "--hard", self.base)
                self.plan(number)
                self.assertEqual(self.report()["verdict"], verdict)

    def test_numbers_land_in_order(self):
        self.plan("0640")
        self.plan("0639")
        report = self.report()
        self.assertEqual(report["verdict"], "FAIL")
        self.assertIn("new ADR in plans/product/adr must be the next number there, 0639", report["commits"][0]["errors"])
        self.assertIn("new ADR in plans/product/adr must be the next number there, 0641", report["commits"][1]["errors"])

    def test_a_number_from_another_directory_is_not_a_conflict(self):
        self.write("nilket/crates/toron/plans/adr/0639_other.md")
        self.publish("fix: retain an existing decision")
        self.plan("0639", "product gate dependency advisories")
        self.commit("impl: adr 0639 product gate dependency advisories")
        self.assertEqual(self.report()["verdict"], "PASS")
        self.git("reset", "--hard", self.base)
        self.plan()
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_a_repository_wide_number_in_a_small_directory_fails(self):
        self.write("crates/editor/plans/adr/0011-earlier.md")
        self.publish("fix: seed the editor directory")
        self.adr("crates/editor/plans/adr/0639-widths.md", "0639")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_same_directory_duplicate_number_fails(self):
        self.write("plans/product/adr/0638_second_document.md")
        self.commit("plan: adr 0638 a second document with the same number")
        report = self.report()
        self.assertEqual(report["verdict"], "FAIL")
        self.assertIn("ADR number 0638 already exists in plans/product/adr", report["commits"][-1]["errors"])

    def test_a_new_directory_starts_at_one(self):
        for number in ["0000", "0002"]:
            with self.subTest(number=number):
                self.git("reset", "--hard", self.base)
                self.adr("crates/editor/plans/adr/" + number + "-skip.md", number)
                self.assertEqual(self.report()["verdict"], "FAIL")
        self.git("reset", "--hard", self.base)
        self.adr("crates/editor/plans/adr/0001-start.md", "0001")
        self.adr("crates/editor/plans/adr/0000-before.md", "0000")
        self.assertEqual(self.report()["verdict"], "FAIL")
        self.git("reset", "--hard", self.base)
        self.adr("crates/editor/plans/adr/0001-start.md", "0001")
        self.adr("crates/editor/plans/adr/0002-next.md", "0002")
        self.commit("impl: adr 0002 build the next decision")
        self.assertEqual(self.report()["verdict"], "PASS")

    def seed_same_number_history(self):
        self.write("crates/editor/plans/adr/0011-earlier.md")
        self.commit("fix: seed the editor directory")
        self.adr("plans/product/adr/0012_product.md", "0012", "product decision")
        for i in range(5):
            self.commit("impl: adr 0012 step " + str(i))
        self.publish("fix: publish the product decision")
        plan, count = lint.lineage(self.repo, self.base, "plans/product/adr", "0012", {})
        self.assertEqual(plan["document"], "plans/product/adr/0012_product.md")
        self.assertEqual(count, 5)

    def test_independent_directories_share_a_number(self):
        self.seed_same_number_history()
        self.adr("crates/editor/plans/adr/0012-widths.md", "0012", "editor table widths")
        for i in range(5):
            self.commit("impl: adr 0012 editor step " + str(i))
        report = self.report()
        self.assertEqual(report["verdict"], "PASS")
        self.assertEqual(report["commits"][-1]["decision"], {"directory": "crates/editor/plans/adr", "adr": "0012"})
        self.assertEqual(report["commits"][-1]["impl_count"], 5)
        self.commit("impl: adr 0012 editor sixth step")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_an_ambiguous_unqualified_number_fails(self):
        self.seed_same_number_history()
        self.adr("crates/editor/plans/adr/0012-widths.md", "0012")
        self.assertEqual(self.report()["verdict"], "FAIL")
        self.git("reset", "--hard", self.base)
        self.adr("crates/editor/plans/adr/0012-widths.md", "0012", "editor table widths")
        self.commit("impl: adr 0012 build the widths")
        report = self.report()
        self.assertEqual(report["verdict"], "FAIL")
        self.assertIn("name its directory", report["commits"][-1]["errors"][0])

    def test_a_qualifier_must_name_the_plans_own_directory(self):
        self.seed_same_number_history()
        self.adr("crates/editor/plans/adr/0012-widths.md", "0012", "product table widths")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_a_count_exception_does_not_transfer_to_another_decision(self):
        self.seed_same_number_history()
        self.adr("crates/editor/plans/adr/0012-widths.md", "0012", "editor table widths")
        for i in range(6):
            self.commit("impl: adr 0012 editor step " + str(i))
        self.policy(counts=[{"directory": "plans/product/adr", "adr": "0012", "max_impl": 7,
                             "reason": "Reviewed product follow-up", "reviewer": "fixture reviewer"}])
        self.assertEqual(self.report()["verdict"], "FAIL")
        self.policy(counts=[{"directory": "crates/editor/plans/adr", "adr": "0012", "max_impl": 6,
                             "reason": "Reviewed editor follow-up", "reviewer": "fixture reviewer"}])
        self.assertEqual(self.report()["verdict"], "PASS")

    def test_deleting_the_bound_document_fails_despite_a_same_number_document(self):
        self.seed_same_number_history()
        self.adr("crates/editor/plans/adr/0012-widths.md", "0012", "editor table widths")
        self.git("rm", "-q", "crates/editor/plans/adr/0012-widths.md")
        self.commit("fix: remove the editor decision")
        self.commit("impl: adr 0012 editor implement the removed decision")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_a_directory_with_the_documents_name_is_not_the_document(self):
        for kind in ["impl", "docs"]:
            with self.subTest(kind=kind):
                self.git("reset", "--hard", self.base)
                self.plan()
                path = "plans/product/adr/0639_gate_dependency_advisories.md"
                self.git("rm", "-q", path)
                self.write(path + "/readme.txt", "not a decision\n")
                self.commit("fix: replace the decision with a directory")
                self.commit(kind + ": adr 0639 act on the missing decision")
                self.assertEqual(self.report()["verdict"], "FAIL")

    def test_interleaved_decisions_keep_their_own_identity(self):
        self.write("crates/editor/plans/adr/0011-earlier.md")
        self.write("crates/other/plans/adr/0011-earlier.md")
        self.publish("fix: seed two directories")
        self.adr("crates/editor/plans/adr/0012-widths.md", "0012")
        for i in range(5):
            self.commit("impl: adr 0012 editor step " + str(i))
        self.adr("crates/other/plans/adr/0012-other.md", "0012", "other decision")
        self.commit("impl: adr 0012 other step")
        self.commit("docs: adr 0012 editor explain the widths")
        report = self.report()
        self.assertEqual(report["verdict"], "PASS")
        self.assertEqual(report["commits"][-2]["impl_count"], 1)
        self.assertEqual(report["commits"][-1]["plan"]["document"], "crates/editor/plans/adr/0012-widths.md")
        self.commit("impl: adr 0012 editor sixth step")
        report = self.report()
        self.assertEqual(report["verdict"], "FAIL")
        self.assertEqual(report["commits"][-1]["impl_count"], 6)
        self.git("reset", "--hard", "HEAD^")
        self.commit("docs: adr 0012 explain both")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def link(self, path, target="missing-document.md"):
        p = self.repo / path
        p.parent.mkdir(parents=True, exist_ok=True)
        if p.is_symlink() or p.exists():
            p.unlink()
        p.symlink_to(target)
        self.git("add", path)

    def test_a_link_cannot_be_a_plan_document(self):
        self.write("crates/editor/plans/adr/0011-earlier.md")
        self.publish("fix: seed the editor directory")
        self.link("crates/editor/plans/adr/0012-next.md")
        self.commit("plan: adr 0012 editor define the next decision")
        report = self.report()
        self.assertEqual(report["verdict"], "FAIL")
        self.assertIn("plan document crates/editor/plans/adr/0012-next.md must be a regular file",
                      report["commits"][-1]["errors"])

    def test_a_published_link_plan_cannot_authorize_after_replacement(self):
        self.write("crates/editor/plans/adr/0011-earlier.md")
        self.commit("fix: seed the editor directory")
        self.link("crates/editor/plans/adr/0012-next.md")
        self.commit("plan: adr 0012 editor define the next decision")
        self.publish("fix: publish the linked plan")
        (self.repo / "crates/editor/plans/adr/0012-next.md").unlink()
        self.write("crates/editor/plans/adr/0012-next.md")
        self.commit("fix: replace link with text")
        for kind in ["impl", "docs"]:
            with self.subTest(kind=kind):
                self.commit(kind + ": adr 0012 editor act on the decision")
                self.assertEqual(self.report()["verdict"], "FAIL")
                self.git("reset", "--hard", "HEAD^")
        self.write("crates/editor/plans/adr/0012-next.md", "# Real decision\n")
        self.commit("plan: adr 0012 editor amend into a real decision")
        self.commit("impl: adr 0012 editor implement the decision")
        report = self.report()
        self.assertEqual(report["verdict"], "PASS")
        self.assertEqual(report["commits"][-1]["plan"]["commit"], report["commits"][-2]["commit"])

    def test_a_directory_name_alone_is_not_a_summary(self):
        self.write("crates/editor/plans/adr/0011-earlier.md")
        self.publish("fix: seed the editor directory")
        self.adr("crates/editor/plans/adr/0012-next.md", "0012", "editor")
        self.assertEqual(self.report()["verdict"], "FAIL")
        self.git("reset", "--hard", self.base)
        self.adr("crates/editor/plans/adr/0012-next.md", "0012", "editor decide")
        for subject in ["impl: adr 0012 editor", "docs: adr 0012 editor"]:
            with self.subTest(subject=subject):
                self.commit(subject)
                report = self.report()
                self.assertEqual(report["verdict"], "FAIL")
                self.assertIn("a terse summary must follow the directory name editor", report["commits"][-1]["errors"])
                self.git("reset", "--hard", "HEAD^")
        self.commit("impl: adr 0012 widths")
        self.assertEqual(self.report()["verdict"], "PASS")

    def test_renaming_and_amending_does_not_reset_the_count(self):
        self.plan()
        for i in range(5):
            self.commit("impl: adr 0639 implement step " + str(i))
        self.git("mv", "plans/product/adr/0639_gate_dependency_advisories.md", "plans/product/adr/0639_renamed.md")
        self.commit("docs: adr 0639 rename the decision")
        self.write("plans/product/adr/0639_renamed.md", "# Amended\n")
        self.commit("plan: adr 0639 amend the renamed decision")
        self.commit("impl: adr 0639 implement after the rename")
        report = self.report()
        self.assertEqual(report["verdict"], "FAIL")
        self.assertEqual(report["commits"][-1]["impl_count"], 6)

    def test_amending_a_plan_does_not_reset_its_count(self):
        self.plan()
        for i in range(3):
            self.commit("impl: adr 0639 implement step " + str(i))
        self.write("plans/product/adr/0639_gate_dependency_advisories.md", "# Amended\n")
        self.commit("plan: adr 0639 amend the decision")
        for i in range(2):
            self.commit("impl: adr 0639 implement amended step " + str(i))
        self.assertEqual(self.report()["verdict"], "PASS")
        self.commit("impl: adr 0639 implement the sixth step")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_a_malformed_later_plan_does_not_unbind_the_decision(self):
        self.plan()
        self.write("plans/product/adr/0639_gate_dependency_advisories.md", "# Amended\n")
        self.write("extra.txt", "not an ADR\n")
        self.commit("plan: adr 0639 amend with an extra file")
        self.publish("fix: publish the legacy history")
        self.commit("impl: adr 0639 implement after the malformed plan")
        report = self.report()
        self.assertEqual(report["verdict"], "PASS")
        self.assertEqual(report["commits"][-1]["plan"]["document"], "plans/product/adr/0639_gate_dependency_advisories.md")

    def test_numbered_docs_bind_to_the_planned_document(self):
        self.seed_same_number_history()
        self.git("checkout", "-q", "--orphan", "fresh")
        self.git("rm", "-rqf", ".")
        self.write("plans/product/adr/0012_product.md")
        self.publish("fix: a document without a plan")
        self.commit("docs: adr 0012 explain the unplanned document")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_a_merge_cannot_join_two_documents_with_one_number(self):
        self.write("crates/editor/plans/adr/0011-earlier.md")
        self.publish("fix: seed the editor directory")
        self.git("checkout", "-qb", "side")
        self.adr("crates/editor/plans/adr/0012-side.md", "0012")
        self.git("checkout", "-q", "main")
        self.adr("crates/editor/plans/adr/0012-main.md", "0012")
        self.git("merge", "-q", "--no-ff", "-m", "fix: join approved work", "side")
        report = self.report()
        self.assertEqual(report["verdict"], "FAIL")
        self.assertEqual([c["subject"] for c in report["commits"] if c["errors"]], ["fix: join approved work"])

    def test_a_merge_may_join_one_number_in_two_directories(self):
        self.write("crates/editor/plans/adr/0011-earlier.md")
        self.write("crates/other/plans/adr/0011-earlier.md")
        self.publish("fix: seed two directories")
        self.git("checkout", "-qb", "side")
        self.adr("crates/other/plans/adr/0012-side.md", "0012")
        self.git("checkout", "-q", "main")
        self.adr("crates/editor/plans/adr/0012-main.md", "0012")
        self.git("merge", "-q", "--no-ff", "-m", "fix: join approved work", "side")
        self.assertEqual(self.report()["verdict"], "PASS")

    def test_a_non_plan_commit_cannot_add_a_duplicate(self):
        self.write("plans/product/adr/0638_copy.md")
        self.commit("fix: copy a decision")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_an_unchanged_legacy_duplicate_is_not_rejudged(self):
        self.write("plans/product/adr/0638_legacy_copy.md")
        self.publish("legacy duplicate")
        self.plan()
        self.commit("fix: touch nothing about the duplicate")
        self.assertEqual(self.report()["verdict"], "PASS")

    def test_schema_one_policy_is_read_only_without_count_limits(self):
        self.policy(schema=1)
        self.assertEqual(self.report()["verdict"], "PASS")
        self.policy(counts=[{"adr": "0639", "max_impl": 6, "reason": "Reviewed", "reviewer": "fixture reviewer"}], schema=1)
        with self.assertRaises(lint.HistoryError):
            self.report()
        self.policy(counts=[{"document": "plans/product/adr/0639_x.md", "max_impl": 6, "reason": "Reviewed", "reviewer": "fixture reviewer"}])
        with self.assertRaises(lint.HistoryError):
            self.report()

    def test_a_side_branch_impl_binds_through_its_own_first_parent(self):
        self.git("checkout", "-qb", "side")
        self.adr("crates/editor/plans/adr/0001-start.md", "0001")
        self.commit("impl: adr 0001 build on the side")
        self.git("checkout", "-q", "main")
        self.commit("fix: update startup")
        self.git("merge", "-q", "--no-ff", "-m", "fix: join approved work", "side")
        self.assertEqual(self.report()["verdict"], "PASS")
        self.commit("impl: adr 0001 build after the merge")
        self.assertEqual(self.report()["verdict"], "FAIL")

    def test_existing_legacy_adr_can_be_amended(self):
        p = self.repo / "plans/product/adr/0175_legacy.md"
        p.parent.mkdir(parents=True, exist_ok=True)
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
        self.policy(counts=[{"directory": "plans/product/adr", "adr": "0639", "max_impl": 6, "reason": "", "reviewer": "fixture reviewer"}])
        with self.assertRaises(lint.HistoryError):
            self.report()
        p = self.repo / lint.LIMITS
        p.write_text('{"schema":2,"schema":2,"oversize":[],"impl_limits":[]}')
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
