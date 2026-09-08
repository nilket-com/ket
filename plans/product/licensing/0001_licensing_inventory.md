# Licensing 0001: inventory for counsel

Status: working document, 2026-09-08. This is a factual inventory of what the
ket repository contains, how it is licensed today, where its code came from,
what it depends on, and what has been distributed. It is written for a
software and intellectual-property lawyer. It makes no licensing
recommendation; the commercial model and any license change are decided
elsewhere, after counsel has read this.

Every conclusion is labelled:

- **Verified**: observed directly in the repository, a build artifact, or a
  public API on 2026-09-08, with the path or command given.
- **Reported**: stated by the operator or by an agent from memory, not
  independently checked here.
- **Unknown**: not determinable from the repository; a question for the
  operator or for counsel.

Authorship and ownership are kept in separate columns throughout. Git
records who committed; it does not establish who owns.

## 1. Repositories

| Repository | Visibility | Evidence | Label |
| --- | --- | --- | --- |
| `github.com/nilket-com/ket` (this repository) | Not publicly visible | `origin` remote in `.git/config`; unauthenticated `GET /repos/nilket-com/ket` returns 404 | Verified |
| `github.com/nilket-com/rnx` | Public | `GET /repos/nilket-com/rnx` returns 200; created 2026-09-08 from this repository's `plans/spikes/rune-scripting/` and `plans/rune-scripting/` | Verified |
| `github.com/nilket-com/nilket` (the frozen predecessor) | Not publicly visible now | 404 unauthenticated; whether it exists privately or was deleted is not distinguishable | Unknown |
| Five predecessor repositories under `github.com/JoelBondurant` | Public today | See section 6 | Verified |

History of this repository (Verified, `git log`):

- 2,405 commits on `main`; every commit across all refs is authored
  `Joel Bondurant <em@joelbondurant.com>`; no other committer name appears.
- The earliest commit is dated 2026-04-13 and belongs to the nilket lineage
  (Toron, Wendelon), which was prefix-rewritten under `nilket/` and grafted
  in; the verification script for that rewrite is
  `plans/product/migration/verify_nilket_graft.py`.
- The Polariton, Baryon, and libaryon code entered in one commit,
  `66332c8f` "Initialize Ket monorepo", 2026-07-18: 346 files, 173,082
  lines. That commit is a snapshot import. The development history of that
  code before 2026-07-18 is not in this repository.

## 2. Stated licensing today

| Item | Finding | Evidence | Label |
| --- | --- | --- | --- |
| Root `LICENSE` | MIT License, "Copyright (c) 2023 Joel Bondurant" | `LICENSE:1-3` | Verified |
| How it arrived | Entered as `baryon/LICENSE` in `66332c8f` (2026-07-18); moved to the root in `1540b490` "docs: promote workspace metadata" (2026-07-22) | `git log --diff-filter=A -- baryon/LICENSE`; `git log -- LICENSE` | Verified |
| Other license or notice files | None anywhere in the tree or in history, apart from the font notice in section 4 | `find` for LICENSE/COPYING/NOTICE names; `git log --all --diff-filter=A --name-only` | Verified |
| Workspace `authors` | `["Joel Bondurant"]` at the root; `authors = true` inheritance in four crates | `Cargo.toml:8`; `crates/*/Cargo.toml` | Verified |
| Crate `license` fields | None declared. `baryon` and `polariton` reference the root file only inside Debian packaging metadata (`package.metadata.deb.license-file`) | `crates/baryon/Cargo.toml:15`; `crates/polariton/Cargo.toml:21` | Verified |
| nilket crates | `publish = false`, no license field, no authors field, on all eight crates | `nilket/crates/*/Cargo.toml:5` | Verified |

Note for counsel: a root MIT file states the repository's licensing as
published. It does not by itself show that every file may be licensed that
way; sections 4 to 6 trace the material that needs checking. `publish =
false` only prevents upload to crates.io; it carries no licensing meaning.

## 3. Crates, authorship, and ownership

| Crate | Path | Role | Git authorship | Ownership | Label |
| --- | --- | --- | --- | --- | --- |
| polariton | `crates/polariton` | Desktop application | Joel Bondurant only | Unknown: depends on employment, assignment, and contract facts not in the repository | Verified / Unknown |
| baryon | `crates/baryon` | Editor application | same | same | Verified / Unknown |
| libaryon | `crates/libaryon` | Editor engine library | same | same | Verified / Unknown |
| boson | `shared/boson` | Shared library used by the root workspace | same | same | Verified / Unknown |
| ket, spectrometer | `crates/ket`, `crates/spectrometer` | Workspace tooling | same | same | Verified / Unknown |
| toron, toron-client, toron-tpch | `nilket/crates/` | Query engine, client, benchmark | same | same | Verified / Unknown |
| wendelon | `nilket/crates/wendelon` | Storage daemon | same | same | Verified / Unknown |
| nilket, nilket-local, observability, tls-pki | `nilket/crates/` | Deployment and support | same | same | Verified / Unknown |

Agent-written code (Reported by the agent writing this document; the
operator can confirm): a substantial share of the code and of the planning
records in this repository was produced by AI coding agents, principally
Claude and Codex, working under the operator's direction, and committed
under the operator's name. The repository does not distinguish agent-written
from hand-written lines. Counsel should be told this when assessing
copyright in the work.

Questions only the operator can answer (Unknown): whether any of this code
was written while employed or under contract with terms touching
intellectual property; whether any contributor other than the operator has
ever supplied code, including by paste; whether any code was copied from a
source not listed in section 4 or 6.

## 4. Third-party material in the tree

| Material | Path | Origin and terms | Notice present | Label |
| --- | --- | --- | --- | --- |
| DejaVu Sans Mono | `crates/polariton/assets/fonts/DejaVuSansMono.ttf`, embedded via `crates/polariton/src/fonts/mod.rs:8` | DejaVu fonts are under the Bitstream Vera license, which requires its copyright, trademark, and permission notice to be included in all copies of the font software | No separate notice file. The notice is embedded in the font file itself (`strings` on the .ttf shows the Bitstream copyright, trademark, and permission text), and because the font is compiled into the binary the same text is present in the packaged `/usr/bin/polariton` (section 7). Whether an embedded notice satisfies the license, and whether a separate readable notice is wanted, are questions for counsel | Verified |
| Polariton Plot Sans | `crates/polariton/assets/fonts/PolaritonPlotSans-Regular.ttf`, embedded via `mod.rs:21` | A Noto Sans Regular derivative whose name table was changed to "Polariton Plot Sans" so the bundled bytes win font selection (`mod.rs:13-19`); `fc-scan` reports foundry GOOG. Noto Sans is under the SIL Open Font License 1.1 | `NotoSans-OFL-1.1.txt` alongside, in Debian copyright format, upstream name Noto | Verified |
| Whether "Noto" is a Reserved Font Name | same | The bundled notice (Debian copyright format, Google Inc. and Google LLC 2010-2020, OFL-1.1) declares no Reserved Font Name. The OFL requires a modified font to be renamed only if a Reserved Font Name is declared; this one was renamed regardless. Whether the upstream release the file came from declares one was not checked | Bundled text: Verified; upstream: Unknown |
| `inspect.js` | `crates/polariton/assets/web/inspect.js` | Written in this repository for the published-page arc; header comment at lines 1-5 | n/a | Verified |
| Documentation images | `docs/img/baryon`, `crates/polariton/doc/img`, `plans/product/impl/exhibits`, one plot snapshot | Screenshots and renders produced by the project | n/a | Verified by location; origin of each image Reported |
| Source-level imported code | Searched all Rust, Rune, shell, TOML, HTML, and CSS files for "copyright (c)", "adapted from", "ported from", "derived from", "vendored", "SPDX", "licensed under" | No third-party copyright or attribution notice appears in any source file | n/a | Verified |
| TPC-H | `nilket/crates/toron-tpch` | Benchmark data is generated by the `tpchgen` crate (Apache-2.0), not by TPC's `dbgen`; the crate carries the 22 TPC-H query texts in Rust source | No TPC notice | Verified |

The TPC point is for counsel: the TPC-H specification has its own terms on
use of the queries and on publishing results. The repository contains
performance boards derived from them (`plans/product/adr/0583*`, and the
Toron benchmark evidence). Whether any such result has been published outside
the repository is Unknown.

## 5. Dependencies

Census taken with `cargo metadata --locked` on both workspaces, counting
external packages only (Verified; raw output was not committed).

| Workspace | External packages | Dominant terms |
| --- | --- | --- |
| Root (`Cargo.lock`) | 1,170 | MIT OR Apache-2.0 484; MIT 315; Apache-2.0 OR MIT 83; MIT/Apache-2.0 52; Apache-2.0 49; Unicode-3.0 18 |
| nilket (`nilket/Cargo.lock`) | 499 | MIT OR Apache-2.0 253; MIT 81; Apache-2.0 OR MIT 28; Apache-2.0 25; Unicode-3.0 18 |

Packages that are not plainly permissive, or that declare no license (root
workspace; nilket has only the first row):

| Package | Declared terms | Note | Label |
| --- | --- | --- | --- |
| r-efi 5.3.0 and 6.0.0 | MIT OR Apache-2.0 OR LGPL-2.1-or-later | Choice-of-license; the permissive option is available | Verified |
| self_cell 1.3.0 | Apache-2.0 OR GPL-2.0-only | Choice-of-license | Verified |
| termina 0.3.3 | MIT OR MPL-2.0 | Choice-of-license | Verified |
| ruff_text_size, and nine sibling `ruff_*` crates | Nine declare MIT; `ruff_text_size` declares nothing | Git dependency on `astral-sh/ruff` at tag v0.4.9, pulled by libaryon for its Python language support (`crates/libaryon/src/svp/python_*.rs`) | Verified; upstream repository is MIT, Reported |
| salsa-2022, salsa-2022-macros | Nothing declared | Git dependency on `salsa-rs/salsa` at a pinned revision, reached through ruff | Verified; upstream repository is MIT OR Apache-2.0, Reported |
| tpchgen 2.0.2 | Apache-2.0 | Benchmark data generator | Verified |

The census above is what the lockfiles resolve. It is not the set of
components in a shipped product: workspace metadata includes development
dependencies, build-time tools, and packages for other platforms. The
per-product closures below count only normal dependencies of one binary for
one target, with the features each is compiled with, and are the lists
counsel should work from. They are generated by
`plans/product/licensing/census/census.sh` and committed beside it with the
lockfile hashes, target, and toolchain in `MANIFEST.txt`, so they can be
regenerated and compared (Verified).

| Binary | Workspace | Target | Rows | Distinct name-version pairs | File |
| --- | --- | --- | --- | --- | --- |
| polariton | root | x86_64-unknown-linux-gnu | 847 | 832 | `census/polariton_x86_64-unknown-linux-gnu.txt` |
| baryon | root | same | 462 | 456 | `census/baryon_x86_64-unknown-linux-gnu.txt` |
| toron | nilket | same | 350 | 341 | `census/toron_x86_64-unknown-linux-gnu.txt` |
| wendelon | nilket | same | 311 | 308 | `census/wendelon_x86_64-unknown-linux-gnu.txt` |
| nilket | nilket | same | 356 | 346 | `census/nilket_x86_64-unknown-linux-gnu.txt` |

A package compiled with two feature sets appears twice, which is why rows
exceed distinct pairs; workspace crates are included. These lists are the
resolver's view and not a verified shipped-component list: Cargo documents
that `cargo tree` does not guarantee exact build equivalence, build scripts
can generate code or link native libraries that no row names, and bundled
assets such as the fonts in section 4 sit outside Cargo's graph. Each
artifact that is actually distributed needs its own verification. Mac and
Windows closures can be collected the same way with `--target` without
building for those platforms and are not yet generated; artifact
verification for them waits on real builds. Per-license obligations differ, and counsel should read each license
rather than a summary. As a guide to what the census contains: MIT, the BSD
variants, ISC, Apache-2.0, Unicode-3.0, and OFL-1.1 each condition
redistribution on carrying their notice with copies, Apache-2.0 additionally
on any NOTICE file the package ships; Zlib conditions notice preservation on
source distributions only and makes product acknowledgment optional;
CC0-1.0, Unlicense, 0BSD, and MIT-0 impose no notice condition; the
choice-of-license entries are satisfied by whichever option is chosen. The
two Debian packages in section 7 carry only the project's own MIT text in
their copyright file; no generated third-party notices file exists in the
repository (Verified).

The rnx repository declares three direct dependencies, rune 0.14.1 (MIT OR
Apache-2.0), serde_json, and libc (Verified, its `Cargo.toml`); its full
resolved set is in its own lockfile and is not censused here. Rune's dual
license was confirmed in the pinned crate manifest.

## 6. Public exposure and predecessor repositories

Operator statement (Reported): "All of this stuff was out in the open on
github until 2026." Which repositories, which revisions, and over what
period were public is Unknown from this repository; the predecessors below
are what the public API shows today.

| Repository | Description on GitHub | Public today | License file | Cargo `license` field | Committers | Created / last push | Label |
| --- | --- | --- | --- | --- | --- | --- | --- |
| JoelBondurant/polariz | "Iced plotting widget for Polariton." | Yes | None | None | Joel Bondurant only | 2026-03-13 / 2026-03-19 | Verified |
| JoelBondurant/editon | "Experimental code editor for Polariton." | Yes | None | None | Joel Bondurant only | 2026-03-26 / 2026-03-28 | Verified |
| JoelBondurant/menuton | menu widget | Yes | None | None | not checked | 2026-03-28 / 2026-03-28 | Verified |
| JoelBondurant/plinthon | "Iced status bar widget for Polariton." | Yes | None | None | not checked | 2026-03-29 / 2026-03-29 | Verified |
| JoelBondurant/lattiton | "Iced pane grid widget for Polariton." | Yes | None | None | not checked | 2026-03-28 / 2026-03-30 | Verified |
| JoelBondurant/rune | Fork of rune-rs/rune | Yes | MIT and Apache-2.0 (upstream's) | upstream's | fork | 2026-07-16 / 2026-07-01 | Verified |

Relation to this repository's code, by file-name overlap only (Verified;
contents were not diffed, so how much text is shared is Unknown):

| Predecessor directory | ket directory | Shared file names |
| --- | --- | --- |
| polariz `src/plot` | `crates/polariton/src/plot` | 5 of 5: `colors.rs`, `common.rs`, `core.rs`, `kernels`, `mod.rs` |
| menuton `src/menu` | `crates/polariton/src/gui/menu` | 6 of 6 |
| plinthon `src/statusbar` | `crates/polariton/src/gui/statusbar` | 3 of 3 |
| editon `src/editor` | `crates/libaryon/src` | 0 |
| lattiton `src/pane_grid` | `crates/polariton/src/gui/pane_grid` | 0 |

Two points for counsel follow from this. First, no explicit project
license was found in the inspected revision of any of the five; what
permissions attach to them is unresolved, since GitHub's terms of service
grant viewing and forking of public repositories, and any earlier grant or
third-party license would apply on its own terms. Second, the five
repositories are still public, so whatever their exposure means, it is
ongoing rather than historical. The Rune fork is not referenced by any manifest or lockfile in
this repository (Verified).

The rnx repository is public with three commits and no license file
(Verified). Its content was exported from this repository's `plans/` tree;
the spike code and the first-release record were written by agents in this
session under the operator's direction.

## 7. Distribution

| Item | Finding | Evidence | Label |
| --- | --- | --- | --- |
| Built packages on disk | `polariton_0.1.0-1_amd64.deb` (2026-07-31) and `baryon_0.1.0-1_amd64.deb` (2026-08-01) | `target/debian/` | Verified |
| Their license statement | Each carries `/usr/share/doc/<pkg>/copyright` with "Copyright: 2026, Joel Bondurant" and the full MIT text, and nothing else | `dpkg-deb --fsys-tarfile` | Verified |
| Font notices inside the packaged binary | The Bitstream Vera notice text and the OFL text are present as strings in `/usr/bin/polariton` from the package, because the fonts are compiled in | `strings` on the extracted binary | Verified |
| Who received them | Packages on disk prove builds exist, not recipients | | Unknown |
| Tagged releases | None; the only non-artifact tags are a pre-squash backup and one evidence tag. Absence of a tag does not show that nothing was distributed | `git tag` | Verified |
| Documented install path | The first-customer brief describes `cargo install --locked --path crates/polariton` from a checkout, Linux only; `docs/baryon.md` describes `cargo deb --install` | `plans/product/brief/0001_the_first_customer_journey.md:43`; `docs/baryon.md:107-110` | Verified |
| Hosting | No download page, deploy script, or hostname for distributing builds exists in the tree | grep over scripts, ADRs, briefs, docs | Verified for the tree; whether builds were shared by other means is Unknown |
| Published web pages | The web arc (PRODUCT-0153 to 0157) produces self-contained HTML artifacts. Whether any has been served to the public, and with what notice, is not recorded | | Unknown |
| Local deployment | A Toron and Wendelon deployment runs on the operator's machine for dogfooding | | Reported |
| Source distribution | Whether any copy of this repository or its predecessors was given to anyone outside GitHub visibility | | Unknown |

## 8. What this document does not do

It does not recommend a license, a commercial model, or a change to the
root `LICENSE`. Changing that file is a decision for the operator with
counsel, made after the questions marked Unknown above are answered, and
with the understanding that a later change does not withdraw permissions
attached to copies already furnished under the current text.

## 9. Open questions, collected

For the operator:

1. Which repositories were public, from when to when, and did any carry a
   license file at the time? The five listed in section 6 are public now.
2. Has any build, package, or source copy been given to anyone? If so,
   which version and with which notice?
3. Were any TPC-H results published outside the repository?
4. Employment, contract, or assignment facts bearing on ownership of code
   written since 2023, and whether anyone else has contributed code.
5. Confirmation of the agent-written share of the code, as best it can be
   stated.

For counsel:

1. Copyright status of AI-agent-written code committed under the
   operator's name, and what that means for licensing it out.
2. Effect of the existing MIT grant on copies already furnished, and on the
   still-public predecessor repositories that carry no license.
3. Notice obligations for the bundled fonts and the dependency set when
   binaries are distributed, including through peer-to-peer channels.
4. Whether the TPC-H query texts and the derived performance boards carry
   obligations.

For the repository, factual gaps to close before a release regardless of
license: a separate readable notice for DejaVu if counsel wants one, a
generated third-party notices file per product built from the census
closures, verification of notice preservation in each artifact that is
actually distributed, and the upstream check of the Noto Reserved Font Name
declaration.
