# PRODUCT-0153 §7: the cross-target dependency probe

Observational evidence for the distribution lane, 2026-09-03. It diagnoses
the red PRODUCT-0152 found in the cross-target loop and decides nothing; the
release-artifact record owns every remedy named here.

## Question

Every cross-target `cargo check` of Polariton on this host, in both heads,
fails while `aws-lc-sys` compiles its C sources with the host compiler.
Which dependency paths bring that crate in, does selecting the `ring`
provider remove them, and would removing them make cross-compilation from
this Linux host succeed?

## Method

`cargo tree -e features -i <crate>` feature-edge inversion against the
`x86_64-pc-windows-msvc` target for both heads, read against the feature
tables in the published manifests of `reqwest` 0.12.28 and 0.13.4,
`gcloud-auth` 1.3.0, `object_store` 0.13, and `quinn` 0.11.9; then a
direct `cargo check -p ring --target <t>` on `x86_64-pc-windows-msvc` and
`aarch64-apple-darwin` through `nilket/scripts/cargo-cgroup`, because the
failed workspace checks stopped at `aws-lc-sys` before ever reaching
`ring`.

## Findings

### 1. The AWS provider has exactly one root, and it is the Google Cloud TLS feature

Two `reqwest` versions coexist in the graph. Polars' cloud reader reaches
`reqwest` 0.12.28 through `object_store`, whose declared feature
`rustls-tls-native-roots` expands to `__rustls-ring`: that path selects
`ring`, not AWS. The Google Cloud crates reach `reqwest` 0.13.4, whose
`rustls` feature expands to `__rustls-aws-lc-rs`; `gcloud-auth` maps its
own `rustls-tls` feature onto exactly that, and Polariton's manifest
enables `rustls-tls` on both `gcloud-auth` and `gcloud-bigquery`,
unconditionally, so the root is present in the reduced head as well as the
default head. Every other TLS consumer — `quinn` through the Toron client
(default `rustls-ring`), `mysql_async`, `ureq`, `tokio-postgres-rustls`,
`hyper-rustls` on the 0.12 path — selects `ring`. `tokio-rustls`'s
`aws-lc-rs` feature is enabled only by that same reqwest 0.13 feature.

So `aws-lc-sys` is in the graph solely because of the Google Cloud crates'
TLS feature. `gcloud-auth` offers `rustls-no-provider`; taking it, and
installing a `ring` provider at runtime, would remove `aws-lc-sys` from
both heads. That is a security and packaging change and is not made here.

### 2. Removing it would not make cross-compilation from this host succeed

`ring` fails to cross-compile from this host on both targets tried:

| target | `ring` 0.17.14 build script |
|---|---|
| `x86_64-pc-windows-msvc` | "GNU compiler is not supported for this target"; build script failed |
| `aarch64-apple-darwin` | host `cc` rejects the clang-only debug flag; build script failed |

Both providers compile C through `cc` with whatever compiler the host
offers, and this host offers GCC for Linux. No provider selection, and no
feature selection reachable from Polariton's manifest, changes that. The
cross-target loop in `ci/check.sh` can only have been green on a host with
a target C toolchain installed; on this one it has been red for as long as
any C-bearing crypto crate has been in the graph.

### 3. What the release-artifact record therefore has to choose among

Named, not chosen:

- **Target-native build hosts.** A macOS machine and a Windows machine, or
  hosted runners of each, building the reduced head natively. This is the
  only path that also produces real link and package evidence, which §7
  requires before anything is saleable.
- **A cross toolchain on Linux.** `clang` and `lld` with `--target` for
  Windows, plus an Apple SDK for macOS, whose licence terms are the
  question that path has to answer first.
- **Shrinking the C surface anyway.** `rustls-no-provider` on the Google
  Cloud crates with a `ring` provider installed at startup removes the
  largest C dependency and one of two providers, independently of which
  build path is chosen; making the Google Cloud adapter an optional feature
  would remove it from the reduced head entirely. Either is a change to
  what ships and belongs to the release-artifact record.

## Control

Neither failure depends on PRODUCT-0152. That cut added `boson`, a crate
with no native code, and moved a std-only enum; the `aws-lc-sys` and
`ring` build scripts fail identically in both heads and did so on the
default head before the cut, which is the head the existing cross-target
loop already checked.

## What this probe does not decide

The TLS provider, the build hosts, the cross toolchain, the licence for an
Apple SDK, or whether the Google Cloud adapter becomes optional.
