# Native Windows gate (ADR 0170). Linux cannot cross-check the desktop crates:
# `ring`, `aws-lc-sys`, `psm`, and `stacker` compile C for their target.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw 'ci/check-windows.ps1 runs on Windows; use ci/check.sh on Linux.'
}

function Invoke-Step {
    param([string[]]$CargoArguments)
    Write-Host "`n==> cargo $($CargoArguments -join ' ')"
    & cargo @CargoArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Cargo failed with exit code $LASTEXITCODE."
    }
}

. (Join-Path $PSScriptRoot 'windows-test-lanes.ps1')

Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    Invoke-Step @('fmt', '--check')
    # Per-package runs include each crate's architecture test. A workspace-wide
    # run would also build Nilket's Linux-only member crates. The engine tests
    # that need uncontended timing (ci/controlled-tests.txt) run one at a time
    # in a 2-CPU controlled lane; everything else runs in parallel beside them.
    Invoke-Step @('test', '--locked', '-p', 'libaryon', '-p', 'baryon', '-p', 'ket', '--no-run')
    Invoke-LanedTests -TestArguments @('test', '--locked', '-p', 'libaryon', '-p', 'baryon', '-p', 'ket') `
        -Packages @('libaryon', 'baryon', 'ket')
    # Polariton's default features: local Toron supervision compiles out here.
    # UNVERIFIED at libtest's default thread count: on Linux, ci/check.sh runs
    # only Polariton's bin tests with --no-default-features (at the default
    # count); this full default-feature package run has not been checked in
    # parallel on any host.
    Invoke-Step @('test', '--locked', '-p', 'polariton')
    Invoke-Step @('check', '--locked', '-p', 'polariton', '--no-default-features')
} finally {
    Pop-Location
}
