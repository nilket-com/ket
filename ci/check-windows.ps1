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

Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    Invoke-Step @('fmt', '--check')
    # Per-package runs include each crate's architecture test. A workspace-wide
    # run would also build Nilket's Linux-only member crates.
    # Serial, because some engine tests depend on uncontended timing.
    Invoke-Step @('test', '--locked', '-p', 'libaryon', '-p', 'baryon', '-p', 'ket', '--', '--test-threads=1')
    # Polariton's default features: local Toron supervision compiles out here.
    Invoke-Step @('test', '--locked', '-p', 'polariton', '--', '--test-threads=1')
    Invoke-Step @('check', '--locked', '-p', 'polariton', '--no-default-features')
} finally {
    Pop-Location
}
