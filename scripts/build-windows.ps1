# Native Windows build entrypoint. Run from PowerShell with the MSVC Rust toolchain.
[CmdletBinding()]
param(
    [ValidateSet('baryon', 'polariton')]
    [string]$Product = 'baryon',
    [switch]$Release,
    [switch]$Test
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT) {
    throw 'This entrypoint requires Windows.'
}

function Invoke-Cargo {
    param([string[]]$CargoArguments)
    Write-Host "cargo $($CargoArguments -join ' ')"
    & cargo @CargoArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Cargo failed with exit code $LASTEXITCODE."
    }
}

Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    $rustVersion = & rustc -vV
    if ($LASTEXITCODE -ne 0) {
        throw 'Could not inspect the Rust toolchain.'
    }
    $hostLine = $rustVersion | Where-Object { $_ -like 'host: *' }
    if (-not $hostLine -or $hostLine -notmatch '^host: (.+-pc-windows-msvc)$') {
        throw 'Select a native Windows MSVC Rust toolchain before building.'
    }
    $target = $Matches[1]
    $productArguments = @('--locked', '--target', $target, '-p', $Product)
    if ($Product -eq 'polariton') {
        # Local Toron supervision depends on Unix. Keep the GUI build independent.
        $productArguments += '--no-default-features'
    }
    $buildArguments = @('build') + $productArguments
    if ($Release) {
        $buildArguments += '--release'
    }
    Invoke-Cargo -CargoArguments $buildArguments

    if ($Test) {
        # Debug assertions are required by Polariton's widget test renderer.
        # Run serially because some engine tests depend on uncontended timing.
        Invoke-Cargo -CargoArguments (@('test', '--locked', '--target', $target,
            '-p', 'libaryon', '--lib', '--', '--test-threads=1'))
        Invoke-Cargo -CargoArguments (@('test') + $productArguments +
            @('--bin', $Product, '--', '--test-threads=1'))
    }
} finally {
    Pop-Location
}
