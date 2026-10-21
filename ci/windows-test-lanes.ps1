# Windows counterpart of scripts/cargo-ctrl (dot-source this file).
# UNVERIFIED: written and parse-checked on Linux; it has not yet run on a
# Windows host. See nilket/plans/ops/test_parallelism.md.
#
# The tests in ci/controlled-tests.txt are noise-sensitive: their timing and
# projection assertions need an uncontended CPU. They run one at a time in a
# controlled lane sized to what one test uses (2 CPUs, or 1 on a 2-CPU host),
# while every other test runs in parallel on the remaining CPUs. Nothing holds
# the whole machine for one test. See nilket/plans/ops/test_parallelism.md.
#
# `start /affinity` sets the CPU mask when the process is created, so every
# rustc, test binary and thread it spawns inherits the lane.

Set-StrictMode -Version Latest

function Get-ControlledTests {
    param([string]$Manifest)
    Get-Content $Manifest | ForEach-Object { $_.Trim() } |
        Where-Object { $_ -and -not $_.StartsWith('#') } |
        ForEach-Object {
            if ($_ -match '^(\S+) --test (\S+) (\S+)$') {
                [pscustomobject]@{ Package = $Matches[1]; Target = $Matches[2]; Name = $Matches[3] }
            } else {
                [pscustomobject]@{ Package = $null; Target = $null; Name = $_ }
            }
        }
}

function Get-LaneMasks {
    param([int]$ProcessorCount = [Environment]::ProcessorCount)
    # An affinity mask covers one processor group: at most 64 CPUs. Unsigned
    # arithmetic, and no shift by 64, so 63 and 64 CPUs do not overflow.
    $cpus = [Math]::Min($ProcessorCount, 64)
    $controlledCpus = if ($cpus -gt 2) { 2 } else { 1 }
    $controlled = ([uint64]1 -shl $controlledCpus) - 1
    $all = if ($cpus -ge 64) { [uint64]::MaxValue } else { ([uint64]1 -shl $cpus) - 1 }
    $general = if ($cpus -gt 1) { $all -bxor $controlled } else { $all }
    [pscustomobject]@{
        GeneralMask = '{0:X}' -f $general
        GeneralCpus = [Math]::Max($cpus - $controlledCpus, 1)
        ControlledMask = '{0:X}' -f $controlled
    }
}

function Start-InLane {
    param([string]$Mask, [string[]]$CargoArguments)
    $line = 'start "" /affinity ' + $Mask + ' /wait /b cargo ' + ($CargoArguments -join ' ')
    Write-Host "`n==> [$Mask] cargo $($CargoArguments -join ' ')"
    $process = Start-Process -FilePath 'cmd.exe' -ArgumentList @('/d', '/c', $line) -NoNewWindow -PassThru
    # Reading the handle now keeps ExitCode available after the process exits.
    $null = $process.Handle
    $process
}

# Runs `cargo <TestArguments> -- <every controlled name skipped>` in the general
# lane while the controlled tests that belong to these packages run one at a
# time, each by exact name, in the controlled lane.
function Invoke-LanedTests {
    param(
        [string[]]$TestArguments,
        [string[]]$Packages,
        [switch]$LibOnly
    )
    $controlled = @(Get-ControlledTests (Join-Path $PSScriptRoot 'controlled-tests.txt'))
    $lanes = Get-LaneMasks
    # --exact makes every --skip match a whole test name, not a substring, so
    # only the controlled tests themselves leave the general lane.
    $general = @($TestArguments) + @('--', "--test-threads=$($lanes.GeneralCpus)", '--exact')
    foreach ($test in $controlled) { $general += @('--skip', $test.Name) }
    $generalProcess = Start-InLane -Mask $lanes.GeneralMask -CargoArguments $general

    $controlledFailed = $false
    foreach ($test in $controlled) {
        if ($test.Package) {
            if ($LibOnly -or $Packages -notcontains $test.Package) { continue }
            $arguments = @('test', '--locked', '-p', $test.Package, '--test', $test.Target, $test.Name, '--', '--exact')
        } else {
            if ($Packages -notcontains 'libaryon') { continue }
            $arguments = @('test', '--locked', '-p', 'libaryon', '--lib', $test.Name, '--', '--exact')
        }
        $env:CARGO_CTRL_LANE = 'controlled'
        try {
            $process = Start-InLane -Mask $lanes.ControlledMask -CargoArguments $arguments
            $process.WaitForExit()
        } finally {
            Remove-Item Env:CARGO_CTRL_LANE -ErrorAction SilentlyContinue
        }
        if ($process.ExitCode -ne 0) {
            Write-Host "controlled test failed: $($test.Name)"
            $controlledFailed = $true
            break
        }
    }

    $generalProcess.WaitForExit()
    if ($generalProcess.ExitCode -ne 0 -or $controlledFailed) {
        throw "Laned tests failed (general=$($generalProcess.ExitCode) controlled=$controlledFailed)."
    }
}
