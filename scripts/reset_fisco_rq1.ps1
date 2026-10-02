[CmdletBinding()]
param(
    [int]$BlockLimit = 400
)

$ErrorActionPreference = "Stop"
$Base = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$FiscoDir = Join-Path $Base "baseline\fiscobcos"
$Compose = Join-Path $FiscoDir "docker-compose.yml"
$NodesRoot = (Resolve-Path (Join-Path $FiscoDir "nodes")).Path

function Invoke-Checked {
    param([string]$File, [string[]]$ArgumentList, [string]$WorkingDirectory = $Base)
    Push-Location $WorkingDirectory
    try {
        & $File @ArgumentList
        if ($LASTEXITCODE -ne 0) {
            throw "Command failed ($LASTEXITCODE): $File $($ArgumentList -join ' ')"
        }
    } finally {
        Pop-Location
    }
}

Invoke-Checked "docker" @("compose", "-f", $Compose, "down")

foreach ($nodeData in Get-ChildItem -LiteralPath $NodesRoot -Directory |
    Where-Object { $_.Name -like "172.25.*" }) {
    $data = Join-Path $nodeData.FullName "node0\data"
    $resolvedData = [IO.Path]::GetFullPath($data)
    if (-not $resolvedData.StartsWith($NodesRoot + "\", [StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to remove path outside FISCO nodes root: $resolvedData"
    }
    if (Test-Path -LiteralPath $resolvedData) {
        Remove-Item -LiteralPath $resolvedData -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $resolvedData | Out-Null

    $genesis = Join-Path $nodeData.FullName "node0\config.genesis"
    $text = Get-Content -LiteralPath $genesis -Raw -Encoding utf8
    $text = [regex]::Replace(
        $text,
        "block_tx_count_limit=\d+",
        "block_tx_count_limit=$BlockLimit"
    )
    [IO.File]::WriteAllText($genesis, $text, (New-Object Text.UTF8Encoding($false)))

    $config = Join-Path $nodeData.FullName "node0\config.ini"
    $configText = Get-Content -LiteralPath $config -Raw -Encoding utf8
    $configText = [regex]::Replace($configText, "min_seal_time=\d+", "min_seal_time=100")
    [IO.File]::WriteAllText($config, $configText, (New-Object Text.UTF8Encoding($false)))
}

Remove-Item -LiteralPath (Join-Path $FiscoDir "results\contract_address") `
    -Force -ErrorAction SilentlyContinue
Invoke-Checked "docker" @("compose", "-f", $Compose, "up", "-d")
Start-Sleep -Seconds 8
Invoke-Checked "docker" @(
    "compose", "-f", $Compose, "--profile", "perf", "run", "--rm",
    "loadgen", "-action", "deploy"
)
