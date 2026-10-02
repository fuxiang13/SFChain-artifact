# Sequential RQ1 campaign. Fabric appends across timing rounds; storage rounds
# reset every platform before taking baseline-subtracted measurements.
# WARNING: resets ONLY the dedicated sfchain/fisco/Fabric experiment state.
[CmdletBinding()]
param(
    [int]$Rounds = 5,
    [int]$FirstSFChainRound = 1,
    [int]$Total = 20000,
    [string]$RunRoot = "",
    [switch]$MeasureStorage
)
$ErrorActionPreference = "Continue" # native tools write progress to stderr
$Base = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$SFDir = Join-Path $Base "deploy\sf-docker"
$FiscoDir = Join-Path $Base "baseline\fiscobcos"
$FabricDir = Join-Path $Base "baseline\fabric"
$env:BS="400"
foreach($name in @("SFCHAIN_RQ1","SFCHAIN_RQ1_DEBUG","ENDORSE_DIRECT","PACK_TICK",
                   "LOG_POLL","LOG_THRESHOLD","LOG_PACING")) {
    Remove-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue
}
if (-not $RunRoot) { $RunRoot = Join-Path $Base ("runs\rq1-v2-" + (Get-Date -Format "yyyyMMdd-HHmmss")) }
if (Test-Path -LiteralPath $RunRoot) { throw "Campaign directory already exists: $RunRoot" }
New-Item -ItemType Directory -Path $RunRoot -Force | Out-Null
function Checked([string]$Exe, [string[]]$Arguments) {
    & $Exe @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Failed ($LASTEXITCODE): $Exe $($Arguments -join ' ')" }
}
function Stop-Platforms {
    Checked "docker" @("compose","-f","$SFDir\docker-compose.yml","stop","sf-mgmt","sf-dev","sf-test","sf-ops","sf-endorse")
    Checked "docker" @("compose","-f","$FiscoDir\docker-compose.yml","stop","node0","node1","node2","node3")
    Checked "docker" @("compose","-f","$FabricDir\net\docker-compose.yml","stop")
}
function Reset-Source {
    Checked "docker" @("exec","sf-mysql","mysql","-uroot","-pqwer@123","-e",
        "DELETE FROM sfchain.software_factory_logs WHERE id LIKE 'anchor-dummy-%'; UPDATE sfchain.software_factory_logs SET processed=0,tx_id=NULL;")
}
function Storage([string]$Platform,[string]$Stage,[string]$Dir) {
    if (-not $MeasureStorage) { return }
    $mode=$Platform
    if($mode -eq "fiscobcos"){$mode="fisco"}
    $a=@("$FiscoDir\scripts\storage_report.py","--$mode-only",
        "--sfchain-mysql","127.0.0.1","13306","root","qwer@123","sfchain",
        "--logs","$Total","--label","${mode}_$Stage","--out",$Dir)
    if($Stage -eq "after") { $a+=@("--baseline","$Dir\storage_${mode}_baseline.json") }
    Checked "python" $a
}
$manifest = @{
    protocol="pipeline-entry-v2"; rounds=$Rounds; total=$Total
    sfchain_endpoint="successor header construction at Management; asynchronous custody excluded"
    sfchain_control="four-role aggregate construction; paired in same round"
    sfchain_bs_per_role=400; fisco_bs=400; fabric_max_message_count=400
    fresh_ledgers=@{ sfchain=$true; fiscobcos=$true; fabric=[bool]$MeasureStorage }
    concurrency="native pipelines; Fabric 128 submit and 128 commit-wait workers"
    trigger="four successor records AFTER business drain; driver polls every 250 ms plus SQL query time"
    source="same staged business rows within each three-platform round"
    aggregation="median per metric across all complete planned rounds; no silent exclusions"
    entry="SF: endorsements[0].timestamp; FISCO: submitted_ms; Fabric: submitTs"
    started=(Get-Date).ToUniversalTime().ToString("o")
    background_containers=(& docker ps --format "{{.Names}}")
    binaries=@{
        sfchain=(Get-FileHash "$Base\bin\sfchain" -Algorithm SHA256).Hash
        endorsement_service=(Get-FileHash "$Base\bin\endorsement_service" -Algorithm SHA256).Hash
        fabricload=(Get-FileHash "$FabricDir\loadgen\fabricload" -Algorithm SHA256).Hash
        fisco_image=(& docker image inspect fisco-loadgen:latest --format "{{.Id}}")
    }
}
$manifest | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 "$RunRoot\protocol.json"
try {
    for ($r=1; $r -le $Rounds; $r++) {
        $dir=Join-Path $RunRoot ("round_{0:d2}" -f $r)
        foreach($p in @("sfchain","fiscobcos","fabric")) {
            New-Item -ItemType Directory -Path "$dir\$p" -Force | Out-Null
        }
        Stop-Platforms
        $sfArgs=@("-NoProfile","-ExecutionPolicy","Bypass","-File",
            "$SFDir\sfchain_e1g_docker_test.ps1","-BS","400",
            "-Round","$($FirstSFChainRound+$r-1)","-Total","$Total","-AnchorSuccessor","-Out","$dir\sfchain\result.json")
        if($MeasureStorage){$sfArgs+=@("-StorageDir","$dir\sfchain")}
        Checked "powershell" $sfArgs
        Reset-Source
        Checked "powershell" @("-NoProfile","-ExecutionPolicy","Bypass","-File",
            "$Base\scripts\reset_fisco_rq1.ps1","-BlockLimit","400")
        Storage "fiscobcos" "baseline" "$dir\fiscobcos"
        $before=@(Get-ChildItem "$FiscoDir\results\fisco_ingest_*.csv" | ForEach-Object {$_.FullName})
        Checked "docker" @("compose","-f","$FiscoDir\docker-compose.yml","--profile","perf",
            "run","--rm","loadgen","-action","ingest","-mysql-port","13306",
            "-poll","200ms","-threshold","1000","-pacing","0",
            "-endorse-evidence=1","-endorse-persist=1","-endorse-interval","200ms",
            "-endorse-check=1")
        $created=@(Get-ChildItem "$FiscoDir\results\fisco_ingest_*.csv" | Where-Object {$_.FullName -notin $before})
        if ($created.Count -ne 1) { throw "Expected exactly one new CSV, found $($created.Count)" }
        Copy-Item -LiteralPath $created[0].FullName -Destination "$dir\fiscobcos\trace.csv"
        Checked "python" @("$Base\scripts\normalize_fisco_rq1.py","--csv",
            "$dir\fiscobcos\trace.csv","--total","$Total","--out","$dir\fiscobcos\result.json")
        Storage "fiscobcos" "after" "$dir\fiscobcos"
        Stop-Platforms
        Reset-Source
        if ($r -eq 1 -or $MeasureStorage) {
            Checked "powershell" @("-NoProfile","-ExecutionPolicy","Bypass","-File",
                "$Base\scripts\reset_fabric_rq1.ps1")
        } else {
            Checked "docker" @("compose","-f","$FabricDir\net\docker-compose.yml","start")
            Start-Sleep -Seconds 8
        }
        Storage "fabric" "baseline" "$dir\fabric"
        $fabricOut="rq1_v2_r${r}_$([guid]::NewGuid().ToString('N')).json"
        Checked "docker" @("run","--rm","--network","fabric_bft",
            "-v","$FabricDir`:/work","-v","$FabricDir\net`:/etc/hyperledger/net:ro",
            "-e","SOURCE=mysql","-e","MODE=openloop","-e","TOTAL=$Total","-e","CONC=128",
            "-e","OUT=/work/results/$fabricOut",
            "-e","MYSQL_DSN=root:qwer@123@tcp(host.docker.internal:13306)/sfchain?parseTime=true",
            "sf/fabric-go:3.1.5","/work/loadgen/fabricload")
        Copy-Item -LiteralPath "$FabricDir\results\$fabricOut" -Destination "$dir\fabric\measurement.json"
        Checked "python" @("$Base\scripts\normalize_fabric_rq1.py","--input",
            "$dir\fabric\measurement.json","--total","$Total","--out","$dir\fabric\result.json")
        Storage "fabric" "after" "$dir\fabric"
        Write-Output "=== FROZEN THREE-PLATFORM ROUND $r/$Rounds ==="
    }
    Checked "python" @("$Base\scripts\aggregate_frozen_rq1.py",$RunRoot,"--total","$Total","--endpoint","anchored")
    Checked "python" @("$Base\scripts\aggregate_frozen_rq1.py",$RunRoot,"--total","$Total","--endpoint","attestation","--out","$RunRoot\attestation_control.json")
} finally {
    Stop-Platforms
}
