# SFChain RQ1: endorsed-transaction entry to successor construction.
# Source preparation is outside the per-record measurement window.
# Usage: powershell -File sfchain_e1g_docker_test.ps1 -BS 400 -Round 1 -Total 20000 -AnchorSuccessor
param(
    [Parameter(Mandatory=$true)][int]$BS,
    [Parameter(Mandatory=$true)][int]$Round,
    [int]$Total = 20000,
    [switch]$AnchorSuccessor,
    [string]$Out = "",
    [string]$StorageDir = ""
)
$ErrorActionPreference = "Continue"
function docker {
    & docker.exe @args
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed: $args" }
}
$base   = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$sfd    = "$base\deploy\sf-docker"
$scripts = "$base\scripts"
$env:BS = "$BS"
$env:SFCHAIN_RQ1 = "1"
$env:ENDORSE_DIRECT = "http://sf-mgmt:9080"
$env:SFCHAIN_RQ1_TOTAL = "$Total"
$env:SFCHAIN_RQ1_RUN_ID = "e1g_r$Round"
$log    = "$base\logs\management_e1g_bs$BS`_r$Round.log"
$MY     = @("exec","sf-mysql","mysql","-uroot","-pqwer@123","-N","-s")
$tables = @("man_blocks_management","man_blocks_development","man_blocks_test","man_blocks_operations",
            "man_headers_management","man_headers_development","man_headers_test","man_headers_operations",
            "dev_blocks","dev_headers_management","dev_headers_development","dev_headers_test","dev_headers_operations",
            "test_blocks","test_headers_management","test_headers_development","test_headers_test","test_headers_operations",
            "ops_blocks","ops_headers_management","ops_headers_development","ops_headers_test","ops_headers_operations",
            "man_blockchain_info","dev_blockchain_info","test_blockchain_info","ops_blockchain_info",
            "processed_transactions")

New-Item -ItemType Directory -Path "$base\logs","$fisco\results" -Force | Out-Null
Write-Output "=== SFChain RQ1 bs=$BS round=$Round ==="
if (Test-Path -LiteralPath $log) { throw "Refusing to overwrite archived round: $log" }
if (-not $Out) { $Out = "$fisco\results\sfchain_e1g_bs$BS`_r$Round`_frozen.json" }
if (Test-Path -LiteralPath $Out) { throw "Refusing to overwrite frozen result: $Out" }

# 0. remove old node containers (keep sf-mysql + its volume)
docker compose -f "$sfd\docker-compose.yml" rm -sf sf-mgmt sf-dev sf-test sf-ops sf-endorse 2>$null | Out-Null

# 1. ensure sf-mysql up + healthy
docker compose -f "$sfd\docker-compose.yml" up -d sf-mysql | Out-Null
$healthy = $false
for ($i = 0; $i -lt 60; $i++) {
    $st = docker inspect --format "{{.State.Health.Status}}" sf-mysql 2>$null
    if ("$st" -eq "healthy") { $healthy = $true; break }
    Start-Sleep -Seconds 2
}
Write-Output "sf-mysql healthy=$healthy"
if (-not $healthy) { Write-Output "sf-mysql not healthy, abort"; exit 1 }

# 2. users check (endorsement needs real ECDSA keys)
$nusers = (& docker $MY -e "SELECT COUNT(*) FROM sfchain.users" 2>$null)
if (-not $nusers) { $nusers = "0" }
Write-Output "users=$nusers"
if ([int]$nusers -lt 1) { Write-Output "sfchain.users empty, abort (seed first)"; exit 1 }

# 3. truncate chain tables + logs + transactions (fresh end-to-end run)
foreach ($t in $tables) { & docker $MY -e "TRUNCATE sfchain.$t" 2>$null | Out-Null }
& docker $MY -e "TRUNCATE sfchain.software_factory_logs" | Out-Null
& docker $MY -e "TRUNCATE sfchain.transactions" | Out-Null
Write-Output "tables cleared (blocks/headers/logs/transactions)"
if ($StorageDir) {
    python "$scripts\storage_report.py" --sfchain-only --sfchain-mysql 127.0.0.1 13306 root qwer@123 sfchain --logs $Total --label sfchain_baseline --out $StorageDir
    if ($LASTEXITCODE -ne 0) { throw "Storage baseline failed" }
}

# 4. set block_size in docker mgmt config
$cfg = "$sfd\configs\management-d.yaml"
$c = [IO.File]::ReadAllText($cfg, [Text.Encoding]::UTF8)
$c = [regex]::Replace($c, "(?m)^  block_size: .*$", "  block_size: $BS")
$c = [regex]::Replace($c, "(?m)^  tps_priority: .*$", "  tps_priority: false")
[IO.File]::WriteAllText($cfg, $c, (New-Object System.Text.UTF8Encoding $false))
Write-Output "config set bs=$BS"

# 5. pre-stage the factory logs (preparation, outside the window)
Write-Output "pre-injecting $Total logs via loadgen container..."
docker run --rm --network sfe1d_default -v "$base\bin:/app/bin:ro" -w /app debian:bookworm-slim ./bin/loadgen -total $Total -rate 0 -host sf-mysql -user root -password "qwer@123"
# The paper's primary endpoint is successor construction. The same trace also
# reports the earlier four-role attestation milestone. Trigger records arrive
# only after business drain, keeping them out of business blocks.
if ($AnchorSuccessor) {
    Write-Output "successor anchoring enabled: dummies will be injected after the business workload drains"
} else {
    Write-Output "attestation control only: no successor triggers"
}
$nlogs = (& docker $MY -e "SELECT COUNT(*) FROM sfchain.software_factory_logs")
Write-Output "staged logs=$nlogs"

# 6. start nodes + endorsement together: the full pipeline (ingest -> endorse -> seal -> consensus) runs end to end
docker compose -f "$sfd\docker-compose.yml" up -d sf-dev sf-test sf-ops 2>$null | Out-Null
Start-Sleep -Seconds 3
docker compose -f "$sfd\docker-compose.yml" up -d sf-mgmt sf-endorse 2>$null | Out-Null
Start-Sleep -Seconds 2
Write-Output "nodes and endorsement service started"

function Count-Pattern([string]$path, [string]$pattern) {
    if (-not (Test-Path $path)) { return 0 }
    $fs = [System.IO.File]::Open($path, "Open", "Read", "ReadWrite")
    $sr = New-Object System.IO.StreamReader($fs, [Text.Encoding]::UTF8)
    $txt = $sr.ReadToEnd(); $sr.Close(); $fs.Close()
    return ([regex]::Matches($txt, [regex]::Escape($pattern))).Count
}

# 7. wait drain: logs pending=0 + endorsed=0 + blocks stable + cons >= seal
#    (compose writes the live log as *_e1d_bsN.log; the e1g_rN archive is created after drain)
$livelog = "$base\logs\management_e1d_bs$BS.log"
$prev = -1; $stable = 0; $drained = $false; $dummyInjected = (-not $AnchorSuccessor)
for ($i = 1; $i -le 240; $i++) {
    Start-Sleep -Milliseconds 250
    $blocks = & docker $MY -e "SELECT (SELECT COUNT(*) FROM sfchain.man_blocks_management)+(SELECT COUNT(*) FROM sfchain.man_blocks_development)+(SELECT COUNT(*) FROM sfchain.man_blocks_test)+(SELECT COUNT(*) FROM sfchain.man_blocks_operations);" 2>$null
    $pend   = & docker $MY -e "SELECT COUNT(*) FROM sfchain.software_factory_logs WHERE processed=0;" 2>$null
    $businessPend = & docker $MY -e "SELECT COUNT(*) FROM sfchain.software_factory_logs WHERE processed=0 AND id NOT LIKE 'anchor-dummy-%';" 2>$null
    $businessBlocked = & docker $MY -e "SELECT COUNT(*) FROM sfchain.transactions t JOIN sfchain.software_factory_logs l ON l.tx_id=t.tx_id WHERE l.id NOT LIKE 'anchor-dummy-%' AND t.status='blocked';" 2>$null
    $endor  = & docker $MY -e "SELECT COUNT(*) FROM sfchain.transactions WHERE status='endorsed';" 2>$null
    $pendtx = & docker $MY -e "SELECT COUNT(*) FROM sfchain.transactions WHERE status!='endorsed';" 2>$null
    $sealN  = Count-Pattern $livelog "block created successfully, txType"
    $consN  = Count-Pattern $livelog "consensus completed, height="
    # Wait until the business workload has left the source table and no
    # endorsed transaction remains in the packaging pool.  Only then add one
    # protocol-only transaction per chain, guaranteeing a successor block.
    if ($AnchorSuccessor -and -not $dummyInjected -and "$businessBlocked" -eq "$Total" -and "$businessPend" -eq "0" -and "$endor" -eq "0" -and $sealN -gt 0 -and $consN -ge $sealN) {
        & docker $MY -e "INSERT INTO sfchain.software_factory_logs (id, category, timestamp, level, message, user_id, module, project, operation, status, processed) VALUES ('anchor-dummy-management','management',ROUND(UNIX_TIMESTAMP(NOW(3))*1000),'INFO','anchor trigger dummy','admin','factory','p1','seal','done',0), ('anchor-dummy-development','development',ROUND(UNIX_TIMESTAMP(NOW(3))*1000),'INFO','anchor trigger dummy','dev1','factory','p1','seal','done',0), ('anchor-dummy-test','test',ROUND(UNIX_TIMESTAMP(NOW(3))*1000),'INFO','anchor trigger dummy','tester1','factory','p1','seal','done',0), ('anchor-dummy-operations','operations',ROUND(UNIX_TIMESTAMP(NOW(3))*1000),'INFO','anchor trigger dummy','ops1','factory','p1','seal','done',0)" 2>$null | Out-Null
        $dummyInjected = $true
        Write-Output "[anchor-trigger] business drained; injected 4 successor dummies"
    }
    $completionOK = $false
    if (-not $AnchorSuccessor) {
        # The four-role attestation endpoint is complete once every business
        # record has been sealed and consensus has caught up.
        $completionOK = ("$businessBlocked" -eq "$Total" -and "$businessPend" -eq "0" -and "$endor" -eq "0")
    }
    if ("$blocks" -eq "$prev") {
        $stable++
        # In direct-push mode (ENDORSE_DIRECT) the endorsed count relies on the
        # asynchronous blocked flag, so it is not required to reach zero:
        # judge by logs drained + stable block count + consensus caught up, and
        # lengthen the stability wait so in-pool transactions get sealed
        # The post-drain trigger records must also be consumed before shutdown.
        # The analyzer below checks successor coverage for every business record.
        $needStable = 2
        if ($env:ENDORSE_DIRECT) { $needStable = 5 }
        $endorOK = ("$endor" -eq "0") -or $env:ENDORSE_DIRECT
        if ((($AnchorSuccessor -and $dummyInjected) -or (-not $AnchorSuccessor)) -and
            $sealN -gt 0 -and $consN -ge $sealN -and
            (($stable -ge $needStable) -or $completionOK) -and
            "$pend" -eq "0" -and "$businessPend" -eq "0" -and "$businessBlocked" -eq "$Total" -and $endorOK) {
            Write-Output "[drained] blocks=$blocks pending_logs=$pend endorsed=$endor seals=$sealN cons=$consN (i=$i)"
            $drained = $true; break
        }
        if ($dummyInjected -and $stable -ge 120) {
            Write-Output "[drained-timeout] blocks=$blocks pending_logs=$pend endorsed=$endor pendtx=$pendtx seals=$sealN cons=$consN (i=$i)"
            break
        }
    } else { $stable = 0 }
    $prev = "$blocks"
}
if (-not $drained) { Write-Output "[loop-timeout] blocks=$prev" }

# 7.5 Check trigger consumption. The analyzer separately verifies successor
# coverage for every business record before declaring this round complete.
if ($AnchorSuccessor) {
    $anchorCheck = & docker $MY -e "SELECT COUNT(*) FROM sfchain.software_factory_logs WHERE id LIKE 'anchor-dummy-%' AND processed=1;" 2>$null
    if ("$anchorCheck" -ne "4") {
        Write-Output "[anchor-warn] dummy consumed=$anchorCheck (expected 4) — check anchoring completeness"
    } else {
        Write-Output "[anchored] all 4 dummy txs consumed (anchor blocks sealed within drain)"
    }
}

# 8. stop nodes (keep sf-mysql for analysis)
Start-Sleep -Seconds 3
docker compose -f "$sfd\docker-compose.yml" rm -sf sf-mgmt sf-dev sf-test sf-ops sf-endorse 2>$null | Out-Null
Start-Sleep -Seconds 2

# 8.5 archive logs under e1g round names (compose writes *_e1d_bsN.log)
foreach ($node in "management","development","test-node","operations") {
    $src = "$base\logs\${node}_e1d_bs$BS.log"
    $dst = "$base\logs\${node}_e1g_bs$BS`_r$Round.log"
    if (Test-Path $src) { Copy-Item $src $dst -Force; Write-Output "archived: ${node}_e1g_bs$BS`_r$Round.log" }
}

# 9. summary
if (Test-Path $log) {
    $sealN = Count-Pattern $log "block created successfully, txType"
    $consN = Count-Pattern $log "consensus completed, height="
    Write-Output "=== bs=$BS round=$Round done: seals=$sealN cons=$consN ==="
} else {
    Write-Output "=== bs=$BS round=$Round done: MANAGEMENT LOG MISSING ==="
}

# Freeze NOW, not after another round has truncated this database.
$required = "attestation"
if ($AnchorSuccessor) { $required = "anchored" }
python "$scripts\sfchain\analyze_rq1.py" "r$Round" --bs $BS --total $Total --log $log --out $Out --require $required
if ($LASTEXITCODE -ne 0) { throw "Invalid round r$Round; retained output/logs for diagnosis" }
if ($StorageDir) {
    python "$scripts\storage_report.py" --sfchain-only --sfchain-mysql 127.0.0.1 13306 root qwer@123 sfchain --logs $Total --label sfchain_after --out $StorageDir --baseline "$StorageDir\storage_sfchain_baseline.json"
    if ($LASTEXITCODE -ne 0) { throw "Storage after measurement failed" }
}
