[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$Base = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$FabricDir = Join-Path $Base "baseline\fabric"
$NetDir = Join-Path $FabricDir "net"
$Compose = Join-Path $NetDir "docker-compose.yml"
$ChannelBlock = Join-Path $NetDir "channel-artifacts\sfchannel.block"

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

function Docker-Tool {
    param([string[]]$ArgumentList)
    Invoke-Checked "docker" (@(
        "run", "--rm", "--network", "fabric_bft",
        "-v", "$FabricDir`:/work",
        "-v", "$NetDir`:/etc/hyperledger/net",
        "sf/fabric:3.1.5"
    ) + $ArgumentList)
}

Invoke-Checked "docker" @("compose", "-f", $Compose, "down", "-v", "--remove-orphans")
Remove-Item -LiteralPath $ChannelBlock -Force -ErrorAction SilentlyContinue
$networkNames = & docker network ls --format "{{.Name}}" 2>$null
if ($networkNames -notcontains "fabric_bft") {
    Invoke-Checked "docker" @("network", "create", "fabric_bft")
}

Docker-Tool @(
    "configtxgen",
    "-configPath", "/etc/hyperledger/net",
    "-profile", "ChannelUsingBFT",
    "-channelID", "sfchannel",
    "-outputBlock", "/etc/hyperledger/net/channel-artifacts/sfchannel.block"
)

Invoke-Checked "docker" @("compose", "-f", $Compose, "up", "-d")
Start-Sleep -Seconds 8

$ordererCA = "/etc/hyperledger/net/organizations/ordererOrganizations/example.com/orderers/{0}/tls/ca.crt"
$ordererCert = "/etc/hyperledger/net/organizations/ordererOrganizations/example.com/orderers/{0}/tls/server.crt"
$ordererKey = "/etc/hyperledger/net/organizations/ordererOrganizations/example.com/orderers/{0}/tls/server.key"
$orderers = @(
    @{ Name = "orderer1.example.com"; Port = 7053 },
    @{ Name = "orderer2.example.com"; Port = 7055 },
    @{ Name = "orderer3.example.com"; Port = 7059 },
    @{ Name = "orderer4.example.com"; Port = 7061 }
)
foreach ($orderer in $orderers) {
    $name = $orderer.Name
    Docker-Tool @(
        "osnadmin", "channel", "join",
        "--channelID", "sfchannel",
        "--config-block", "/etc/hyperledger/net/channel-artifacts/sfchannel.block",
        "-o", "$name`:$($orderer.Port)",
        "--ca-file", ($ordererCA -f $name),
        "--client-cert", ($ordererCert -f $name),
        "--client-key", ($ordererKey -f $name)
    )
}

for ($n = 1; $n -le 4; $n++) {
    $org = "org$n"
    $msp = "/etc/hyperledger/net/organizations/peerOrganizations/$org.example.com/users/Admin@$org.example.com/msp"
    $peerTLS = "/etc/hyperledger/net/organizations/peerOrganizations/$org.example.com/peers/peer0.$org.example.com/tls/ca.crt"
    Invoke-Checked "docker" (@(
        "run", "--rm", "--network", "fabric_bft",
        "-v", "$FabricDir`:/work",
        "-v", "$NetDir`:/etc/hyperledger/net",
        "-e", "CORE_PEER_LOCALMSPID=Org${n}MSP",
        "-e", "CORE_PEER_MSPCONFIGPATH=$msp",
        "-e", "CORE_PEER_ADDRESS=peer0.$org.example.com:7051",
        "-e", "CORE_PEER_TLS_ENABLED=true",
        "-e", "CORE_PEER_TLS_ROOTCERT_FILE=$peerTLS",
        "sf/fabric:3.1.5",
        "peer", "channel", "join",
        "-b", "/etc/hyperledger/net/channel-artifacts/sfchannel.block"
    ))
}

Invoke-Checked "python" @(Join-Path $FabricDir "scripts\deploycc_role.py")
