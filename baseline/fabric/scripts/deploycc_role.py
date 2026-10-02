# -*- coding: utf-8 -*-
"""Deploy 4 instances of the sflogs chaincode, each with a role-specific
endorsement policy: sflogs-m requires Org1, sflogs-d requires Org2, etc.
This routes each category to its responsible organization peer; it does not
add a source-user ECDSA signature."""
import os, re, subprocess, sys

FAB = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
NET = os.path.join(FAB, "net")
IMG = "sf/fabric:3.1.5"
CHANNEL = "sfchannel"
VER = "1.0"
SEQ = "1"
ORDERER = "orderer1.example.com:7050"
OCA = "/etc/hyperledger/net/organizations/ordererOrganizations/example.com/orderers/orderer1.example.com/tls/ca.crt"

# role → (chaincode name, org index, policy)
ROLES = [
    ("sflogs-m", 1, "OR('Org1MSP.peer')"),   # management → only Org1
    ("sflogs-d", 2, "OR('Org2MSP.peer')"),   # development → only Org2
    ("sflogs-t", 3, "OR('Org3MSP.peer')"),   # testing → only Org3
    ("sflogs-o", 4, "OR('Org4MSP.peer')"),   # operations → only Org4
]

def run(mounts, env, args, must=True):
    cmd = ["docker", "run", "--rm", "--network", "fabric_bft"] + mounts + env + [IMG] + args
    r = subprocess.run(cmd, capture_output=True, text=True)
    if must and r.returncode != 0:
        print("FAILED:", " ".join(args)[:150])
        print(r.stdout[-600:])
        print(r.stderr[-600:])
        sys.exit(1)
    return r.stdout + r.stderr

NETM = ["-v", NET + ":/etc/hyperledger/net"]

def org_env(n):
    org = "org%d" % n
    return ["-e", "CORE_PEER_LOCALMSPID=Org%dMSP" % n,
            "-e", "CORE_PEER_MSPCONFIGPATH=/etc/hyperledger/net/organizations/peerOrganizations/%s.example.com/users/Admin@%s.example.com/msp" % (org, org),
            "-e", "CORE_PEER_ADDRESS=peer0.%s.example.com:7051" % org,
            "-e", "CORE_PEER_TLS_ENABLED=true",
            "-e", "CORE_PEER_TLS_ROOTCERT_FILE=/etc/hyperledger/net/organizations/peerOrganizations/%s.example.com/peers/peer0.%s.example.com/tls/ca.crt" % (org, org)]

# 1. Package once (same code, same package ID for all 4 instances)
os.makedirs(os.path.join(FAB, "ccpkg"), exist_ok=True)
print("== package ==")
subprocess.run(["docker", "run", "--rm", "-v", FAB + ":/work", "-v", FAB + "/bin:/usr/local/bin",
                "-e", "FABRIC_CFG_PATH=/work/config", "-e", "GOFLAGS=-mod=vendor",
                "sf/ccenv:local",
                "peer", "lifecycle", "chaincode", "package", "/work/ccpkg/sflogs.tar.gz",
                "--path", "/work/chaincode/sflogs", "--lang", "golang", "--label", "sflogs_1"],
               check=True)

# 2. Install once per org (same package reused by all 4 definitions)
for n in (1, 2, 3, 4):
    print("== install org%d ==" % n)
    out = run(NETM + ["-v", FAB + "/ccpkg:/ccpkg"], org_env(n),
              ["peer", "lifecycle", "chaincode", "install", "/ccpkg/sflogs.tar.gz"], must=False)
    if "already successfully installed" in out:
        print("   already installed")

qi = run(NETM, org_env(1), ["peer", "lifecycle", "chaincode", "queryinstalled", "--output", "json"])
pkg_id = re.search(r'"package_id":\s*"([^"]+)"', qi).group(1)
print("package_id:", pkg_id)

# 3. For each role-specific instance: approve (all 4 orgs) + commit
for cc_name, org_n, policy in ROLES:
    print("== %s (policy: %s) ==" % (cc_name, policy))
    for n in (1, 2, 3, 4):
        run(NETM, org_env(n),
            ["peer", "lifecycle", "chaincode", "approveformyorg", "-o", ORDERER,
             "--tls", "--cafile", OCA,
             "--channelID", CHANNEL, "--name", cc_name, "--version", VER,
             "--package-id", pkg_id, "--sequence", SEQ,
             "--signature-policy", policy])

    peers_args = []
    for n in (1, 2, 3, 4):
        org = "org%d" % n
        peers_args += ["--peerAddresses", "peer0.%s.example.com:7051" % org,
                       "--tlsRootCertFiles", "/etc/hyperledger/net/organizations/peerOrganizations/%s.example.com/peers/peer0.%s.example.com/tls/ca.crt" % (org, org)]
    run(NETM, org_env(1),
        ["peer", "lifecycle", "chaincode", "commit", "-o", ORDERER,
         "--tls", "--cafile", OCA,
         "--channelID", CHANNEL, "--name", cc_name, "--version", VER,
         "--sequence", SEQ, "--signature-policy", policy] + peers_args)
    print("   %s committed" % cc_name)

print("== verify ==")
qcc = run(NETM, org_env(1), ["peer", "lifecycle", "chaincode", "querycommitted",
                             "--channelID", CHANNEL, "--output", "json"])
print(qcc[:600])
print("role deploy done")
