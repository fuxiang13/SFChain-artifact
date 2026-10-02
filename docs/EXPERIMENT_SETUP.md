# Experiment setup

Commands assume a generated working copy with the same directory structure as
the source artifact; see `README.md`. Reset scripts delete experiment state.

## 1. Reported testbeds

RQ1 uses an Intel Core i7-12700H host, 32 GB RAM, and an NVMe SSD with Windows
11, WSL2, and Docker. SFChain has four role nodes; FISCO BCOS v3.17.1 has four
PBFT nodes; Fabric v3.1.5 has four organization peers and four SmartBFT
orderers. The SFChain source requires Go 1.26 or newer. Analysis requires
Python 3.12 or newer and `pymysql` for MySQL storage measurements.

RQ2 uses a Management server (Xeon Platinum 8358P, 754 GB RAM, MySQL 5.7.44)
and three role workstations (Windows 10, Core i7, 16 GB RAM, MySQL 8.0.39)
on an enterprise LAN. The supplied loopback configurations are for local
execution; use the corresponding private node and database addresses to
recreate a multi-host placement. Local results are not measurements of that
LAN testbed. RQ3 runs native processes; RQ4 reads retained chains.

## 2. Baseline setup

After building SFChain and fetching dependencies, install analysis dependencies:

```powershell
python -m pip install pymysql
```

### FISCO BCOS

From `baseline/fiscobcos/`, use Linux/WSL or Git Bash:

```bash
bash scripts/generate_nodes.sh
docker build -f node.Dockerfile -t fisco-bcos-node:v3.17.1 .
docker build -f loadgen/Dockerfile -t fisco-loadgen .
```

The campaign invokes `scripts/reset_fisco_rq1.ps1 -BlockLimit 400` from the
workspace root. It deploys the checked evidence contract after each reset.
The ingest command uses `-poll 200ms -threshold 1000 -pacing 0
-endorse-evidence=1 -endorse-persist=1 -endorse-interval 200ms -endorse-check=1`
and the shared source MySQL on port 13306. Evidence construction, signing,
storage, and readback precede `submitted_ms`. Receipt recovery retains its
actual observation time; no record is dropped from the latency distribution.

### Fabric

The dependency fetcher extracts the Fabric distribution into
`baseline/fabric/`. From the workspace root:

```powershell
docker build -f baseline/fabric/images/Dockerfile.fabric -t sf/fabric:3.1.5 baseline/fabric
docker build -f baseline/fabric/images/Dockerfile.fabric-go -t sf/fabric-go:3.1.5 baseline/fabric
$fab = (Resolve-Path baseline/fabric).Path
docker run --rm -v "${fab}/net:/etc/hyperledger/net" sf/fabric:3.1.5 cryptogen generate --config=/etc/hyperledger/net/crypto-config.yaml --output=/etc/hyperledger/net/organizations
docker run --rm -v "${fab}:/work" -w /work/loadgen golang:1.26 go build -trimpath -buildvcs=false -o fabricload .
```

The campaign initializes the channel and calls `deploycc_role.py`.
`sflogs-m/d/t/o` respectively require Org1/2/3/4's peer. Each policy names
one organization; the `OR(...)` syntax with one argument is not an
any-organization policy. Fabric adds no responsible-user ECDSA signature.
The load generator uses 128 submission workers and 128 commit waiters.

## 3. RQ1 timing and storage

Each three-platform round shares one 20,000-row source snapshot, with 5,000
records per category. `loadgen` generates source records; signatures are
created during execution. Source preparation is outside the per-record
clock. Start fields and completion events are listed in `README.md`.
Throughput is `N / (last completion - first entry)`.

Run `scripts/run_rq1_frozen.ps1 -Rounds 5 -Total 20000` for timing. SFChain
and FISCO reset each round. Fabric resets once, then appends across timing
rounds. Each SFChain run injects its four trigger records only after business
drain. Its analyzer requires every business record to have a timestamp,
a matching persisted block, and the requested completion event.

`-MeasureStorage` selects the separate fresh-ledger storage campaign. It
measures baseline and final bytes in SFChain block/header tables, all four
FISCO RocksDB directories, and all Fabric peer/orderer ledger directories.
Input rows, transaction pools, images, runtime logs, keys, and chaincode
build caches are excluded. Trigger records remain included. Use decimal MB
(`bytes / 1,000,000`), not the human-readable binary-unit display.

The RQ1 round driver invokes `scripts/sfchain/analyze_rq1.py` with an explicit
output path.
Do not re-analyze a previous round's log against a subsequent database.
`aggregate_frozen_rq1.py` reads only the saved summaries and fails on
incomplete counts, invalid endpoints, failures, or non-finite metrics.

## 4. RQ2 block-processing study

The paper varies block size over 128, 256, 512, and 1,024. Its measurement
runs from block-build start to four-role attestation completion; endorsement,
source reading, and successor anchoring are excluded.

The two RQ2 configurations use the same pre-endorsed workload, native helper,
node configuration, block sizes, DTO behavior, timing boundary, and analyzer.
The A/B switch is deliberately applied only at Management:

| Configuration | Management environment | RQ2 meaning |
|---|---|---|
| Configuration B | `SFCHAIN_SYNC_BLOCK_PERSIST` unset or not equal to `1` | Management enqueues block-custody writes to its asynchronous persistence worker (default) |
| Configuration A | `SFCHAIN_SYNC_BLOCK_PERSIST=1` | Management executes the block-custody write synchronously before continuing the Management path |

DTO nodes do not package transactions in this study. Management stages and
packages the pre-endorsed transactions; DTO nodes receive the resulting block
or header, validate it, retain their role evidence through the common DTO
path, and return their role signatures. Therefore the A/B comparison holds
transaction packaging, role witnessing, DTO processing, network behavior, and
the workload fixed, and changes only the Management-side block-custody
scheduling.

### Shared preparation

Build the native binaries and initialize an isolated test database as
described above. With all nodes stopped, create the pre-endorsed RQ2 pool:

```bash
bin/preparepool \
  -dsn 'root:qwer@123@tcp(127.0.0.1:3306)/sfchain' \
  -total <RQ2-total-divisible-by-four>
```

Use the same total and the same staged pool for the A/B runs in a round. The
preparation is outside the measured interval. The native helper clears block
and header tables before each run and resets the transaction rows to
`endorsed`, so the same staged rows can be reused for the paired run. If a
fresh workload is desired for another round, run `preparepool` again before
that round. Never point these commands at a production database.

### Shared run procedure

For each configuration and each block size in `{128, 256, 512, 1,024}`:

1. Stop any previous native nodes and ensure the dedicated database is
   reachable.
2. Set or unset `SFCHAIN_SYNC_BLOCK_PERSIST` according to the table above.
3. Run `scripts/sfchain/sfchain_e1_test.sh <block-size> <run-tag>`. It starts the three DTO
   processes first, starts Management last so that the pre-endorsed
   Management packaging pool is ready, waits for all chains to drain and for
   attestation events to catch up, then stops the processes and retains the
   logs.
4. Use a fresh alphanumeric run tag for every configuration, block size, and
   repetition; the helper refuses to overwrite an existing Management log.
5. Analyze the resulting Management log with `analyze_rq2.py`.

Configuration B uses the shared default:

```bash
unset SFCHAIN_SYNC_BLOCK_PERSIST
bash scripts/sfchain/sfchain_e1_test.sh 128 rq2_B_bs128_r1
```

Configuration A uses the same helper and workload, with only the
Management-side switch changed:

```bash
SFCHAIN_SYNC_BLOCK_PERSIST=1 \
  bash scripts/sfchain/sfchain_e1_test.sh 128 rq2_A_bs128_r1
```

Repeat these commands for block sizes `256`, `512`, and `1024`, and use
distinct tags for additional repetitions. The environment variable must be
set before the helper starts Management; changing it after the processes have
started does not change an ongoing run. For example:

```bash
python3 scripts/sfchain/analyze_rq2.py \
  logs/management_rq2_B_bs128_r1.log \
  --out runs/rq2_B_bs128_r1.json

python3 scripts/sfchain/analyze_rq2.py \
  logs/management_rq2_A_bs128_r1.log \
  --out runs/rq2_A_bs128_r1.json
```

The analyzer requires matched build/count/seal/attestation events and counts
transactions, not blocks, when calculating TPS. It reports each input round
separately. Throughput is the total transaction count divided by the
Management-side window from the first block-build start to the last four-role
attestation completion. Mean and p50 block latency use matched per-block
start and attestation events. Incomplete or unmatched event sets are rejected
instead of being silently treated as complete results.

The supplied commands reproduce the RQ2 procedure and both
Management-side persistence modes. They generate new outputs under the
selected environment; the artifact does not contain the historical logs used
for the paper's reported values. For a multi-host enterprise-LAN deployment,
perform the same preparation and launch steps on the corresponding hosts,
collect the Management logs in one analysis workspace, and preserve the same
configuration variable and run-tag mapping.

## 5. RQ3 fault injection

Use a dedicated native MySQL containing the schema in
`prototype/mysql_schema.sql`. Initialize identities with `bin/seedusers`
using the native database DSN. With all nodes stopped, stage the paper's
pre-endorsed workload:

```bash
bin/preparepool -dsn 'root:qwer@123@tcp(127.0.0.1:3306)/sfchain' -total $((4*200*5))
```

This is the RQ3 workload (four roles, five 200-record blocks per role), not
an alternative RQ1 workload. For each scenario below run
`bash scripts/sfchain/rq3_run.sh <scenario> <round>` for rounds 1, 2, and 3,
then `python3 scripts/sfchain/rq3_analyze.py`.
Run native processes in one timezone. The proxy includes the date in its
resume log, so analysis uses the date of each run.

| Scenario | Injection |
|---|---|
| `base` | none, block size 200 |
| `d100`, `d500`, `d1000` | delay Management-to-Development requests by 100/500/1,000 ms |
| `dual500` | delay requests to Development and Testing by 500 ms each |
| `bs100`, `bs400`, `bs500` | the 500-ms Development delay at the named block size |
| `disc` | queue Development requests for 10 seconds, then resume |
| `refuse` | Development refuses to sign for the 40-second observation window |

Faults act on the proxy request path, not independently on both directions.
All required signatures remain mandatory. The paper reports a residual
reconnection tail, not complete fault masking.

## 6. RQ4 audit input and verification

RQ4 uses TXID-based responsible-user signatures, block size 100, and ten
blocks per chain. Do not generate its input with the RQ1 driver, which uses
a different endorsement digest and workload size.

With native nodes stopped and the native database initialized, run:

```bash
bin/preparepool -dsn 'root:qwer@123@tcp(127.0.0.1:3306)/sfchain' -total $((4*100*10))
SFCHAIN_PACK_TICK=1s bash scripts/sfchain/sfchain_e1_test.sh 100 audit
bin/auditverify -host 127.0.0.1:3306 -user root -pass 'qwer@123' -config prototype/configs/management-node.yaml -chain management -header-source development -iters 200
```

Check the tool's output for ten retained headers and a 100-record target
block. If the run cuts smaller blocks, it is not the paper's stated audit
input. Choose a target with a retained successor (the default selects the
second-highest block).

The auditor uses public keys, the custodian payload, and independently
retained role headers. It rejects missing successor evidence and inconsistent
payload/header bindings. It does not load role private keys.

Per-step timings cover hashing, Merkle recomputation, ECDSA, header walking,
and BLS. The reported 1.19-ms median is the paper's preloaded five-step
kernel. The tool's `full_audit` also performs binding checks and must not be
labelled a timing-identical reproduction of that kernel. Retrieval, decoding,
and key preparation remain outside the timed steps.
