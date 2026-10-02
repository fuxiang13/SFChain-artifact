# SFChain artifact

Source code and experiment procedures for **SFChain: Dependable Execution-Log
Attestation for Role-Structured Software Factory Platforms**.

The package covers the paper's RQ1 comparison and storage measurements, RQ2
block-processing study, RQ3 fault injection, and RQ4 audit. It contains no
measured result files or prebuilt binaries. Workload generators create local
inputs; they do not recover the exact inputs or traces of the reported runs.

## Prepare a working copy

Run this command from the artifact directory. The destination must be new and
outside this directory. All remaining commands run in that working copy.

```powershell
python -B scripts/materialize_workspace.py ../sfchain-workspace
Set-Location ../sfchain-workspace
$root = (Get-Location).Path
# Linux binaries; Go tests run before building.
docker run --rm -v "${root}:/app" -w /app golang:1.26 bash scripts/build_linux.sh
python -B scripts/fetch_dependencies.py
```

Use a dedicated test environment: the reset scripts clear the experiment
ledgers and database tables. They must not be pointed at a production database.
The supplied role keys and passwords are public test credentials.

Start and initialize the source database:

```powershell
docker compose -f deploy/sf-docker/docker-compose.yml up -d sf-mysql
# Wait for the container's health status to become healthy.
docker run --rm --network sfe1d_default -v "${root}/bin:/app/bin:ro" -w /app debian:bookworm-slim ./bin/seedusers
```

Build and initialize the two baselines as described in
`docs/EXPERIMENT_SETUP.md`, then run:

```powershell
# Timing: SFChain/FISCO reset each round; Fabric appends after its first reset.
powershell -File scripts/run_rq1_frozen.ps1 -Rounds 5 -Total 20000
# Separate storage campaign: every platform resets before each baseline.
powershell -File scripts/run_rq1_frozen.ps1 -Rounds 5 -Total 20000 -FirstSFChainRound 6 -MeasureStorage
```

The driver freezes each round before its database is reused. Incomplete or
failed summaries stop aggregation; they are not silently removed. Results,
logs, generated certificates, downloaded dependencies, and binaries stay in
the working copy. Submit the source artifact, not this generated workspace.

## Measurement boundary

| Platform | Per-record entry | Completion |
|---|---|---|
| SFChain | `endorsements[0].timestamp`, assigned after the responsible-user signature is constructed | Successor-header construction at Management; later custody writes excluded |
| FISCO BCOS | `submitted_ms`, immediately before SDK submission | Successful receipt observation, including recovery time if a callback is recovered |
| Fabric | Worker `submitTs`, immediately before `SubmitAsync` | Successful commit confirmation |

These are the paper's post-admission native-processing entry events. SFChain's
raw `Transaction.Timestamp` is created earlier by Management and is not used
as a fallback measurement start. SFChain and FISCO carry responsible-user
ECDSA evidence. Fabric instead requires the organization peer associated with
the record category to endorse it.

SFChain adds one successor-trigger record per chain **after** the business
workload drains. These four records are excluded from the 20,000-record timing
count and included in the physical-storage footprint. The paired attestation
milestone is also reported, as in the paper.

## Contents

- `prototype/`: SFChain, workload and identity generators, fault proxy, and audit tool.
- `deploy/sf-docker/`: SFChain container configuration and RQ1 round driver.
- `baseline/fiscobcos/`: FISCO contract, ingest driver, deployment, and native SFChain RQ2/RQ3 helpers.
- `baseline/fabric/`: four-organization SmartBFT deployment and role-routed chaincode.
- `scripts/`: workspace preparation, builds, RQ1 summaries, and RQ2 analysis.
- `docs/EXPERIMENT_SETUP.md`: setup, procedures, and reproduction limits.
- `docs/RESULTS.md`: reported values and their measurement scope.
- `docs/FILE_MAP.md`: mapping from paper measurements to source files.

All paths are relative to this layout. RQ2's supplied native helper runs the
included asynchronous implementation; it does not reproduce the paper's full
Configuration A. RQ4's combined audit includes binding checks beyond the
reported five-step kernel. These limits are detailed in the setup document.
