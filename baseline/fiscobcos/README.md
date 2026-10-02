# FISCO BCOS baseline

Four FISCO BCOS v3.17.1 PBFT nodes for RQ1. Setup is described in
`../../docs/EXPERIMENT_SETUP.md`. The checked Solidity contract is in
`contracts/SoftwareFactoryLogs.sol`; `loadgen/contract.go` embeds its ABI and
bytecode. `scripts/compile_contract.mjs` regenerates that embedding.

The load generator supports `deploy`, `ingest`, and `check`. RQ1 uses
`ingest` with responsible-user evidence, persisted endorsement preparation,
and on-chain signature checking. Its clock is `submitted_ms` to successful
receipt observation. The package-level driver resets and deploys the chain
before every round.

`bin/build_chain.sh` is upstream deployment tooling, not an experiment trace.
The official node binary and SDK shared library are fetched with checksums;
they are not included in this source package.

Native SFChain helpers under `scripts/` support the paper's block-processing
and liveness studies. The package-level `scripts/analyze_rq2.py` analyzes RQ2
logs. See the setup document for the limits of the supplied RQ2 driver.
