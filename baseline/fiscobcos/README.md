# FISCO BCOS baseline

This directory contains the four-node FISCO BCOS v3.17.1 PBFT baseline used
for RQ1. Setup is described in `../../docs/EXPERIMENT_SETUP.md`. The checked
Solidity contract is in `contracts/SoftwareFactoryLogs.sol`;
`loadgen/contract.go` embeds its ABI and bytecode. The contract embedding can
be regenerated with `scripts/compile_contract.mjs`.

The load generator supports `deploy`, `ingest`, and `check`. RQ1 uses
`ingest` with responsible-user evidence, persisted endorsement preparation,
and on-chain signature checking. Its clock runs from `submitted_ms` to
successful receipt observation. The package-level driver resets and deploys
the chain before every round.

`bin/build_chain.sh` is upstream deployment tooling, not an experiment trace.
The official node binary and SDK shared library are fetched with checksums;
they are not included in this source package.
