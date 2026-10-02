# Paper measurements

These tables transcribe the paper's reported values; they are not regenerated
results or raw experimental evidence. Run outputs depend on hardware and
runtime conditions. Procedures and reproduction limits are in
`EXPERIMENT_SETUP.md`.

## RQ1: 20,000 business records, five complete rounds

| Platform | TPS | Mean latency (ms) | p50 (ms) | p95 (ms) |
|---|---:|---:|---:|---:|
| SFChain, successor construction | 505.4 | 2322 | 2297 | 2946 |
| FISCO BCOS, checked contract | 459.2 | 6815 | 5150 | 14710 |
| Fabric, role-routed endorsement | 285.0 | 13037 | 13246 | 17397 |

The paired SFChain attestation control reports 529.5 TPS and 1.15 s p50.
Use the post-admission starts in the README. Do not substitute source-log,
SQL-read, or block-seal timestamps for them.

| Platform | Baseline-subtracted network storage (decimal MB) |
|---|---:|
| SFChain | 55.050 |
| FISCO BCOS | 199.686 |
| Fabric | 957.018 |

Timing: `scripts/run_rq1_frozen.ps1 -Rounds 5 -Total 20000`.
Storage: add `-MeasureStorage` in a separate campaign with new SFChain round
labels. The latter resets Fabric each round, unlike the timing campaign.

## RQ2: block-build start to four-role attestation

| Block size | Config A TPS | Config B TPS | Config A mean (s) | Config B mean (s) |
|---|---:|---:|---:|---:|
| 128 | 202.93 | 2040.56 | 1.4548 | 0.2101 |
| 256 | 204.83 | 2330.37 | 2.8797 | 0.3880 |
| 512 | 201.95 | 2420.58 | 5.7632 | 0.7448 |
| 1024 | 238.24 | 2651.88 | 11.3778 | 1.4808 |

The native helper and `scripts/analyze_rq2.py` support the included SFChain
implementation. They do not supply the complete Config A campaign; see the
setup document. These values must not be inferred from an RQ1 run.

## RQ3: unanimous-witness liveness

The paper reports 216-ms baseline p50; 271/646/1135 ms with
100/500/1,000-ms role delays; 661 ms with two 500-ms delays; 17 of 20 blocks
resuming after reconnection; and no completed attestations among four sealed
blocks during refusal. These are three-round summaries under the conditions
in the liveness table. The fault driver and analyzer are
`baseline/fiscobcos/scripts/rq3_run.sh` and `rq3_analyze.py`.

## RQ4: preloaded verification kernel

| Step | Mean (ms) | p50 (ms) |
|---|---:|---:|
| Content hash | 0.003 | 0.002 |
| Merkle inclusion | 0.34 | 0.27 |
| ECDSA endorsement | 0.07 | 0.03 |
| Header walk | 0.005 | 0.005 |
| BLS aggregate | 1.00 | 0.85 |
| Combined five-step kernel | 1.40 | 1.19 |

Block size 100; chain depth ten; 200 repetitions per micro-step and 50 for
walking/combined checks. The supplied full audit also checks evidence binding;
its combined result has a broader scope than this reported kernel.
