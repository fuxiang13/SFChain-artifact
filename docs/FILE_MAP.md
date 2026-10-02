# Paper-to-code map

| Paper measurement or mechanism | Source and procedure |
|---|---|
| RQ1 timing table and attestation control | `scripts/run_rq1_frozen.ps1`; `deploy/sf-docker/sfchain_e1g_docker_test.ps1`; `baseline/fiscobcos/scripts/analyze_e1g.py`; both platform normalizers; `scripts/aggregate_frozen_rq1.py` |
| RQ1 storage table | Separate `-MeasureStorage` campaign; `baseline/fiscobcos/scripts/storage_report.py` |
| RQ2 block-processing figure | `baseline/fiscobcos/scripts/sfchain_e1_test.sh`; `scripts/analyze_rq2.py` |
| RQ3 liveness table | `prototype/cmd/rq3proxy`; `baseline/fiscobcos/scripts/rq3_run.sh` and `rq3_analyze.py` |
| RQ4 audit table and evidence binding | `prototype/cmd/auditverify`; `binding_test.go`; input generation in setup Section 6; combined full-audit timing includes additional checks |
| Transaction endorsement | `prototype/internal/core/endorsement_service.go`; `management_node.go`; `prototype/pkg/crypto/ecdsa_crypto.go` |
| Block witnessing and successor anchoring | `prototype/internal/core/management_node.go`; `dto_node.go`; `prototype/pkg/types/block.go` |
| Custody and storage layout | `prototype/internal/database/mysql/block_manager.go`; `prototype/mysql_schema.sql` |

All `bin/`, `logs/`, `runs/`, baseline `results/`, node certificates, and
baseline binaries are generated in the working copy. None is shipped as
measured evidence. RQ1 has 20,000 business records; RQ3 and RQ4 retain their
own paper-specific workload sizes and signing paths.
