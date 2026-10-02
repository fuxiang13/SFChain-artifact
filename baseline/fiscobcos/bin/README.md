# bin/ - FISCO BCOS node binary

This directory intentionally does **not** ship the official FISCO BCOS node
executable (53 MB). It contains only the official `build_chain.sh`
(configuration generator).

## Obtaining the node binary

Run the fetch helper (downloads the release tarball, verifies SHA-256 against
the value published on the GitHub release, extracts into this directory):

```bash
bash ../scripts/fetch_fisco_binary.sh
```

`scripts/generate_nodes.sh` invokes it automatically when the binary is
missing.

## Manual download

| Item | Value |
|---|---|
| Release | FISCO BCOS v3.17.1 (`github.com/FISCO-BCOS/FISCO-BCOS/releases/tag/v3.17.1`) |
| Asset | `fisco-bcos-linux-x86_64.tar.gz` |
| URL | https://github.com/FISCO-BCOS/FISCO-BCOS/releases/download/v3.17.1/fisco-bcos-linux-x86_64.tar.gz |
| **tarball sha256** | `28eb054bb222b805e116bf7d2462709f99d1f460a732e978555abe61973b3cbb` (matches the digest published on the release) |
| extracted `fisco-bcos` sha256 | `e67cd8f869264cbf78e7b2155a420dd7b71ec747bdb9b1fbf7d464484a070541` |

After extracting, this directory must contain:

```
bin/build_chain.sh    (shipped)
bin/fisco-bcos        (fetched)
```
