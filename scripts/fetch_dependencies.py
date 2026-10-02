#!/usr/bin/env python3
"""Fetch pinned upstream runtime dependencies into the generated workspace."""
import hashlib
from pathlib import Path
import tarfile
import urllib.request

BASE = Path(__file__).resolve().parents[1]
FILES = (
    ("https://github.com/FISCO-BCOS/bcos-c-sdk/releases/download/v3.6.0/libbcos-c-sdk.so",
     "baseline/fiscobcos/loadgen/lib/libbcos-c-sdk.so",
     "b5a2a1606086245cc73d356d70011b32a112a38cf5e6a10631377a4b4a7dec83"),
    ("https://github.com/hyperledger/fabric/releases/download/v3.1.5/hyperledger-fabric-linux-amd64-3.1.5.tar.gz",
     "baseline/fabric/fabric-3.1.5.tar.gz",
     "b9c31fd490991e76f8acb1835dee09fc19fee5428cb13e190ee6e0bdd2c37858"),
    ("https://github.com/FISCO-BCOS/FISCO-BCOS/releases/download/v3.17.1/fisco-bcos-linux-x86_64.tar.gz",
     "baseline/fiscobcos/bin/fisco-bcos-linux-x86_64.tar.gz",
     "28eb054bb222b805e116bf7d2462709f99d1f460a732e978555abe61973b3cbb"),
)


def main():
    for url, name, digest in FILES:
        target = BASE / name
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            print(f"Downloading {url}", flush=True)
            req = urllib.request.Request(url, headers={"User-Agent": "SFChain-artifact"})
            with urllib.request.urlopen(req, timeout=120) as response:
                target.write_bytes(response.read())
        if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
            raise SystemExit(f"Checksum mismatch: {target}")
        print(f"SHA256 verified: {name}")
        if name.endswith(".tar.gz"):
            with tarfile.open(target) as archive:
                archive.extractall(target.parent, filter="data")


if __name__ == "__main__":
    main()
