#!/usr/bin/env python3
"""Create an isolated runnable workspace; never write outputs into the package."""
import argparse
import shutil
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    package = Path(__file__).resolve().parents[1]
    dest = args.destination.resolve()
    if dest == package or package in dest.parents or dest.exists():
        raise SystemExit("Destination must be new and outside the artifact directory")
    dest.mkdir(parents=True)
    for name in ("prototype", "deploy", "baseline", "scripts", "docs"):
        shutil.copytree(package / name, dest / name,
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    shutil.copy2(package / "README.md", dest / "README.md")
    for name in ("bin", "logs", "runs", "baseline/fabric/results", "baseline/fiscobcos/results"):
        (dest / name).mkdir(parents=True, exist_ok=True)
    print(f"Runnable workspace: {dest}")
    print("Read docs/EXPERIMENT_SETUP.md before building or resetting experiment state.")


if __name__ == "__main__":
    main()
