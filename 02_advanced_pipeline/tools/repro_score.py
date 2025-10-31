"""Compute a simple reproducibility fingerprint for pipeline artefacts."""

from __future__ import annotations

import hashlib
from pathlib import Path

ARTEFACTS = [
    "report.md",
    "provenance.manifest.json",
    "fairness_parity.csv",
]


def fingerprint(root: Path) -> str:
    digest = hashlib.sha256()
    for artefact in ARTEFACTS:
        path = root / artefact
        if not path.exists():
            continue
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


if __name__ == "__main__":  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser(description="Compute reproducibility fingerprint")
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    print(fingerprint(args.root))
