"""Simple pipeline compliance checks for produced artefacts."""

from __future__ import annotations

import json
from pathlib import Path


def check_outputs(root: Path) -> dict:
    artefacts = [
        "report.md",
        "audit.jsonl",
        "provenance.manifest.json",
        "fairness_parity.csv",
        "retention.json",
    ]
    missing = [a for a in artefacts if not (root / a).exists()]
    return {"missing": missing, "root": str(root)}


if __name__ == "__main__":  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser(description="Check pipeline outputs for compliance")
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    result = check_outputs(args.root)
    print(json.dumps(result, indent=2))
