"""Integrity utilities for tamper-evident pipeline artefacts."""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Dict, Optional


def sha256_file(p: Path) -> Optional[str]:
    """Return the SHA256 hash for a file if it exists."""
    if not p.exists() or not p.is_file():
        return None
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_dir(d: Path) -> str:
    """Return a stable SHA256 hash for the contents of a directory."""
    h = hashlib.sha256()
    for p in sorted(d.rglob("*")):
        if p.is_file():
            h.update(p.relative_to(d).as_posix().encode())
            h.update(p.read_bytes())
    return h.hexdigest()


def write_manifest(root: Path, prev_hash: Optional[str] = None) -> Path:
    """Create a manifest with hashed artefacts and return the written path."""
    artefacts: Dict[str, Optional[str]] = {
        "report_md": sha256_file(root / "report.md"),
        "audit_jsonl": sha256_file(root / "audit.jsonl"),
        "provenance_manifest": sha256_file(root / "provenance.manifest.json"),
        "charts_dir_hash": sha256_dir(root / "charts") if (root / "charts").exists() else None,
        "lineage_json": sha256_file(root / "lineage" / "lineage.json"),
    }
    record = {
        "ts": time.time(),
        "artefacts": artefacts,
        "prev_manifest_hash": prev_hash,
    }
    payload = json.dumps(record, sort_keys=True, separators=(",", ":")).encode()
    chain_hash = hashlib.sha256(payload).hexdigest()
    out = root / f"integrity.manifest.{int(record['ts'])}.json"
    out.write_text(
        json.dumps({**record, "manifest_hash": chain_hash}, indent=2),
        encoding="utf-8",
    )
    signer = os.getenv("SURVEYKIT_SIGN_CMD")
    if signer:
        os.system(f'{signer} "{out}"')
    return out
