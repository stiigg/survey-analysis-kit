"""Governance utilities for privacy and retention hooks.""" 

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd

DEFAULT_PII_PATTERNS = [
    re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I),
    re.compile(r"\b\d{10,}\b"),
]


def scan_pii(
    df: pd.DataFrame,
    columns: Optional[Iterable[str]] = None,
    patterns: Optional[Iterable[re.Pattern]] = None,
    sample_size: int = 5000,
) -> Dict[str, int]:
    """Return a mapping of column names to detected PII hit counts."""
    columns = list(columns or df.columns)
    patterns = list(patterns or DEFAULT_PII_PATTERNS)
    hits: Dict[str, int] = {}
    for c in columns:
        if c not in df.columns:
            continue
        series = df[c]
        if not pd.api.types.is_string_dtype(series):
            continue
        sample = series.dropna().astype(str).head(sample_size)
        if sample.empty:
            continue
        for pat in patterns:
            mask = sample.str.contains(pat)
            if mask.any():
                hits.setdefault(c, 0)
                hits[c] += int(mask.sum())
    return hits


def retention_gate(outputs_root: Path, days: int) -> None:
    """Write a retention metadata file used by downstream cleanup jobs."""
    outputs_root.mkdir(parents=True, exist_ok=True)
    payload = {"created": time.time(), "ttl_days": days}
    (outputs_root / "retention.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
