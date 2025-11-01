"""Generate executive-ready survey summaries with auditability."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

from .validate_data import SchemaDefinition


@dataclass
class SummaryFinding:
    """Structured representation of a narrative insight."""

    topic: str
    text: str
    evidence: Dict[str, float | str]
    severity: str = "info"

    def as_dict(self) -> Dict[str, object]:
        return {
            "topic": self.topic,
            "text": self.text,
            "evidence": self.evidence,
            "severity": self.severity,
        }


@dataclass
class SummaryDocument:
    findings: List[SummaryFinding] = field(default_factory=list)

    def add(self, finding: SummaryFinding) -> None:
        self.findings.append(finding)

    def to_markdown(self) -> str:
        lines: List[str] = ["# Executive Summary", ""]
        for finding in self.findings:
            lines.append(f"- **{finding.topic}:** {finding.text} ")
            evidence_bits = ", ".join(f"{k}={v}" for k, v in finding.evidence.items())
            lines.append(f"  - Evidence: {evidence_bits}")
            lines.append(f"  - Severity: {finding.severity}")
            lines.append("")
        return "\n".join(lines).strip()

    def to_json(self) -> str:
        return json.dumps([finding.as_dict() for finding in self.findings], indent=2)


def _top_categories(series: pd.Series, limit: int = 3) -> List[tuple[str, float]]:
    counts = series.value_counts(normalize=True).head(limit)
    return [(str(idx), float(round(value * 100, 2))) for idx, value in counts.items()]


def _numeric_summary(series: pd.Series) -> Dict[str, float]:
    return {
        "mean": float(series.mean()),
        "median": float(series.median()),
        "std": float(series.std()),
        "min": float(series.min()),
        "max": float(series.max()),
    }


def generate_summary(
    df: pd.DataFrame,
    *,
    schema: Optional[SchemaDefinition] = None,
    categorical_limit: int = 3,
    include_numeric: bool = True,
    include_categorical: bool = True,
) -> SummaryDocument:
    """Create a structured narrative summary for the dataset."""

    doc = SummaryDocument()

    if include_numeric:
        numeric_cols = df.select_dtypes(include=["number"]).columns
        for col in numeric_cols:
            stats = _numeric_summary(df[col].dropna())
            topic = f"{col} (numeric)"
            text = (
                f"Average of {stats['mean']:.2f} with range {stats['min']:.2f}–{stats['max']:.2f}. "
                f"Standard deviation is {stats['std']:.2f}."
            )
            doc.add(
                SummaryFinding(
                    topic=topic,
                    text=text,
                    evidence={"mean": stats["mean"], "std": stats["std"]},
                )
            )

    if include_categorical:
        cat_cols = df.select_dtypes(include=["object", "category"]).columns
        for col in cat_cols:
            top_values = _top_categories(df[col].dropna(), limit=categorical_limit)
            if not top_values:
                continue
            formatted = ", ".join(f"{label} ({pct:.1f}% )" for label, pct in top_values)
            doc.add(
                SummaryFinding(
                    topic=f"{col} (categorical)",
                    text=f"Top responses: {formatted}",
                    evidence={label: pct for label, pct in top_values},
                )
            )

    if schema is not None:
        doc.add(
            SummaryFinding(
                topic="Data Quality",
                text=f"Summary generated against schema version {schema.version or 'unspecified'}.",
                evidence={"schema_version": schema.version or "n/a"},
                severity="audit",
            )
        )

    return doc


def write_summary(doc: SummaryDocument, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(doc.to_markdown(), encoding="utf-8")


def save_summary_json(doc: SummaryDocument, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(doc.to_json(), encoding="utf-8")


def verify_summary_against_stats(
    doc: SummaryDocument,
    stats: Dict[str, Dict[str, float]],
    *,
    tolerance: float = 1e-6,
) -> List[str]:
    """Compare summary evidence against expected statistics."""

    mismatches: List[str] = []
    for finding in doc.findings:
        if finding.topic not in stats:
            continue
        expected = stats[finding.topic]
        for key, value in finding.evidence.items():
            if key not in expected:
                mismatches.append(f"{finding.topic}: evidence '{key}' missing from stats")
                continue
            if abs(float(expected[key]) - float(value)) > tolerance:
                mismatches.append(
                    f"{finding.topic}: evidence '{key}' differs (expected {expected[key]}, observed {value})"
                )
    return mismatches


__all__ = [
    "SummaryFinding",
    "SummaryDocument",
    "generate_summary",
    "write_summary",
    "save_summary_json",
    "verify_summary_against_stats",
]
