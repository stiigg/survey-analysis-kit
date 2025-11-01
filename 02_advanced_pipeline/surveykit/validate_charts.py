"""Automated checks that confirm charts align with validated data."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd


@dataclass
class ChartSpec:
    """Description of how a chart was produced."""

    identifier: str
    kind: str
    x: str
    y: Optional[str] = None
    aggregation: Optional[str] = None
    filters: Dict[str, Any] = field(default_factory=dict)
    data_signature: Optional[str] = None
    created_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "identifier": self.identifier,
            "kind": self.kind,
            "x": self.x,
            "y": self.y,
            "aggregation": self.aggregation,
            "filters": self.filters,
            "data_signature": self.data_signature,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ChartSpec":
        created_at = (
            datetime.fromisoformat(payload["created_at"]) if payload.get("created_at") else None
        )
        return cls(
            identifier=payload["identifier"],
            kind=payload["kind"],
            x=payload["x"],
            y=payload.get("y"),
            aggregation=payload.get("aggregation"),
            filters=payload.get("filters", {}),
            data_signature=payload.get("data_signature"),
            created_at=created_at,
            metadata=payload.get("metadata", {}),
        )


@dataclass
class ChartIssue:
    identifier: str
    level: str
    message: str
    context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "identifier": self.identifier,
            "level": self.level,
            "message": self.message,
            "context": self.context,
        }


@dataclass
class ChartValidationReport:
    issues: List[ChartIssue]
    data_signature: str

    def has_errors(self) -> bool:
        return any(issue.level == "ERROR" for issue in self.issues)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "issues": [issue.to_dict() for issue in self.issues],
            "data_signature": self.data_signature,
        }


# ---------------------------------------------------------------------------
# Core validation helpers
# ---------------------------------------------------------------------------


def _hash_dataframe(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()


def _apply_filters(df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    filtered = df
    for column, expected in filters.items():
        if column not in df.columns:
            raise KeyError(f"Filter column not found: {column}")
        if isinstance(expected, Iterable) and not isinstance(expected, (str, bytes)):
            filtered = filtered[filtered[column].isin(list(expected))]
        else:
            filtered = filtered[filtered[column] == expected]
    return filtered


def verify_chart(
    df: pd.DataFrame,
    spec: ChartSpec,
    chart_data: pd.DataFrame,
    *,
    tolerance: float = 1e-6,
) -> ChartIssue | None:
    """Validate a single chart result against the raw data."""

    filtered = _apply_filters(df, spec.filters) if spec.filters else df

    if spec.kind == "bar":
        if spec.y is None or spec.aggregation is None:
            return ChartIssue(
                identifier=spec.identifier,
                level="ERROR",
                message="Bar charts must define 'y' and 'aggregation'.",
            )
        expected = filtered.groupby(spec.x)[spec.y]
        if spec.aggregation == "mean":
            expected_series = expected.mean()
        elif spec.aggregation == "sum":
            expected_series = expected.sum()
        elif spec.aggregation == "count":
            expected_series = expected.count()
        else:
            return ChartIssue(
                identifier=spec.identifier,
                level="ERROR",
                message=f"Unsupported aggregation: {spec.aggregation}",
            )

        expected_df = expected_series.reset_index().sort_values(spec.x).reset_index(drop=True)
        candidate = chart_data.sort_values(spec.x).reset_index(drop=True)
        if not expected_df[spec.x].equals(candidate[spec.x]):
            return ChartIssue(
                identifier=spec.identifier,
                level="ERROR",
                message="Category mismatch between chart and data.",
                context={"expected": expected_df[spec.x].tolist(), "observed": candidate[spec.x].tolist()},
            )
        if not (expected_df[spec.y].astype(float).round(6).equals(candidate[spec.y].astype(float).round(6))):
            deltas = (expected_df[spec.y] - candidate[spec.y]).abs()
            if (deltas > tolerance).any():
                return ChartIssue(
                    identifier=spec.identifier,
                    level="ERROR",
                    message="Aggregated values differ from data.",
                    context={"max_delta": float(deltas.max())},
                )
    elif spec.kind == "line":
        # assume the caller already provided the right time/order column
        if not chart_data[spec.x].isin(filtered[spec.x]).all():
            missing = chart_data.loc[~chart_data[spec.x].isin(filtered[spec.x]), spec.x].tolist()
            return ChartIssue(
                identifier=spec.identifier,
                level="ERROR",
                message="Chart includes x-values that do not exist in the dataset.",
                context={"values": missing},
            )
    else:
        return ChartIssue(
            identifier=spec.identifier,
            level="WARN",
            message=f"No validator registered for chart kind: {spec.kind}",
        )

    return None


def validate_charts(
    df: pd.DataFrame,
    specs: List[ChartSpec],
    charts: Dict[str, pd.DataFrame],
    *,
    tolerance: float = 1e-6,
) -> ChartValidationReport:
    """Validate multiple charts produced from the dataset."""

    data_signature = _hash_dataframe(df)
    issues: List[ChartIssue] = []

    for spec in specs:
        if spec.identifier not in charts:
            issues.append(
                ChartIssue(
                    identifier=spec.identifier,
                    level="ERROR",
                    message="Chart output missing.",
                )
            )
            continue

        chart_df = charts[spec.identifier]
        issue = verify_chart(df, spec, chart_df, tolerance=tolerance)
        if issue:
            issues.append(issue)

        if spec.data_signature and spec.data_signature != data_signature:
            issues.append(
                ChartIssue(
                    identifier=spec.identifier,
                    level="ERROR",
                    message="Chart is stale relative to the dataset signature.",
                    context={"chart_signature": spec.data_signature, "data_signature": data_signature},
                )
            )

    return ChartValidationReport(issues=issues, data_signature=data_signature)


def save_chart_metadata(spec: ChartSpec, destination: Path) -> None:
    """Persist chart metadata so staleness can be detected."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = spec.to_dict()
    payload["created_at"] = (spec.created_at or datetime.utcnow()).isoformat()
    destination.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_chart_metadata(path: Path) -> ChartSpec:
    """Load chart metadata."""

    data = json.loads(path.read_text(encoding="utf-8"))
    return ChartSpec.from_dict(data)


__all__ = [
    "ChartSpec",
    "ChartIssue",
    "ChartValidationReport",
    "validate_charts",
    "save_chart_metadata",
    "load_chart_metadata",
]
