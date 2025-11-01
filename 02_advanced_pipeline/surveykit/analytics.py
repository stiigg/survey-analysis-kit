"""Advanced analytical helpers for freelance/consulting workflows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
import pandas as pd


@dataclass
class ReliabilityResult:
    scale: str
    alpha: float
    item_count: int


def cronbach_alpha(frame: pd.DataFrame) -> ReliabilityResult:
    """Calculate Cronbach's alpha for a set of columns."""

    if frame.empty or frame.shape[1] < 2:
        raise ValueError("Cronbach's alpha requires at least two items.")
    variance_items = frame.var(axis=0, ddof=1)
    total_score = frame.sum(axis=1)
    variance_total = total_score.var(ddof=1)
    k = frame.shape[1]
    alpha = (k / (k - 1)) * (1 - variance_items.sum() / variance_total)
    return ReliabilityResult(scale=",".join(frame.columns), alpha=float(alpha), item_count=k)


@dataclass
class BiasReport:
    column: str
    metric: str
    value: float
    detail: Optional[Dict[str, float]] = None


def response_rate_by_segment(df: pd.DataFrame, segment: str, response_column: str) -> BiasReport:
    """Compute response rates by segment to uncover representation gaps."""

    counts = df.groupby(segment)[response_column].apply(lambda s: s.notna().mean())
    return BiasReport(column=segment, metric="response_rate", value=float(counts.std()), detail=counts.to_dict())


@dataclass
class HypothesisResult:
    metric: str
    statistic: float
    p_value: float
    details: Dict[str, float]


def _welch_t_test(a: np.ndarray, b: np.ndarray) -> tuple[float, float]:
    mean_a, mean_b = np.mean(a), np.mean(b)
    var_a, var_b = np.var(a, ddof=1), np.var(b, ddof=1)
    n_a, n_b = len(a), len(b)
    t_stat = (mean_a - mean_b) / np.sqrt(var_a / n_a + var_b / n_b)
    # degrees of freedom Welch-Satterthwaite
    numerator = (var_a / n_a + var_b / n_b) ** 2
    denominator = ((var_a / n_a) ** 2) / (n_a - 1) + ((var_b / n_b) ** 2) / (n_b - 1)
    df = numerator / denominator
    try:
        from scipy.stats import t as _student_t  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("scipy is required for Welch's t-test p-value computation") from exc

    p_value = (1 - _student_t.cdf(abs(t_stat), df)) * 2
    return float(t_stat), float(p_value)


def t_test(df: pd.DataFrame, column: str, group: str, group_a: str, group_b: str) -> HypothesisResult:
    """Run an independent t-test between two groups."""

    subset = df[df[group].isin([group_a, group_b])]
    a = subset[subset[group] == group_a][column].dropna().to_numpy()
    b = subset[subset[group] == group_b][column].dropna().to_numpy()
    if len(a) < 2 or len(b) < 2:
        raise ValueError("Insufficient data for Welch's t-test.")
    statistic, p_value = _welch_t_test(a, b)
    return HypothesisResult(
        metric=f"{column}_by_{group}",
        statistic=float(statistic),
        p_value=float(p_value),
        details={
            "mean_a": float(np.mean(a)),
            "mean_b": float(np.mean(b)),
            "n_a": float(len(a)),
            "n_b": float(len(b)),
        },
    )


def effect_size(df: pd.DataFrame, column: str, group: str, group_a: str, group_b: str) -> float:
    """Cohen's d effect size for two groups."""

    subset = df[df[group].isin([group_a, group_b])]
    a = subset[subset[group] == group_a][column].dropna().to_numpy()
    b = subset[subset[group] == group_b][column].dropna().to_numpy()
    if len(a) < 2 or len(b) < 2:
        raise ValueError("Insufficient data for effect size computation.")
    pooled_std = np.sqrt(((len(a) - 1) * np.var(a, ddof=1) + (len(b) - 1) * np.var(b, ddof=1)) / (len(a) + len(b) - 2))
    return float((np.mean(a) - np.mean(b)) / pooled_std)


__all__ = [
    "ReliabilityResult",
    "BiasReport",
    "HypothesisResult",
    "cronbach_alpha",
    "response_rate_by_segment",
    "t_test",
    "effect_size",
]
