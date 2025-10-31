"""Fairness diagnostics for the advanced pipeline."""

from typing import Dict, List

import numpy as np
import pandas as pd


def parity_gaps(df: pd.DataFrame, cfg: Dict, question: str, group_col: str, thr: int) -> pd.DataFrame:
    """Compute weighted top-box rate gaps versus the best performing group."""
    g = df[[group_col, question, "_w"]].dropna()
    if g.empty:
        return pd.DataFrame(columns=[group_col, "rate", "gap_vs_ref"])
    g["tb"] = (pd.to_numeric(g[question], errors="coerce") >= thr).astype(float)
    grp = (
        g.groupby(group_col)
        .apply(lambda s: np.average(s["tb"], weights=s["_w"]))
        .rename("rate")
        .reset_index()
    )
    if grp.empty:
        return grp
    ref = grp.sort_values("rate", ascending=False).iloc[0]["rate"]
    grp["gap_vs_ref"] = grp["rate"] - ref
    return grp


def fairness_report(df: pd.DataFrame, cfg: Dict) -> pd.DataFrame:
    """Generate a fairness parity table for configured questions and groups."""
    thr = cfg.get("metrics", {}).get("top_box_threshold", 4)
    groups: List[str] = cfg.get("metrics", {}).get("groupby") or []
    qs: List[str] = [c for c in df.columns if c.startswith("Q")]
    rows = []
    for q in qs[:10]:
        if not groups:
            break
        for g in groups:
            if g not in df.columns:
                continue
            res = parity_gaps(df.dropna(subset=[g]), cfg, q, g, thr)
            for _, r in res.iterrows():
                rows.append(
                    {
                        "question": q,
                        "group": g,
                        "bucket": r[g],
                        "rate": r["rate"],
                        "gap_vs_ref": r["gap_vs_ref"],
                    }
                )
    columns = ["question", "group", "bucket", "rate", "gap_vs_ref"]
    return pd.DataFrame(rows, columns=columns if rows else columns)
