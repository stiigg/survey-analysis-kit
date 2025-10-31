"""Command-line entrypoint for the advanced SurveyKit pipeline."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict

import pandas as pd
try:  # defer import if PyYAML is not available
    import yaml as _yaml
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    _yaml = None

from surveykit import report_jinja
from surveykit.fairness import fairness_report
from surveykit.governance import retention_gate, scan_pii
from surveykit.integrity import write_manifest


class AuditLogger:
    """Minimal JSONL audit logger."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, level: str = "INFO", **fields: Any) -> None:
        record = {"ts": time.time(), "level": level, **fields}
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, sort_keys=True) + "\n")


def load_config(path: Path) -> Dict[str, Any]:
    if _yaml is None:
        raise RuntimeError("PyYAML is required to load configuration files.")
    with path.open("r", encoding="utf-8") as fh:
        return _yaml.safe_load(fh)


def load_data(cfg: Dict[str, Any]) -> pd.DataFrame:
    data_cfg = cfg.get("data", {})
    csv_path = Path(data_cfg.get("input_csv", "data.csv"))
    if not csv_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {csv_path}")
    df = pd.read_csv(csv_path)
    if "_w" not in df.columns:
        df["_w"] = 1.0
    return df


def ensure_outputs(out_dir: Path) -> None:
    for sub in ("charts", "lineage"):
        (out_dir / sub).mkdir(parents=True, exist_ok=True)


def write_lineage(out_dir: Path, cfg: Dict[str, Any]) -> None:
    lineage = {
        "nodes": [
            {"id": "raw", "type": "dataset"},
            {"id": "weighted", "type": "transform"},
            {"id": "report", "type": "artefact"},
        ],
        "edges": [
            {"from": "raw", "to": "weighted"},
            {"from": "weighted", "to": "report"},
        ],
        "config_version": cfg.get("version"),
    }
    (out_dir / "lineage" / "lineage.json").write_text(
        json.dumps(lineage, indent=2), encoding="utf-8"
    )


def write_provenance(out_dir: Path, cfg: Dict[str, Any]) -> None:
    provenance = {
        "created": time.time(),
        "inputs": cfg.get("data", {}),
        "git": cfg.get("git", {}),
    }
    (out_dir / "provenance.manifest.json").write_text(
        json.dumps(provenance, indent=2), encoding="utf-8"
    )


def build_charts(out_dir: Path) -> None:
    chart_path = out_dir / "charts" / "placeholder.txt"
    chart_path.write_text("charts would be rendered here", encoding="utf-8")


def main(args: argparse.Namespace | None = None) -> Path:
    parser = argparse.ArgumentParser(description="Run the advanced SurveyKit pipeline")
    parser.add_argument("config", type=Path, help="Path to the pipeline config YAML")
    namespace = parser.parse_args(args=args)

    cfg = load_config(namespace.config)
    out_dir = Path(cfg.get("outputs", {}).get("dir", "outputs"))
    out_dir.mkdir(parents=True, exist_ok=True)
    ensure_outputs(out_dir)

    audit = AuditLogger(out_dir / "audit.jsonl")
    audit.log(event="pipeline_start", config=str(namespace.config))

    df = load_data(cfg)
    audit.log(event="data_loaded", rows=len(df))

    pii_hits = scan_pii(
        df,
        columns=[c for c in df.columns if "email" in c.lower() or "phone" in c.lower()],
    )
    if pii_hits:
        audit.log(level="WARN", event="pii_hits_detected", columns=list(pii_hits.keys()), counts=pii_hits)

    retention_gate(out_dir, days=cfg.get("governance", {}).get("retention_days", 365))
    audit.log(event="retention_gate_written")

    fair_df = fairness_report(df, cfg)
    fair_csv = out_dir / "fairness_parity.csv"
    fair_df.to_csv(fair_csv, index=False)
    audit.log(event="fairness_parity_computed", rows=len(fair_df))

    report_text = report_jinja.render_jinja_report(cfg, df, out_dir)
    (out_dir / "report.md").write_text(report_text, encoding="utf-8")
    audit.log(event="report_rendered", path=str(out_dir / "report.md"))

    build_charts(out_dir)
    write_lineage(out_dir, cfg)
    write_provenance(out_dir, cfg)
    audit.log(event="artefacts_prepared")

    manifest_path = write_manifest(out_dir)
    audit.log(event="integrity_manifest_written", path=str(manifest_path))
    audit.log(event="pipeline_complete")
    return out_dir


if __name__ == "__main__":  # pragma: no cover
    main()
