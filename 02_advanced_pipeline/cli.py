"""Command-line entrypoint for the advanced SurveyKit pipeline."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Dict, Iterable

import pandas as pd
try:  # defer import if PyYAML is not available
    import yaml as _yaml
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    _yaml = None

from surveykit import report_jinja
from surveykit.chart_style import apply_matplotlib_theme, load_theme
from surveykit.fairness import fairness_report
from surveykit.governance import retention_gate, scan_pii
from surveykit.integrity import write_manifest
from surveykit.summary_writer import (
    generate_summary,
    save_summary_json,
    verify_summary_against_stats,
    write_summary,
)
from surveykit.validate_charts import ChartSpec, save_chart_metadata, validate_charts
from surveykit.validate_data import (
    SchemaDefinition,
    ValidationError,
    load_schema,
    save_summary as save_validation_summary,
    validate_dataframe,
)


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
    for sub in ("charts", "lineage", "audit"):
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


def _load_schema(cfg: Dict[str, Any]) -> SchemaDefinition | None:
    schema_path = cfg.get("data", {}).get("schema")
    if not schema_path:
        return None
    path = Path(schema_path)
    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")
    return load_schema(path)


def _apply_chart_theme(cfg: Dict[str, Any], audit: AuditLogger) -> None:
    theme_path = cfg.get("charts", {}).get("theme")
    if not theme_path:
        return
    path = Path(theme_path)
    if not path.exists():
        audit.log(level="WARN", event="chart_theme_missing", path=str(path))
        return
    try:
        theme = load_theme(path)
        apply_matplotlib_theme(theme)
        audit.log(event="chart_theme_applied", name=theme.name)
    except Exception as exc:  # pragma: no cover - defensive logging
        audit.log(level="WARN", event="chart_theme_error", message=str(exc))


def _chart_specs(cfg: Dict[str, Any], data_signature: str | None) -> list[ChartSpec]:
    specs_cfg = cfg.get("charts", {}).get("specs", [])
    specs: list[ChartSpec] = []
    for raw in specs_cfg:
        specs.append(
            ChartSpec(
                identifier=raw["id"],
                kind=raw.get("kind", "bar"),
                x=raw["x"],
                y=raw.get("y"),
                aggregation=raw.get("aggregation"),
                filters=raw.get("filters", {}),
                data_signature=data_signature,
                metadata={k: raw[k] for k in ("title", "description") if k in raw},
            )
        )
    return specs


def _materialise_charts(df: pd.DataFrame, specs: list[ChartSpec], out_dir: Path) -> Dict[str, pd.DataFrame]:
    chart_frames: Dict[str, pd.DataFrame] = {}
    for spec in specs:
        filtered = df.copy()
        for column, value in spec.filters.items():
            if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
                filtered = filtered[filtered[column].isin(list(value))]
            else:
                filtered = filtered[filtered[column] == value]
        if spec.kind == "bar" and spec.y:
            if spec.aggregation == "mean":
                aggregated = filtered.groupby(spec.x)[spec.y].mean().reset_index()
            elif spec.aggregation == "sum":
                aggregated = filtered.groupby(spec.x)[spec.y].sum().reset_index()
            elif spec.aggregation == "count":
                aggregated = filtered.groupby(spec.x)[spec.y].count().reset_index()
            else:
                raise ValueError(f"Unsupported aggregation: {spec.aggregation}")
        elif spec.kind == "line" and spec.y:
            aggregated = filtered[[spec.x, spec.y]].sort_values(spec.x)
        else:
            raise ValueError(f"Unsupported chart specification: {spec.kind}")

        chart_frames[spec.identifier] = aggregated
        csv_path = out_dir / "charts" / f"{spec.identifier}.csv"
        aggregated.to_csv(csv_path, index=False)

        try:
            import matplotlib.pyplot as plt

            ax = aggregated.plot(kind="bar" if spec.kind == "bar" else "line", x=spec.x, y=spec.y)
            ax.set_title(spec.metadata.get("title", spec.identifier))
            ax.figure.tight_layout()
            fig_path = out_dir / "charts" / f"{spec.identifier}.png"
            ax.figure.savefig(fig_path)
            plt.close(ax.figure)
        except Exception:  # pragma: no cover - plotting optional
            pass

        save_chart_metadata(spec, out_dir / "charts" / f"{spec.identifier}.meta.json")

    return chart_frames


def _summary_stats_for_crosscheck(df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    stats: Dict[str, Dict[str, float]] = {}
    for col in df.select_dtypes(include=["number"]).columns:
        stats[f"{col} (numeric)"] = {
            "mean": float(df[col].mean()),
            "std": float(df[col].std()),
        }
    for col in df.select_dtypes(include=["object", "category"]).columns:
        top = df[col].value_counts(normalize=True).head(3)
        stats[f"{col} (categorical)"] = {str(idx): float(round(val * 100, 2)) for idx, val in top.items()}
    return stats


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

    schema: SchemaDefinition | None = None
    validation_report = None
    try:
        schema = _load_schema(cfg)
        if schema is not None:
            validation_log = Path(cfg.get("validation", {}).get("log", out_dir / "audit" / "validation.jsonl"))
            validation_report = validate_dataframe(
                df,
                schema,
                log_path=validation_log,
                halt_on_error=True,
            )
            save_validation_summary(validation_report, out_dir / "audit" / "validation_summary.json")
            audit.log(
                event="data_validated",
                data_signature=validation_report.data_signature,
                errors=len(validation_report.errors),
                warnings=len(validation_report.warnings),
            )
    except FileNotFoundError as exc:
        audit.log(level="WARN", event="schema_missing", message=str(exc))
    except ValidationError as exc:
        audit.log(level="ERROR", event="data_validation_failed", message=str(exc))
        raise

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

    summary_doc = generate_summary(df, schema=schema)
    write_summary(summary_doc, out_dir / "summary.md")
    save_summary_json(summary_doc, out_dir / "summary.json")
    audit.log(event="summary_generated", findings=len(summary_doc.findings))

    summary_stats = _summary_stats_for_crosscheck(df)
    mismatches = verify_summary_against_stats(summary_doc, summary_stats)
    if mismatches:
        audit.log(level="WARN", event="summary_crosscheck_failed", details=mismatches)

    _apply_chart_theme(cfg, audit)
    data_signature = validation_report.data_signature if validation_report else None
    specs = _chart_specs(cfg, data_signature)
    if specs:
        chart_frames = _materialise_charts(df, specs, out_dir)
        chart_report = validate_charts(df, specs, chart_frames)
        (out_dir / "audit" / "chart_validation.json").write_text(
            json.dumps(chart_report.to_dict(), indent=2), encoding="utf-8"
        )
        audit.log(
            event="charts_validated",
            errors=sum(1 for issue in chart_report.issues if issue.level == "ERROR"),
            warnings=sum(1 for issue in chart_report.issues if issue.level == "WARN"),
        )
    else:
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
