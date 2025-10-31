"""Jinja-based reporting helpers for the advanced pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd
from jinja2 import Environment, FileSystemLoader


def _render_appendix(cfg: Dict[str, Any], out_dir: Path) -> str:
    env = Environment(loader=FileSystemLoader(str(Path(__file__).parent / "report_templates")))
    template = env.get_template("appendix_methods.md.j2")
    return template.render(config=cfg, out_dir=str(out_dir))


def render_jinja_report(cfg: Dict[str, Any], df: pd.DataFrame, out_dir: Path) -> str:
    """Render the report markdown including the fairness appendix."""
    env = Environment(loader=FileSystemLoader(str(Path(__file__).parent / "report_templates")))
    base = "# Survey Results\n\n" + f"Total responses: {len(df)}\n"
    appendix = _render_appendix(cfg, out_dir)
    return base + "\n\n" + appendix
