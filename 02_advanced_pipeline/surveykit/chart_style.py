"""Utilities to apply brand-specific chart styling."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

try:  # Optional dependency
    import yaml as _yaml
except ModuleNotFoundError:  # pragma: no cover
    _yaml = None


@dataclass
class ChartTheme:
    name: str
    fonts: Dict[str, Any]
    palette: Dict[str, str]
    background: Optional[str] = None
    logo_path: Optional[str] = None

    @classmethod
    def from_mapping(cls, mapping: Dict[str, Any]) -> "ChartTheme":
        return cls(
            name=mapping.get("name", "default"),
            fonts=mapping.get("fonts", {}),
            palette=mapping.get("palette", {}),
            background=mapping.get("background"),
            logo_path=mapping.get("logo_path"),
        )


def load_theme(config_path: Path) -> ChartTheme:
    if config_path.suffix.lower() not in {".yaml", ".yml"}:
        raise ValueError("Chart configuration must be a YAML file.")
    if _yaml is None:
        raise RuntimeError("PyYAML is required to load chart themes.")
    with config_path.open("r", encoding="utf-8") as fh:
        data = _yaml.safe_load(fh)
    return ChartTheme.from_mapping(data)


def apply_matplotlib_theme(theme: ChartTheme) -> None:
    try:
        import matplotlib.pyplot as plt
    except ModuleNotFoundError:  # pragma: no cover
        raise RuntimeError("matplotlib is required to apply matplotlib themes.")

    plt.rcParams.update({
        "figure.facecolor": theme.background or "white",
        "axes.facecolor": theme.background or "white",
        "font.family": theme.fonts.get("base", "sans-serif"),
        "axes.titlesize": theme.fonts.get("title_size", 14),
        "axes.labelsize": theme.fonts.get("label_size", 12),
    })

    if theme.palette:
        colors = list(theme.palette.values())
        plt.rcParams["axes.prop_cycle"] = plt.cycler(color=colors)


def apply_plotly_theme(theme: ChartTheme) -> Dict[str, Any]:
    base = {
        "layout": {
            "font": {"family": theme.fonts.get("base", "Arial")},
            "paper_bgcolor": theme.background or "white",
            "plot_bgcolor": theme.background or "white",
            "colorway": list(theme.palette.values()),
        }
    }
    if theme.logo_path:
        base["layout"]["images"] = [
            {
                "source": theme.logo_path,
                "xref": "paper",
                "yref": "paper",
                "x": 1,
                "y": 1,
                "sizex": 0.2,
                "sizey": 0.2,
                "xanchor": "right",
                "yanchor": "bottom",
                "opacity": 0.8,
                "layer": "above",
            }
        ]
    return base


__all__ = ["ChartTheme", "load_theme", "apply_matplotlib_theme", "apply_plotly_theme"]
