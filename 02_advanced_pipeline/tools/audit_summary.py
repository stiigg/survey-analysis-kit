"""Summarise audit logs for quick review."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path


def summarise(path: Path) -> dict:
    levels = Counter()
    events = Counter()
    if not path.exists():
        return {"levels": levels, "events": events, "missing": True}
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            record = json.loads(line)
            levels[record.get("level", "INFO")] += 1
            events[record.get("event", "unknown")] += 1
    return {"levels": levels, "events": events, "missing": False}


if __name__ == "__main__":  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser(description="Summarise audit JSONL logs")
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    summary = summarise(args.path)
    print(json.dumps({k: dict(v) if hasattr(v, "items") else v for k, v in summary.items()}, indent=2))
