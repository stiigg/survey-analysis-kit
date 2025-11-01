"""Simple review tracker to capture client approvals."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List


@dataclass
class DeliverableStatus:
    identifier: str
    status: str
    comments: List[str]


def load_status(path: Path) -> Dict[str, DeliverableStatus]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {item["identifier"]: DeliverableStatus(**item) for item in data}


def save_status(statuses: Dict[str, DeliverableStatus], path: Path) -> None:
    payload = [asdict(status) for status in statuses.values()]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def update_status(path: Path, identifier: str, status: str, comment: str | None = None) -> None:
    statuses = load_status(path)
    record = statuses.get(identifier) or DeliverableStatus(identifier=identifier, status="pending", comments=[])
    record.status = status
    if comment:
        record.comments.append(comment)
    statuses[identifier] = record
    save_status(statuses, path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Track approval status for deliverables.")
    parser.add_argument("identifier", help="Identifier of the deliverable (e.g., chart_1)")
    parser.add_argument("status", choices=["approved", "needs_revision", "client_comment"], help="New status")
    parser.add_argument("--comment", help="Optional comment from reviewer")
    parser.add_argument("--store", type=Path, default=Path("outputs/review_status.json"), help="Path to the status file")
    args = parser.parse_args()
    update_status(args.store, args.identifier, args.status, comment=args.comment)


if __name__ == "__main__":  # pragma: no cover
    main()
