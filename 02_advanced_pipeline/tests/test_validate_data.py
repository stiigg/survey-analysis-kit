import json
from pathlib import Path

import pytest

pandas = pytest.importorskip("pandas")
pd = pandas

from surveykit.validate_data import (
    ColumnSchema,
    SchemaDefinition,
    ValidationError,
    validate_dataframe,
)


def test_validate_dataframe_creates_log_and_reports(tmp_path: Path) -> None:
    df = pd.DataFrame({"respondent_id": [1, 2], "age": [30, 28]})
    schema = SchemaDefinition(
        columns=[
            ColumnSchema(name="respondent_id", dtype="integer", nullable=False),
            ColumnSchema(name="age", dtype="number", minimum=18, maximum=99),
            ColumnSchema(name="segment", dtype="string", required=False),
        ]
    )

    log_path = tmp_path / "validation.jsonl"
    report = validate_dataframe(df, schema, log_path=log_path, halt_on_error=False)

    assert log_path.exists()
    entries = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    assert entries[0]["data_signature"] == report.data_signature
    assert len(report.warnings) == 1  # optional column missing
    assert not report.errors


def test_validate_dataframe_raises_on_missing_required_column(tmp_path: Path) -> None:
    df = pd.DataFrame({"age": [34]})
    schema = SchemaDefinition(columns=[ColumnSchema(name="respondent_id", dtype="integer")])

    with pytest.raises(ValidationError):
        validate_dataframe(df, schema, log_path=tmp_path / "log.jsonl", halt_on_error=True)
