"""Data validation utilities for the SurveyKit advanced pipeline."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd
try:  # optional dependency used when schema files are YAML
    import yaml as _yaml
except ModuleNotFoundError:  # pragma: no cover - the module is optional
    _yaml = None


class ValidationError(RuntimeError):
    """Raised when validation detects blocking issues."""


@dataclass
class ColumnSchema:
    """Definition of an expected column in the survey dataset."""

    name: str
    dtype: str
    required: bool = True
    nullable: bool = True
    allowed_values: Optional[Sequence[Any]] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    notes: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "dtype": self.dtype,
            "required": self.required,
            "nullable": self.nullable,
            "allowed_values": list(self.allowed_values) if self.allowed_values else None,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "notes": self.notes,
        }


@dataclass
class SchemaDefinition:
    """Collection of column definitions with optional metadata."""

    columns: List[ColumnSchema]
    version: Optional[str] = None
    description: Optional[str] = None

    @classmethod
    def from_mapping(cls, mapping: Dict[str, Any]) -> "SchemaDefinition":
        cols = [ColumnSchema(**raw) for raw in mapping.get("columns", [])]
        return cls(columns=cols, version=mapping.get("version"), description=mapping.get("description"))


@dataclass
class ValidationIssue:
    level: str
    column: Optional[str]
    message: str
    context: Dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "level": self.level,
            "column": self.column,
            "message": self.message,
            "context": self.context,
        }


@dataclass
class ValidationReport:
    """Structured validation result with helper accessors."""

    issues: List[ValidationIssue]
    data_signature: str
    schema_version: Optional[str] = None
    validated_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def errors(self) -> List[ValidationIssue]:
        return [issue for issue in self.issues if issue.level == "ERROR"]

    @property
    def warnings(self) -> List[ValidationIssue]:
        return [issue for issue in self.issues if issue.level == "WARN"]

    def has_errors(self) -> bool:
        return bool(self.errors)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "issues": [issue.as_dict() for issue in self.issues],
            "data_signature": self.data_signature,
            "schema_version": self.schema_version,
            "validated_at": self.validated_at.isoformat(),
        }

    def to_json(self, indent: Optional[int] = None) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------


def load_schema(path: Path) -> SchemaDefinition:
    """Load a schema definition from YAML or JSON."""

    if not path.exists():
        raise FileNotFoundError(f"Schema file not found: {path}")

    if path.suffix.lower() in {".yaml", ".yml"}:
        if _yaml is None:
            raise RuntimeError("PyYAML is required to load YAML schemas.")
        with path.open("r", encoding="utf-8") as fh:
            data = _yaml.safe_load(fh)
    else:
        data = json.loads(path.read_text(encoding="utf-8"))
    return SchemaDefinition.from_mapping(data)


# ---------------------------------------------------------------------------
# Validation logic
# ---------------------------------------------------------------------------


_PANDAS_TYPE_CHECKS: Dict[str, Iterable] = {
    "integer": (pd.api.types.is_integer_dtype,),
    "float": (pd.api.types.is_float_dtype,),
    "number": (
        pd.api.types.is_float_dtype,
        pd.api.types.is_integer_dtype,
    ),
    "string": (pd.api.types.is_string_dtype, pd.api.types.is_object_dtype),
    "boolean": (pd.api.types.is_bool_dtype,),
    "datetime": (pd.api.types.is_datetime64_any_dtype,),
    "category": (pd.api.types.is_categorical_dtype,),
}


def _series_has_dtype(series: pd.Series, expected: str) -> bool:
    expected_lower = expected.lower()
    checkers = _PANDAS_TYPE_CHECKS.get(expected_lower)
    if checkers is None:
        raise ValueError(f"Unsupported dtype in schema: {expected}")
    return any(checker(series.dtype) for checker in checkers)


def _hash_dataframe(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()


def validate_dataframe(
    df: pd.DataFrame,
    schema: SchemaDefinition,
    *,
    log_path: Optional[Path] = None,
    halt_on_error: bool = True,
) -> ValidationReport:
    """Validate a dataframe against a schema."""

    issues: List[ValidationIssue] = []
    present_columns = set(df.columns)

    for col_schema in schema.columns:
        name = col_schema.name
        if name not in df.columns:
            if col_schema.required:
                issues.append(
                    ValidationIssue(
                        level="ERROR",
                        column=name,
                        message="Missing required column",
                    )
                )
            else:
                issues.append(
                    ValidationIssue(
                        level="WARN",
                        column=name,
                        message="Optional column missing",
                    )
                )
            continue

        series = df[name]
        if not col_schema.nullable and series.isna().any():
            issues.append(
                ValidationIssue(
                    level="ERROR",
                    column=name,
                    message="Null values not permitted",
                    context={"null_count": int(series.isna().sum())},
                )
            )

        try:
            dtype_ok = _series_has_dtype(series, col_schema.dtype)
        except ValueError as exc:
            issues.append(ValidationIssue(level="ERROR", column=name, message=str(exc)))
            dtype_ok = True  # avoid duplicate messages
        if not dtype_ok:
            issues.append(
                ValidationIssue(
                    level="ERROR",
                    column=name,
                    message="Unexpected dtype",
                    context={"observed": str(series.dtype), "expected": col_schema.dtype},
                )
            )

        if col_schema.allowed_values is not None:
            invalid_values = sorted(set(series.dropna().unique()) - set(col_schema.allowed_values))
            if invalid_values:
                issues.append(
                    ValidationIssue(
                        level="ERROR",
                        column=name,
                        message="Values outside allowed set",
                        context={"invalid_values": invalid_values},
                    )
                )

        if col_schema.minimum is not None:
            below = series.dropna() < col_schema.minimum
            if below.any():
                issues.append(
                    ValidationIssue(
                        level="ERROR",
                        column=name,
                        message="Values below minimum",
                        context={"minimum": col_schema.minimum, "count": int(below.sum())},
                    )
                )
        if col_schema.maximum is not None:
            above = series.dropna() > col_schema.maximum
            if above.any():
                issues.append(
                    ValidationIssue(
                        level="ERROR",
                        column=name,
                        message="Values above maximum",
                        context={"maximum": col_schema.maximum, "count": int(above.sum())},
                    )
                )

        present_columns.discard(name)

    for extra in sorted(present_columns):
        issues.append(
            ValidationIssue(
                level="WARN",
                column=extra,
                message="Unexpected column present",
            )
        )

    report = ValidationReport(
        issues=issues,
        data_signature=_hash_dataframe(df),
        schema_version=schema.version,
    )

    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"ts": datetime.utcnow().isoformat(), **report.to_dict()}) + "\n")

    if halt_on_error and report.has_errors():
        raise ValidationError(f"Validation failed with {len(report.errors)} error(s)")

    return report


def validate_csv(
    csv_path: Path,
    schema_path: Path,
    *,
    log_path: Optional[Path] = None,
    halt_on_error: bool = True,
    **read_csv_kwargs: Any,
) -> tuple[pd.DataFrame, ValidationReport]:
    """Load and validate a CSV file in one go."""

    df = pd.read_csv(csv_path, **read_csv_kwargs)
    schema = load_schema(schema_path)
    report = validate_dataframe(df, schema, log_path=log_path, halt_on_error=halt_on_error)
    return df, report


def save_summary(report: ValidationReport, destination: Path) -> None:
    """Persist a human-readable audit summary."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "validated_at": report.validated_at.isoformat(),
        "schema_version": report.schema_version,
        "data_signature": report.data_signature,
        "error_count": len(report.errors),
        "warning_count": len(report.warnings),
        "issues": [issue.as_dict() for issue in report.issues],
    }
    destination.write_text(json.dumps(summary, indent=2), encoding="utf-8")


__all__ = [
    "ColumnSchema",
    "SchemaDefinition",
    "ValidationError",
    "ValidationIssue",
    "ValidationReport",
    "load_schema",
    "validate_dataframe",
    "validate_csv",
    "save_summary",
]
