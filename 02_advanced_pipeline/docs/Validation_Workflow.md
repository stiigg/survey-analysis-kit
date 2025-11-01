# Automated Validation Workflow

This pipeline ships with a fully automated validation layer so that consultants and clients can trace every data decision.

## Data validation

1. Configure the dataset schema in `schema.example.yaml` (or your own schema file) with expected columns, types, value ranges, and enumerations.
2. Reference the schema from your pipeline configuration under `data.schema`.
3. On execution, `surveykit.validate_data.validate_dataframe` halts the pipeline if blocking errors are encountered. A JSONL audit trail is written to `outputs/audit/validation.jsonl` and a summary report to `outputs/audit/validation_summary.json`.
4. Use the generated summary to populate client audit packs or internal QA reviews.

## Chart validation

* Define chart specifications in the config file under `charts.specs`.
* Each chart specification is represented by a `surveykit.validate_charts.ChartSpec` instance with filters, aggregation logic, and the originating data signature.
* The CLI assembles chart data, saves CSV/PNG artefacts, and validates every output against the source dataframe. Validation findings are stored at `outputs/audit/chart_validation.json` for compliance packages.

## Continuous validation hooks

* The CLI raises a `ValidationError` when the schema check fails, preventing downstream notebooks from working with bad data.
* Chart validation compares creation metadata with the dataset hash so stale charts are surfaced immediately during automation runs.
