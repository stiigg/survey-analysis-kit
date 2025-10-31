# Trust Checklist

* **Provenance:** Ensure `provenance.manifest.json` and lineage artefacts exist and are linked in audit logs.
* **Integrity:** Confirm the latest `integrity.manifest.*.json` points to the previous manifest and hashes validate.
* **Audit:** Review `audit.jsonl` for WARN/ERROR levels; document dispositions for any findings.
* **Privacy:** Verify no unexpected `pii_hits_detected` events and confirm `retention.json` is present.
* **Bias:** Inspect `bias_checks.csv`, `bias_subgroup_outliers.csv`, and `fairness_parity.csv` for notable gaps.
* **Reporting:** Validate `report.md` renders with charts exported to PNG/SVG and matches brand guidelines.
* **Repro:** Run `tools/pipeline_compliance.py` and `tools/repro_score.py` to confirm reproducibility posture.
