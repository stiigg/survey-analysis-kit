# Summary Review Dashboard

The pipeline now creates a narrative summary (`outputs/summary.md` and `outputs/summary.json`) that captures key quantitative and categorical insights.

## Streamlit reviewer

* Launch `python -m streamlit run tools/summary_review_app.py` to open the review interface.
* Pass `?summary=path/to/summary.json&feedback=path/to/feedback.json` in the URL to read from alternate locations.
* Reviewers can approve, request revisions, or leave freeform comments per insight. Feedback is appended to the provided feedback log in JSON format.

## Command-line approval tracker

* Use `python tools/review_tracker.py <identifier> <status> --comment "..."` to log approvals or revisions for charts, tables, or summaries.
* Status files are stored at `outputs/review_status.json` by default and can be version-controlled for complete audit trails.

## Cross-check automation

* `surveykit.summary_writer.verify_summary_against_stats` compares narrative evidence with numerical checks to ensure alignment between text and figures.
* Warnings are emitted to the audit log if mismatches are detected.
