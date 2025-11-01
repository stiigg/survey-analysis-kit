# Chart Styling and Export Playbook

The advanced pipeline centralises brand styling so that every artefact is on-message for client presentations.

## Config-driven themes

* Store fonts, colour palettes, and background preferences in `chart_config.example.yaml` (or a client-specific variant).
* Update the pipeline configuration `charts.theme` with the path to the chosen YAML file.
* `surveykit.chart_style.load_theme` parses the theme and `apply_matplotlib_theme` broadcasts the settings to matplotlib before any charts render.

## Automated renders

* Chart specifications defined in the config automatically produce CSV extracts and PNG renders inside `outputs/charts/`.
* Each render is accompanied by a `.meta.json` file that captures the data signature and creation timestamp for auditability.

## PowerPoint / PDF handoff

* Generated CSVs and PNGs can be dropped directly into PowerPoint or PDF templates.
* For templated decks, consider wrapping the output directory with `python-pptx` or `reportlab` scripts—this repository focuses on producing clean, validated chart assets you can reuse downstream.
