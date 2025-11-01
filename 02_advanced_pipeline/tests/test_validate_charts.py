import pytest

pd = pytest.importorskip("pandas")

from surveykit.validate_charts import ChartSpec, validate_charts


def test_validate_charts_detects_stale_chart() -> None:
    df = pd.DataFrame({"segment": ["A", "A", "B"], "satisfaction": [4, 5, 3]})
    aggregated = df.groupby("segment")["satisfaction"].mean().reset_index()
    specs = [
        ChartSpec(
            identifier="satisfaction_by_segment",
            kind="bar",
            x="segment",
            y="satisfaction",
            aggregation="mean",
            data_signature="old_signature",
        )
    ]
    charts = {"satisfaction_by_segment": aggregated}

    report = validate_charts(df, specs, charts)
    assert report.has_errors()
    assert any(issue.message.startswith("Chart is stale") for issue in report.issues)


def test_validate_charts_happy_path() -> None:
    df = pd.DataFrame({"segment": ["A", "A", "B"], "satisfaction": [4, 5, 3]})
    aggregated = df.groupby("segment")["satisfaction"].mean().reset_index()
    specs = [
        ChartSpec(
            identifier="fresh_chart",
            kind="bar",
            x="segment",
            y="satisfaction",
            aggregation="mean",
        )
    ]
    charts = {"fresh_chart": aggregated}

    report = validate_charts(df, specs, charts)
    assert not report.has_errors()
