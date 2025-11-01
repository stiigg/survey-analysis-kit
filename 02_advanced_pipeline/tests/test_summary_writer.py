import pytest

pd = pytest.importorskip("pandas")

from surveykit.summary_writer import generate_summary, verify_summary_against_stats


def test_generate_summary_returns_findings() -> None:
    df = pd.DataFrame(
        {
            "satisfaction": [4, 5, 3],
            "segment": ["A", "A", "B"],
        }
    )
    doc = generate_summary(df)
    topics = [finding.topic for finding in doc.findings]
    assert any("satisfaction" in topic for topic in topics)
    assert any("segment" in topic for topic in topics)


def test_verify_summary_against_stats_flags_mismatch() -> None:
    df = pd.DataFrame({"score": [1, 2, 3]})
    doc = generate_summary(df)
    stats = {"score (numeric)": {"mean": 999, "std": 1}}

    mismatches = verify_summary_against_stats(doc, stats, tolerance=0.1)
    assert mismatches
