"""Lightweight NLP helpers for open-ended survey responses."""

from __future__ import annotations

from collections import Counter
from typing import Dict, Iterable


try:  # optional dependency for better sentiment
    from textblob import TextBlob  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    TextBlob = None  # type: ignore


def sentiment_score(text: str) -> float:
    """Return a polarity score between -1 and 1."""

    if not text:
        return 0.0
    if TextBlob is None:
        positive_words = {"good", "great", "love", "excellent", "satisfied"}
        negative_words = {"bad", "poor", "hate", "terrible", "unsatisfied"}
        tokens = text.lower().split()
        score = sum(1 for t in tokens if t in positive_words) - sum(1 for t in tokens if t in negative_words)
        return max(-1.0, min(1.0, score / max(len(tokens), 1)))
    blob = TextBlob(text)
    return float(blob.sentiment.polarity)


def keyword_counts(responses: Iterable[str], top_n: int = 20) -> Dict[str, int]:
    counter: Counter[str] = Counter()
    for text in responses:
        counter.update(token for token in text.lower().split())
    return dict(counter.most_common(top_n))


def word_cloud_frequencies(responses: Iterable[str]) -> Dict[str, int]:
    return keyword_counts(responses, top_n=200)


__all__ = ["sentiment_score", "keyword_counts", "word_cloud_frequencies"]
