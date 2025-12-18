import pytest

from app.services.match_review_service import MatchDisposition, route_match


@pytest.mark.parametrize(
    "method, score, candidates, expected",
    [
        ("isbn", 1, 1, MatchDisposition.AUTO_LINK),
        ("upc", 1, 2, MatchDisposition.REVIEW_QUEUE),
        ("fuzzy", 9, 0, MatchDisposition.AUTO_LINK),
        ("fuzzy", 7, 0, MatchDisposition.REVIEW_QUEUE),
        ("fuzzy", 3, 0, MatchDisposition.NO_MATCH),
    ],
)
def test_route_match_behaviour(method, score, candidates, expected):
    """Route thresholds are enforced and exact-id matches auto-link when unique."""
    assert route_match(method, score, candidates) == expected
