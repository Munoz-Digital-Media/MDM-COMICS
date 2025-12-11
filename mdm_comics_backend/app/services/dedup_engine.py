"""
Fuzzy Matching and Deduplication Engine v1.0.0

Per 20251207_MDM_COMICS_DATA_ACQUISITION_PIPELINE.json:
- Multi-key fuzzy logic (title, series, issue #, upc, variant, date)
- Confidence scoring based on source trust, recency, and data completeness
- Quarantine low-confidence/fuzzy matches for review

This engine handles:
1. Fuzzy matching for potential duplicates
2. Confidence scoring for data quality
3. Conflict resolution between sources
4. Merge operations with field-level tracking
"""
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from app.core.utils import utcnow

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Result of a fuzzy match comparison."""
    is_match: bool
    confidence: float  # 0.0 to 1.0
    match_details: Dict[str, float] = field(default_factory=dict)
    matched_keys: List[str] = field(default_factory=list)
    needs_review: bool = False
    review_reason: Optional[str] = None


@dataclass
class SourceWeight:
    """Weight configuration for a data source."""
    name: str
    trust_weight: float = 1.0  # Base trust (0.0-2.0)
    recency_weight: float = 1.0  # How much recency matters
    completeness_weight: float = 1.0  # How much completeness matters


# Default source weights - higher = more trusted for conflicts
DEFAULT_SOURCE_WEIGHTS = {
    "pricecharting": SourceWeight("pricecharting", trust_weight=1.2, recency_weight=1.5),
    "metron": SourceWeight("metron", trust_weight=1.3, recency_weight=1.0),
    "gcd": SourceWeight("gcd", trust_weight=1.1, recency_weight=0.8),  # Less recent but authoritative
    "marvel_fandom": SourceWeight("marvel_fandom", trust_weight=0.9, recency_weight=1.0),
    "manual": SourceWeight("manual", trust_weight=1.5, recency_weight=0.5),  # Manual overrides are trusted
}


class FuzzyMatcher:
    """
    Fuzzy matching engine for comic/collectible records.

    Supports multiple matching strategies:
    - Exact match on unique identifiers (UPC, ISBN, SKU)
    - Fuzzy match on title + series + issue number
    - Partial match with confidence scoring
    """

    # Thresholds for matching
    EXACT_MATCH_THRESHOLD = 0.95
    FUZZY_MATCH_THRESHOLD = 0.75
    REVIEW_THRESHOLD = 0.60

    def __init__(
        self,
        exact_match_threshold: float = EXACT_MATCH_THRESHOLD,
        fuzzy_match_threshold: float = FUZZY_MATCH_THRESHOLD,
        review_threshold: float = REVIEW_THRESHOLD,
    ):
        self.exact_match_threshold = exact_match_threshold
        self.fuzzy_match_threshold = fuzzy_match_threshold
        self.review_threshold = review_threshold

    def normalize_text(self, text: Optional[str]) -> str:
        """Normalize text for comparison."""
        if not text:
            return ""

        # Lowercase
        text = text.lower()

        # Remove common prefixes/suffixes
        prefixes = ["the ", "a ", "an "]
        for prefix in prefixes:
            if text.startswith(prefix):
                text = text[len(prefix):]

        # Remove special characters except alphanumeric and spaces
        text = re.sub(r"[^a-z0-9\s]", "", text)

        # Normalize whitespace
        text = " ".join(text.split())

        return text

    def normalize_issue_number(self, number: Optional[str]) -> str:
        """Normalize issue number for comparison."""
        if not number:
            return ""

        number = str(number).lower().strip()

        # Remove # prefix
        number = number.lstrip("#")

        # Handle common issue number formats
        # "001" -> "1"
        # "1A" -> "1a"
        # "Annual 1" -> "annual 1"

        # Try to extract numeric part
        match = re.match(r"(\d+)", number)
        if match:
            num_part = str(int(match.group(1)))  # Remove leading zeros
            rest = number[match.end():].strip()
            return f"{num_part}{rest}"

        return number

    def similarity_ratio(self, a: str, b: str) -> float:
        """Calculate similarity ratio between two strings."""
        if not a and not b:
            return 1.0
        if not a or not b:
            return 0.0

        return SequenceMatcher(None, a, b).ratio()

    def match_comics(
        self,
        record1: Dict[str, Any],
        record2: Dict[str, Any],
    ) -> MatchResult:
        """
        Compare two comic records for potential match.

        Checks multiple keys with weighted scoring:
        - UPC: Exact match = definite match
        - ISBN: Exact match = definite match
        - Title + Series + Issue: Fuzzy match
        - Cover date: Approximate match
        - Variant name: Fuzzy match
        """
        scores = {}
        matched_keys = []

        # EXACT IDENTIFIERS - immediate match if present
        # UPC match
        upc1 = record1.get("upc", "").strip()
        upc2 = record2.get("upc", "").strip()
        if upc1 and upc2:
            if upc1 == upc2:
                return MatchResult(
                    is_match=True,
                    confidence=1.0,
                    match_details={"upc": 1.0},
                    matched_keys=["upc"],
                )
            else:
                # Different UPCs = definitely different
                return MatchResult(
                    is_match=False,
                    confidence=0.0,
                    match_details={"upc": 0.0},
                )

        # ISBN match
        isbn1 = record1.get("isbn", "").replace("-", "").strip()
        isbn2 = record2.get("isbn", "").replace("-", "").strip()
        if isbn1 and isbn2:
            if isbn1 == isbn2:
                return MatchResult(
                    is_match=True,
                    confidence=1.0,
                    match_details={"isbn": 1.0},
                    matched_keys=["isbn"],
                )
            else:
                return MatchResult(
                    is_match=False,
                    confidence=0.0,
                    match_details={"isbn": 0.0},
                )

        # FUZZY MATCHING
        # Series name (weight: 0.3)
        series1 = self.normalize_text(record1.get("series_name", ""))
        series2 = self.normalize_text(record2.get("series_name", ""))
        if series1 and series2:
            scores["series_name"] = self.similarity_ratio(series1, series2)
            if scores["series_name"] >= self.fuzzy_match_threshold:
                matched_keys.append("series_name")

        # Issue number (weight: 0.25)
        num1 = self.normalize_issue_number(record1.get("number", ""))
        num2 = self.normalize_issue_number(record2.get("number", ""))
        if num1 and num2:
            scores["number"] = 1.0 if num1 == num2 else 0.0
            if scores["number"] >= self.fuzzy_match_threshold:
                matched_keys.append("number")

        # Title/Issue name (weight: 0.2)
        title1 = self.normalize_text(
            record1.get("issue_name", "") or record1.get("title", "")
        )
        title2 = self.normalize_text(
            record2.get("issue_name", "") or record2.get("title", "")
        )
        if title1 and title2:
            scores["title"] = self.similarity_ratio(title1, title2)
            if scores["title"] >= self.fuzzy_match_threshold:
                matched_keys.append("title")

        # Cover date (weight: 0.15)
        date1 = record1.get("cover_date")
        date2 = record2.get("cover_date")
        if date1 and date2:
            # Parse dates if needed
            if isinstance(date1, str):
                date1 = date1[:10]  # Just the date part
            if isinstance(date2, str):
                date2 = date2[:10]
            scores["cover_date"] = 1.0 if date1 == date2 else 0.0
            if scores["cover_date"] >= 0.5:
                matched_keys.append("cover_date")

        # Variant name (weight: 0.1)
        var1 = self.normalize_text(record1.get("variant_name", ""))
        var2 = self.normalize_text(record2.get("variant_name", ""))
        if var1 or var2:
            if var1 and var2:
                scores["variant_name"] = self.similarity_ratio(var1, var2)
            elif var1 == var2:  # Both empty
                scores["variant_name"] = 1.0
            else:
                scores["variant_name"] = 0.5  # One has variant, one doesn't
            if scores["variant_name"] >= self.fuzzy_match_threshold:
                matched_keys.append("variant_name")

        # Calculate weighted average
        weights = {
            "series_name": 0.30,
            "number": 0.25,
            "title": 0.20,
            "cover_date": 0.15,
            "variant_name": 0.10,
        }

        total_weight = 0
        weighted_sum = 0
        for key, weight in weights.items():
            if key in scores:
                weighted_sum += scores[key] * weight
                total_weight += weight

        if total_weight > 0:
            confidence = weighted_sum / total_weight
        else:
            confidence = 0.0

        # Determine match status
        is_match = confidence >= self.fuzzy_match_threshold
        needs_review = (
            self.review_threshold <= confidence < self.exact_match_threshold
        )

        review_reason = None
        if needs_review:
            if confidence < self.fuzzy_match_threshold:
                review_reason = "Low confidence match - manual verification needed"
            elif "number" not in matched_keys:
                review_reason = "Issue number mismatch - may be different issues"
            elif "variant_name" in scores and scores["variant_name"] < 0.5:
                review_reason = "Possible variant conflict"

        return MatchResult(
            is_match=is_match,
            confidence=confidence,
            match_details=scores,
            matched_keys=matched_keys,
            needs_review=needs_review,
            review_reason=review_reason,
        )


class ConfidenceScorer:
    """
    Calculate confidence scores for data quality.

    Considers:
    - Source trust weight
    - Data recency
    - Field completeness
    """

    def __init__(self, source_weights: Optional[Dict[str, SourceWeight]] = None):
        self.source_weights = source_weights or DEFAULT_SOURCE_WEIGHTS

    def calculate_completeness(
        self,
        record: Dict[str, Any],
        required_fields: List[str],
        optional_fields: Optional[List[str]] = None,
    ) -> float:
        """
        Calculate completeness score for a record.

        Args:
            record: The data record
            required_fields: Fields that must be present
            optional_fields: Fields that add to completeness

        Returns:
            Completeness score (0.0 to 1.0)
        """
        if not required_fields:
            return 1.0

        # Check required fields (60% weight)
        required_present = sum(
            1 for f in required_fields
            if record.get(f) is not None and record.get(f) != ""
        )
        required_score = required_present / len(required_fields)

        # Check optional fields (40% weight)
        if optional_fields:
            optional_present = sum(
                1 for f in optional_fields
                if record.get(f) is not None and record.get(f) != ""
            )
            optional_score = optional_present / len(optional_fields)
        else:
            optional_score = 1.0

        return (required_score * 0.6) + (optional_score * 0.4)

    def calculate_recency(
        self,
        fetched_at: Optional[datetime],
        max_age_days: int = 30,
    ) -> float:
        """
        Calculate recency score based on when data was fetched.

        Args:
            fetched_at: When the data was last fetched
            max_age_days: Age at which recency score becomes 0

        Returns:
            Recency score (0.0 to 1.0)
        """
        if not fetched_at:
            return 0.5  # Unknown recency gets neutral score

        now = utcnow()
        if hasattr(fetched_at, 'tzinfo') and fetched_at.tzinfo is None:
            from datetime import timezone
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)

        age = now - fetched_at
        age_days = age.total_seconds() / 86400

        if age_days <= 0:
            return 1.0
        elif age_days >= max_age_days:
            return 0.0
        else:
            return 1.0 - (age_days / max_age_days)

    def calculate_confidence(
        self,
        record: Dict[str, Any],
        source: str,
        fetched_at: Optional[datetime] = None,
        required_fields: Optional[List[str]] = None,
        optional_fields: Optional[List[str]] = None,
    ) -> float:
        """
        Calculate overall confidence score for a record.

        Combines source trust, recency, and completeness.
        """
        # Get source weight
        source_weight = self.source_weights.get(
            source,
            SourceWeight(source)
        )

        # Calculate component scores
        completeness = self.calculate_completeness(
            record,
            required_fields or [],
            optional_fields
        )
        recency = self.calculate_recency(fetched_at)

        # Weighted combination
        score = (
            (source_weight.trust_weight * 0.4) +
            (completeness * source_weight.completeness_weight * 0.35) +
            (recency * source_weight.recency_weight * 0.25)
        )

        # Normalize to 0-1 range
        return min(1.0, max(0.0, score / 2.0))


class ConflictResolver:
    """
    Resolve conflicts between data from different sources.

    Uses source priority and confidence scoring to determine
    which value to use for each field.
    """

    def __init__(self, scorer: Optional[ConfidenceScorer] = None):
        self.scorer = scorer or ConfidenceScorer()

    def resolve_field(
        self,
        field_name: str,
        values: List[Tuple[str, Any, datetime, float]],  # (source, value, fetched_at, confidence)
    ) -> Tuple[Any, str, float, bool]:
        """
        Resolve a single field conflict.

        Args:
            field_name: Name of the field
            values: List of (source, value, fetched_at, confidence) tuples

        Returns:
            Tuple of (chosen_value, chosen_source, confidence, needs_review)
        """
        if not values:
            return None, "", 0.0, False

        if len(values) == 1:
            source, value, _, confidence = values[0]
            return value, source, confidence, False

        # Filter out None/empty values
        valid_values = [
            v for v in values
            if v[1] is not None and v[1] != ""
        ]

        if not valid_values:
            return None, "", 0.0, False

        if len(valid_values) == 1:
            source, value, _, confidence = valid_values[0]
            return value, source, confidence, False

        # Check if all values are the same
        unique_values = set(str(v[1]) for v in valid_values)
        if len(unique_values) == 1:
            # All sources agree - use highest confidence source
            best = max(valid_values, key=lambda x: x[3])
            return best[1], best[0], best[3], False

        # Values differ - this is a conflict
        # Sort by confidence, then by source priority
        sorted_values = sorted(
            valid_values,
            key=lambda x: (x[3], self._source_priority(x[0])),
            reverse=True
        )

        best = sorted_values[0]
        second_best = sorted_values[1] if len(sorted_values) > 1 else None

        # Determine if review is needed
        needs_review = False
        if second_best:
            confidence_gap = best[3] - second_best[3]
            if confidence_gap < 0.2:  # Close confidence - ambiguous
                needs_review = True

        return best[1], best[0], best[3], needs_review

    def _source_priority(self, source: str) -> int:
        """Get priority order for a source (higher = better)."""
        priorities = {
            "manual": 100,
            "metron": 80,
            "pricecharting": 70,
            "gcd": 60,
            "marvel_fandom": 50,
        }
        return priorities.get(source, 0)

    def merge_records(
        self,
        records: List[Tuple[str, Dict[str, Any], datetime]],  # (source, record, fetched_at)
        fields_to_merge: List[str],
    ) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]], bool]:
        """
        Merge multiple records from different sources.

        Args:
            records: List of (source, record, fetched_at) tuples
            fields_to_merge: Fields to include in merged record

        Returns:
            Tuple of (merged_record, provenance, needs_review)
        """
        merged = {}
        provenance = {}
        any_needs_review = False

        for field in fields_to_merge:
            # Collect all values for this field
            values = []
            for source, record, fetched_at in records:
                value = record.get(field)
                if value is not None:
                    confidence = self.scorer.calculate_confidence(
                        record, source, fetched_at
                    )
                    values.append((source, value, fetched_at, confidence))

            # Resolve the field
            chosen_value, chosen_source, confidence, needs_review = \
                self.resolve_field(field, values)

            merged[field] = chosen_value
            provenance[field] = {
                "source": chosen_source,
                "confidence": confidence,
                "needs_review": needs_review,
                "alternatives": len(values) - 1 if values else 0,
            }

            if needs_review:
                any_needs_review = True

        return merged, provenance, any_needs_review


# Singleton instances for easy access
fuzzy_matcher = FuzzyMatcher()
confidence_scorer = ConfidenceScorer()
conflict_resolver = ConflictResolver(confidence_scorer)
