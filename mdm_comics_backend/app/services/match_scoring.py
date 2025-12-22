"""
Match Scoring Service for PriceCharting Integration v1.0.0

Document ID: PC-OPT-2024-001 Phase 3
Status: APPROVED

Multi-factor match scoring to reduce false positives in comic/funko matching.

Current Problem:
- Simple substring matching causes false positives
- "Spider-Man" matches "Ultimate Spider-Man"
- No confidence scoring for match quality

Solution:
- Multi-factor scoring (title, year, publisher, issue number)
- Configurable threshold (default: 0.6)
- Confidence levels (high, medium, low)
- Logging for match analysis

Per constitution_cyberSec.json: Input validation + data quality
"""
import logging
import re
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Scoring thresholds
MATCH_THRESHOLD = 0.6  # Minimum score to consider a match (comics)
FUNKO_MATCH_THRESHOLD = 0.4  # Lower threshold for Funkos (less metadata available)
HIGH_CONFIDENCE_THRESHOLD = 0.8  # Score for "high" confidence
LOW_CONFIDENCE_THRESHOLD = 0.4  # Below this, match is rejected


@dataclass
class MatchResult:
    """Result of a match attempt."""
    matched: bool
    pricecharting_id: Optional[int]
    score: float
    factors: Dict[str, float]
    confidence: str  # "high", "medium", "low", "none"
    product_name: Optional[str] = None  # For logging


def normalize_title(title: str) -> str:
    """
    Normalize title for comparison.

    - Lowercase
    - Remove punctuation
    - Normalize whitespace
    - Remove common prefixes/suffixes
    """
    if not title:
        return ""

    # Lowercase
    title = title.lower()

    # Remove punctuation except #
    title = re.sub(r'[^\w\s#]', ' ', title)

    # Normalize whitespace
    title = ' '.join(title.split())

    return title


def extract_issue_number(text: str) -> Optional[str]:
    """
    Extract issue number from text.

    Handles formats: #1, #01, #001, Issue 1, Vol 1, etc.
    """
    if not text:
        return None

    # Common patterns
    patterns = [
        r'#(\d+)',           # #1, #01, #001
        r'issue\s*(\d+)',    # Issue 1
        r'vol\s*(\d+)',      # Vol 1
        r'\s(\d+)$',         # Trailing number
    ]

    for pattern in patterns:
        match = re.search(pattern, text.lower())
        if match:
            return match.group(1).lstrip('0') or '0'

    return None


def extract_year(text: str) -> Optional[int]:
    """
    Extract year from text (1900-2099).
    """
    if not text:
        return None

    match = re.search(r'\b(19|20)\d{2}\b', str(text))
    if match:
        return int(match.group(0))

    return None


def calculate_match_score(
    item: Dict,
    product: Dict,
    item_type: str = "comic",
) -> MatchResult:
    """
    Calculate multi-factor match score.

    Factors for comics:
    - Title match: 0.4 (exact) or 0.2 (substring)
    - Year match: 0.2
    - Publisher match: 0.2
    - Issue number in name: 0.2

    Factors for funkos:
    - Title match: 0.5 (exact) or 0.25 (substring)
    - Category match: 0.25 (funko/pop in name)
    - Box number match: 0.25

    Args:
        item: Local record dict (comic or funko)
        product: PriceCharting product dict
        item_type: "comic" or "funko"

    Returns:
        MatchResult with score and factor breakdown
    """
    score = 0.0
    factors = {}

    product_name = product.get("product-name", "")
    normalized_product = normalize_title(product_name)

    if item_type == "comic":
        return _score_comic_match(item, product, normalized_product)
    else:
        return _score_funko_match(item, product, normalized_product)


def _score_comic_match(
    comic: Dict,
    product: Dict,
    normalized_product: str,
) -> MatchResult:
    """Score a comic match."""
    score = 0.0
    factors = {}

    series_name = comic.get("series_name", "")
    normalized_series = normalize_title(series_name)

    # === Title matching (max 0.4) ===
    if normalized_series and normalized_product:
        if normalized_series == normalized_product:
            factors["title_exact"] = 0.4
            score += 0.4
        elif normalized_series in normalized_product:
            factors["title_substring"] = 0.2
            score += 0.2
        elif normalized_product in normalized_series:
            factors["title_reverse_substring"] = 0.15
            score += 0.15
        else:
            # Check word overlap
            series_words = set(normalized_series.split())
            product_words = set(normalized_product.split())
            if series_words and product_words:
                overlap = len(series_words & product_words)
                overlap_ratio = overlap / len(series_words)
                if overlap_ratio >= 0.8:
                    factors["title_word_overlap"] = 0.25
                    score += 0.25
                elif overlap_ratio >= 0.5:
                    factors["title_partial_overlap"] = 0.1
                    score += 0.1

    # === Year match (max 0.2) ===
    comic_year = comic.get("year") or comic.get("cover_date", "")
    if comic_year:
        comic_year_str = str(comic_year)[:4]
        console_name = product.get("console-name", "")
        product_year = extract_year(console_name) or extract_year(product.get("product-name", ""))

        if comic_year_str.isdigit() and product_year:
            if int(comic_year_str) == product_year:
                factors["year_exact"] = 0.2
                score += 0.2
            elif abs(int(comic_year_str) - product_year) <= 1:
                factors["year_close"] = 0.1
                score += 0.1

    # === Publisher match (max 0.2) ===
    publisher = normalize_title(comic.get("publisher_name", ""))
    console_name = normalize_title(product.get("console-name", ""))
    if publisher and console_name:
        if publisher in console_name or console_name in publisher:
            factors["publisher_match"] = 0.2
            score += 0.2

    # === Issue number in product name (max 0.2) ===
    issue_num = comic.get("number", "")
    if issue_num:
        product_issue = extract_issue_number(normalized_product)
        item_issue = str(issue_num).lstrip('0') or '0'

        if product_issue and product_issue == item_issue:
            factors["issue_number"] = 0.2
            score += 0.2
        elif f"#{issue_num}" in normalized_product.lower():
            factors["issue_hash_match"] = 0.15
            score += 0.15

    # Determine confidence
    if score >= HIGH_CONFIDENCE_THRESHOLD:
        confidence = "high"
    elif score >= MATCH_THRESHOLD:
        confidence = "medium"
    elif score >= LOW_CONFIDENCE_THRESHOLD:
        confidence = "low"
    else:
        confidence = "none"

    return MatchResult(
        matched=score >= MATCH_THRESHOLD,
        pricecharting_id=int(product.get("id")) if score >= MATCH_THRESHOLD else None,
        score=round(score, 2),
        factors=factors,
        confidence=confidence,
        product_name=product.get("product-name"),
    )


def _score_funko_match(
    funko: Dict,
    product: Dict,
    normalized_product: str,
) -> MatchResult:
    """Score a funko match. Uses lower threshold than comics due to sparser metadata."""
    score = 0.0
    factors = {}
    threshold = FUNKO_MATCH_THRESHOLD  # Use lower threshold for Funkos

    title = funko.get("title", "")
    normalized_title = normalize_title(title)
    category = normalize_title(funko.get("category", ""))
    license_name = normalize_title(funko.get("license", ""))

    series_names = funko.get("series_names") or ""
    normalized_series = normalize_title(series_names)

    # === Title matching (max 0.5) ===
    if normalized_title and normalized_product:
        if normalized_title == normalized_product:
            factors["title_exact"] = 0.35
            score += 0.35
        elif normalized_title in normalized_product:
            factors["title_substring"] = 0.25
            score += 0.25
        elif normalized_product in normalized_title:
            factors["title_reverse_substring"] = 0.2
            score += 0.2
        else:
            # Word overlap
            title_words = set(normalized_title.split())
            product_words = set(normalized_product.split())
            if title_words and product_words:
                overlap = len(title_words & product_words)
                overlap_ratio = overlap / len(title_words)
                if overlap_ratio >= 0.8:
                    factors["title_word_overlap"] = 0.3
                    score += 0.3
                elif overlap_ratio >= 0.5:
                    factors["title_partial_overlap"] = 0.15
                    score += 0.15

    # === Box number match (max 0.45) ===
    box_number = funko.get("box_number", "")
    if box_number:
        box_num_str = str(box_number).strip()
        if box_num_str and (box_num_str in normalized_product or f"#{box_num_str}" in normalized_product):
            factors["box_number"] = 0.45
            score += 0.45

    # === Category / series / license cues (max ~0.25 combined) ===
    if "funko" in normalized_product or "pop" in normalized_product:
        factors["category_funko"] = 0.1
        score += 0.1

    if category and category in normalized_product:
        factors["category_match"] = 0.1
        score += 0.1

    if normalized_series:
        series_tokens = set(normalized_series.split())
        product_tokens = set(normalized_product.split())
        if series_tokens and product_tokens:
            overlap = len(series_tokens & product_tokens)
            if overlap > 0:
                factors["series_overlap"] = 0.1
                score += 0.1

    if license_name and license_name in normalized_product:
        factors["license_match"] = 0.05
        score += 0.05

    # Determine confidence (using Funko-specific threshold)
    if score >= HIGH_CONFIDENCE_THRESHOLD:
        confidence = "high"
    elif score >= threshold:
        confidence = "medium"
    elif score >= LOW_CONFIDENCE_THRESHOLD:
        confidence = "low"
    else:
        confidence = "none"

    return MatchResult(
        matched=score >= threshold,
        pricecharting_id=int(product.get("id")) if score >= threshold else None,
        score=round(score, 2),
        factors=factors,
        confidence=confidence,
        product_name=product.get("product-name"),
    )


def find_best_match(
    item: Dict,
    products: List[Dict],
    item_type: str = "comic",
    threshold: float = None,  # Auto-selects based on item_type
    max_candidates: int = 10,
) -> Optional[MatchResult]:
    """
    Find best matching product above threshold.

    Args:
        item: Local record dict (comic or funko)
        products: List of PriceCharting products from search
        item_type: "comic" or "funko"
        threshold: Minimum score to consider (auto-selects if None)
        max_candidates: Max products to evaluate

    Returns:
        Best MatchResult above threshold, or None
    """
    # Auto-select threshold based on item type
    if threshold is None:
        threshold = FUNKO_MATCH_THRESHOLD if item_type == "funko" else MATCH_THRESHOLD

    best_match = None
    best_score = 0.0

    for product in products[:max_candidates]:
        result = calculate_match_score(item, product, item_type)

        if result.score > best_score and result.score >= threshold:
            best_match = result
            best_score = result.score

    if best_match:
        logger.debug(
            f"[MATCH] Best match: score={best_match.score}, "
            f"confidence={best_match.confidence}, "
            f"factors={best_match.factors}"
        )

    return best_match


def score_and_log_match(
    item: Dict,
    products: List[Dict],
    item_type: str = "comic",
    item_id: int = None,
    job_name: str = "match",
) -> Optional[MatchResult]:
    """
    Find best match and log details for analysis.

    Use this in jobs for visibility into match quality.

    Args:
        item: Local record dict
        products: PriceCharting search results
        item_type: "comic" or "funko"
        item_id: ID of local record (for logging)
        job_name: Job name (for logging)

    Returns:
        Best MatchResult or None
    """
    result = find_best_match(item, products, item_type)

    if result and result.matched:
        item_name = item.get("series_name") or item.get("title") or f"ID:{item_id}"
        logger.info(
            f"[{job_name}] Matched {item_type} '{item_name}' -> "
            f"PC:{result.pricecharting_id} "
            f"(score={result.score}, confidence={result.confidence}, "
            f"factors={list(result.factors.keys())})"
        )
    elif result and result.score > 0:
        item_name = item.get("series_name") or item.get("title") or f"ID:{item_id}"
        logger.debug(
            f"[{job_name}] No match for {item_type} '{item_name}' "
            f"(best_score={result.score}, threshold={MATCH_THRESHOLD})"
        )

    return result if result and result.matched else None
