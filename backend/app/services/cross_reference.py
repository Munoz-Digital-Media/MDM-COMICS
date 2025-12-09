"""
Cross-Reference Matcher Service v1.0.0

Links records across data sources (GCD, Metron, PriceCharting) using:
1. Direct ID matches (ISBN, UPC/barcode)
2. Fuzzy title + issue + year matching
3. Series name normalization

Part of GCD-Primary Architecture:
- GCD provides authoritative catalog data
- This service links GCD records to Metron (rich metadata) and PriceCharting (pricing)
"""
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from rapidfuzz import fuzz
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Result of a cross-reference match attempt."""
    matched: bool
    confidence: float  # 0.0 - 1.0
    match_type: str  # 'isbn', 'upc', 'fuzzy_title', 'series_issue'
    source_id: Optional[str] = None
    target_id: Optional[int] = None
    details: Optional[Dict[str, Any]] = None


class CrossReferenceMatcher:
    """
    Service for matching records across data sources.

    Match strategies (in priority order):
    1. ISBN match - exact match on ISBN (most reliable)
    2. UPC/Barcode match - exact match on UPC code
    3. Series + Issue + Year - fuzzy title match with issue number
    4. Title similarity - fuzzy match on full title string
    """

    # Minimum confidence thresholds
    CONFIDENCE_ISBN = 1.0
    CONFIDENCE_UPC = 0.95
    CONFIDENCE_SERIES_ISSUE = 0.85
    CONFIDENCE_FUZZY_TITLE = 0.75

    # Fuzzy match thresholds
    TITLE_SIMILARITY_THRESHOLD = 85
    SERIES_SIMILARITY_THRESHOLD = 80

    def __init__(self):
        self._series_cache: Dict[str, str] = {}

    def normalize_series_name(self, name: str) -> str:
        """
        Normalize series name for matching.

        Handles:
        - Remove "The" prefix
        - Remove volume indicators
        - Normalize spacing and punctuation
        - Handle common variations
        """
        if not name:
            return ""

        normalized = name.lower().strip()

        # Remove leading "The"
        normalized = re.sub(r'^the\s+', '', normalized)

        # Remove volume indicators
        normalized = re.sub(r'\s*\(vol\.?\s*\d+\)$', '', normalized)
        normalized = re.sub(r'\s*vol\.?\s*\d+$', '', normalized)
        normalized = re.sub(r'\s*v\d+$', '', normalized)

        # Remove year in parentheses
        normalized = re.sub(r'\s*\(\d{4}\)$', '', normalized)
        normalized = re.sub(r'\s*\(\d{4}-\d{4}\)$', '', normalized)
        normalized = re.sub(r'\s*\(\d{4}-\)$', '', normalized)

        # Normalize punctuation
        normalized = re.sub(r'[:\-–—]', ' ', normalized)
        normalized = re.sub(r'[\'"`]', '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized)

        return normalized.strip()

    def normalize_issue_number(self, issue: str) -> Optional[str]:
        """
        Normalize issue number for matching.

        Handles:
        - Numeric issues (1, 01, 001 -> "1")
        - Letter suffixes (1A, 1B -> "1A")
        - Special issues (Annual 1, Special 1)
        """
        if not issue:
            return None

        issue = str(issue).strip().upper()

        # Handle numeric with leading zeros
        numeric_match = re.match(r'^0*(\d+)([A-Z]?)$', issue)
        if numeric_match:
            num = numeric_match.group(1)
            suffix = numeric_match.group(2)
            return f"{num}{suffix}" if suffix else num

        return issue

    def extract_year(self, date_str: str) -> Optional[int]:
        """Extract year from various date formats."""
        if not date_str:
            return None

        year_match = re.search(r'(\d{4})', str(date_str))
        if year_match:
            return int(year_match.group(1))
        return None

    def calculate_title_similarity(
        self,
        title1: str,
        title2: str,
        series1: Optional[str] = None,
        series2: Optional[str] = None,
    ) -> float:
        """
        Calculate similarity score between two titles.

        If series names are provided, weights series match higher.
        """
        if not title1 or not title2:
            return 0.0

        # Normalize titles
        t1 = title1.lower().strip()
        t2 = title2.lower().strip()

        # Direct fuzzy match
        title_score = fuzz.ratio(t1, t2) / 100.0

        # If series provided, factor in series match
        if series1 and series2:
            s1 = self.normalize_series_name(series1)
            s2 = self.normalize_series_name(series2)
            series_score = fuzz.ratio(s1, s2) / 100.0

            # Weight: 60% series, 40% title
            return 0.6 * series_score + 0.4 * title_score

        return title_score

    async def match_by_isbn(
        self,
        db: AsyncSession,
        isbn: str,
        source: str = "gcd",
    ) -> Optional[MatchResult]:
        """
        Match by ISBN across sources.

        Args:
            db: Database session
            isbn: ISBN to match
            source: Source of the ISBN ('gcd', 'metron', 'pricecharting')

        Returns:
            MatchResult if found, None otherwise
        """
        if not isbn:
            return None

        # Clean ISBN (remove hyphens, spaces)
        clean_isbn = re.sub(r'[\s\-]', '', isbn)

        # Search based on source
        if source == "gcd":
            # GCD record - find matching Metron/PriceCharting
            result = await db.execute(text("""
                SELECT id, isbn, metron_id, pricecharting_id
                FROM comic_issues
                WHERE isbn = :isbn AND gcd_id IS NULL
                LIMIT 1
            """), {"isbn": clean_isbn})
        else:
            # Non-GCD record - find matching GCD
            result = await db.execute(text("""
                SELECT id, gcd_id, isbn
                FROM comic_issues
                WHERE isbn = :isbn AND gcd_id IS NOT NULL
                LIMIT 1
            """), {"isbn": clean_isbn})

        row = result.fetchone()
        if row:
            return MatchResult(
                matched=True,
                confidence=self.CONFIDENCE_ISBN,
                match_type="isbn",
                source_id=isbn,
                target_id=row.id,
                details={"isbn": clean_isbn},
            )

        return None

    async def match_by_upc(
        self,
        db: AsyncSession,
        upc: str,
        source: str = "gcd",
    ) -> Optional[MatchResult]:
        """Match by UPC/barcode across sources."""
        if not upc:
            return None

        # Clean UPC (remove spaces)
        clean_upc = upc.strip()

        if source == "gcd":
            result = await db.execute(text("""
                SELECT id, upc, metron_id, pricecharting_id
                FROM comic_issues
                WHERE upc = :upc AND gcd_id IS NULL
                LIMIT 1
            """), {"upc": clean_upc})
        else:
            result = await db.execute(text("""
                SELECT id, gcd_id, upc
                FROM comic_issues
                WHERE upc = :upc AND gcd_id IS NOT NULL
                LIMIT 1
            """), {"upc": clean_upc})

        row = result.fetchone()
        if row:
            return MatchResult(
                matched=True,
                confidence=self.CONFIDENCE_UPC,
                match_type="upc",
                source_id=upc,
                target_id=row.id,
                details={"upc": clean_upc},
            )

        return None

    async def match_by_series_issue(
        self,
        db: AsyncSession,
        series_name: str,
        issue_number: str,
        year: Optional[int] = None,
        source: str = "gcd",
    ) -> Optional[MatchResult]:
        """
        Match by series name + issue number (+ optional year).

        Uses fuzzy matching for series name to handle variations.
        """
        if not series_name or not issue_number:
            return None

        normalized_series = self.normalize_series_name(series_name)
        normalized_issue = self.normalize_issue_number(issue_number)

        # Build query based on source
        base_query = """
            SELECT id, title, issue_number, series_id, gcd_id, metron_id,
                   cover_date, release_date
            FROM comic_issues
            WHERE issue_number = :issue
        """

        if source == "gcd":
            base_query += " AND gcd_id IS NULL"
        else:
            base_query += " AND gcd_id IS NOT NULL"

        # Add year filter if provided
        params = {"issue": normalized_issue}
        if year:
            base_query += """
                AND (
                    EXTRACT(YEAR FROM cover_date)::INTEGER = :year
                    OR EXTRACT(YEAR FROM release_date)::INTEGER = :year
                )
            """
            params["year"] = year

        base_query += " LIMIT 50"

        result = await db.execute(text(base_query), params)
        candidates = result.fetchall()

        if not candidates:
            return None

        # Find best match by series name similarity
        best_match = None
        best_score = 0.0

        for candidate in candidates:
            # Get series name from title (simplified - should use series table)
            candidate_title = candidate.title or ""

            score = fuzz.ratio(normalized_series, self.normalize_series_name(candidate_title))

            if score >= self.SERIES_SIMILARITY_THRESHOLD and score > best_score:
                best_score = score
                best_match = candidate

        if best_match:
            confidence = min(self.CONFIDENCE_SERIES_ISSUE, best_score / 100.0)
            return MatchResult(
                matched=True,
                confidence=confidence,
                match_type="series_issue",
                source_id=f"{series_name}#{issue_number}",
                target_id=best_match.id,
                details={
                    "series": series_name,
                    "issue": issue_number,
                    "year": year,
                    "similarity": best_score,
                },
            )

        return None

    async def find_matches_for_gcd_record(
        self,
        db: AsyncSession,
        gcd_record: Dict[str, Any],
    ) -> List[MatchResult]:
        """
        Find all potential matches for a GCD record.

        Tries all match strategies in order of reliability.
        """
        matches = []

        # Try ISBN match first (most reliable)
        if gcd_record.get("isbn"):
            isbn_match = await self.match_by_isbn(db, gcd_record["isbn"], source="gcd")
            if isbn_match:
                matches.append(isbn_match)

        # Try UPC match
        if gcd_record.get("upc"):
            upc_match = await self.match_by_upc(db, gcd_record["upc"], source="gcd")
            if upc_match:
                matches.append(upc_match)

        # Try series + issue match
        series = gcd_record.get("series_name")
        issue = gcd_record.get("issue_number")
        year = self.extract_year(gcd_record.get("release_date") or gcd_record.get("cover_date"))

        if series and issue:
            series_match = await self.match_by_series_issue(
                db, series, issue, year, source="gcd"
            )
            if series_match:
                matches.append(series_match)

        # Sort by confidence (highest first)
        matches.sort(key=lambda m: m.confidence, reverse=True)

        return matches

    async def link_records(
        self,
        db: AsyncSession,
        source_id: int,
        target_id: int,
        match_type: str,
        confidence: float,
    ) -> bool:
        """
        Link two records by copying cross-reference IDs.

        This merges the records by ensuring both have all available IDs.
        """
        try:
            # Get both records
            source_result = await db.execute(text("""
                SELECT id, gcd_id, metron_id, pricecharting_id
                FROM comic_issues WHERE id = :id
            """), {"id": source_id})
            source = source_result.fetchone()

            target_result = await db.execute(text("""
                SELECT id, gcd_id, metron_id, pricecharting_id
                FROM comic_issues WHERE id = :id
            """), {"id": target_id})
            target = target_result.fetchone()

            if not source or not target:
                return False

            # Merge IDs - copy from source to target where target is NULL
            updates = []
            params = {"target_id": target_id}

            if source.gcd_id and not target.gcd_id:
                updates.append("gcd_id = :gcd_id")
                params["gcd_id"] = source.gcd_id

            if source.metron_id and not target.metron_id:
                updates.append("metron_id = :metron_id")
                params["metron_id"] = source.metron_id

            if source.pricecharting_id and not target.pricecharting_id:
                updates.append("pricecharting_id = :pricecharting_id")
                params["pricecharting_id"] = source.pricecharting_id

            if updates:
                await db.execute(text(f"""
                    UPDATE comic_issues
                    SET {', '.join(updates)}, updated_at = NOW()
                    WHERE id = :target_id
                """), params)

                # Log the cross-reference link
                await db.execute(text("""
                    INSERT INTO field_changelog
                    (entity_type, entity_id, field_name, old_value, new_value, reason, batch_id)
                    VALUES ('comic_issue', :target_id, 'cross_reference', :source_id::text, :match_type, 'cross_reference_link', :batch)
                """), {
                    "target_id": target_id,
                    "source_id": source_id,
                    "match_type": match_type,
                    "batch": f"xref_{match_type}_{confidence:.2f}",
                })

                await db.commit()
                logger.info(f"Linked records: {source_id} -> {target_id} ({match_type}, {confidence:.2f})")
                return True

            return False

        except Exception as e:
            logger.error(f"Error linking records {source_id} -> {target_id}: {e}")
            return False


# Global instance
cross_reference_matcher = CrossReferenceMatcher()
