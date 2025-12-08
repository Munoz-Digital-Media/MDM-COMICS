"""
The Rack Factor Service

v1.5.2: Unified content engine for newsletter and social media

Branding:
- The Rack Factor Flagrant - Weekly #1 top gainer (outlier, can't-miss)
- Rack Factor Favorites - Runners-up/top gainers (2-10)
- Rack Factor Funk - Weekly top losers/decliners ("caught the funk")
- The Rack Reliables - Stable performers (no significant change)
- The Rack Factor Recap - The actual newsletter/weekly drop
- The Rack Factor Family - Subscriber base, insiders, community
- The Rack Factor Forecast - Next week's AI picks, speculation

Schedule:
- Newsletter: Weekly (Fridays)
- Social: Daily at 4:30 PM EST (Top 5 gainers + Top 5 losers)

Aggregates data from:
- CollectionValuationService (portfolio totals)
- PriceAnalyticsService (price movers)
- NewArrivalsService (new items)
- PriceSnapshots (historical trends)
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Optional, Dict, Any
import logging
import hashlib

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.utils import utcnow
from app.core.config import settings
from app.services.collection_valuation import CollectionValuationService, CollectionSummary
from app.services.price_analytics import PriceAnalyticsService, PriceMover, PriceMoversReport
from app.services.new_arrivals import NewArrivalsService, NewArrival, NewArrivalsReport
from app.services.content_ai import ContentAIService

logger = logging.getLogger(__name__)


@dataclass
class MarketTrend:
    """Market trend observation."""
    trend_type: str  # 'hot_category', 'cooling_off', 'stable', 'emerging'
    description: str
    supporting_data: Dict[str, Any]


@dataclass
class RackReportData:
    """
    Complete data for The Rack Factor newsletter.

    Sections:
    - The Rack Factor Flagrant: #1 top gainer (outlier)
    - Rack Factor Favorites: Top gainers 2-10
    - Rack Factor Funk: Top losers ("caught the funk")
    - The Rack Reliables: Stable performers
    - The Rack Factor Forecast: AI predictions
    """
    generated_at: datetime
    week_number: int
    year: int

    # Portfolio snapshot
    portfolio: CollectionSummary

    # THE RACK FACTOR FLAGRANT - #1 top gainer (the outlier, can't-miss)
    flagrant_winner: Optional[PriceMover]

    # RACK FACTOR FAVORITES - Runners-up/top gainers (2-10)
    comic_favorites: List[PriceMover]  # Top 10 comic gainers
    funko_favorites: List[PriceMover]  # Top 10 funko gainers

    # RACK FACTOR FUNK - Top losers ("caught the funk")
    comic_funk: List[PriceMover]  # Top 10 comic losers
    funko_funk: List[PriceMover]  # Top 10 funko losers

    # THE RACK RELIABLES - Stable performers (no significant change)
    comic_reliables: List[Dict[str, Any]]
    funko_reliables: List[Dict[str, Any]]

    # New arrivals
    new_arrivals: List[NewArrival]

    # Editor's picks
    editors_choice: Optional[NewArrival]
    featured_comic: Optional[Dict[str, Any]]
    featured_funko: Optional[Dict[str, Any]]

    # THE RACK FACTOR FORECAST - AI predictions for next week
    forecast: List[Dict[str, Any]]

    # Market trends
    market_trends: List[MarketTrend]

    # Stats
    total_price_increases: int
    total_price_decreases: int
    avg_change_percent: float


@dataclass
class SocialPost:
    """Generated social media post."""
    platform: str  # bluesky, facebook, instagram
    content_type: str  # price_winner, price_loser, new_arrival, weekly_recap
    content: str
    image_url: Optional[str]
    source_entity_type: Optional[str]
    source_entity_id: Optional[int]
    idempotency_key: str
    scheduled_for: Optional[datetime] = None


class RackReportService:
    """
    The Rack Report - Unified content engine.

    Generates:
    1. Weekly newsletter content (The Rack Report)
    2. Social media posts from the same data
    3. Content queue items for approval workflow
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self.valuation_service = CollectionValuationService(db)
        self.price_service = PriceAnalyticsService(db)
        self.arrivals_service = NewArrivalsService(db)
        self.ai_service = ContentAIService()

    async def generate_weekly_report(self) -> RackReportData:
        """
        Generate complete Rack Factor newsletter data.

        Sections:
        - The Rack Factor Flagrant: #1 top gainer
        - Rack Factor Favorites: Top 10 gainers (2-10)
        - Rack Factor Funk: Top 10 losers
        - The Rack Reliables: Stable performers
        - The Rack Factor Forecast: AI predictions
        """
        now = utcnow()
        week_number = now.isocalendar()[1]
        year = now.year

        # Get portfolio valuation
        portfolio = await self.valuation_service.get_collection_summary()

        # Get price movers (TOP 10 per category for newsletter)
        comic_movers = await self.price_service.get_weekly_movers(entity_type="comic", limit=10)
        funko_movers = await self.price_service.get_weekly_movers(entity_type="funko", limit=10)

        # THE RACK FACTOR FLAGRANT - Find the #1 overall winner
        all_winners = comic_movers.winners + funko_movers.winners
        all_winners.sort(key=lambda x: x.change_percent, reverse=True)
        flagrant_winner = all_winners[0] if all_winners else None

        # RACK FACTOR FAVORITES - Top 10 gainers (excluding the flagrant)
        comic_favorites = comic_movers.winners[:10]
        funko_favorites = funko_movers.winners[:10]

        # RACK FACTOR FUNK - Top 10 losers
        comic_funk = comic_movers.losers[:10]
        funko_funk = funko_movers.losers[:10]

        # THE RACK RELIABLES - Stable performers (no significant change)
        comic_reliables = await self._get_reliable_performers("comic")
        funko_reliables = await self._get_reliable_performers("funko")

        # Get new arrivals
        arrivals_report = await self.arrivals_service.get_new_arrivals(limit=10)

        # Pick editor's choice
        editors_choice = await self.arrivals_service.pick_editors_choice(
            arrivals_report.items,
            strategy="hybrid",
        )

        # Get featured items
        featured_comic = await self._get_featured_item("comic")
        featured_funko = await self._get_featured_item("funko")

        # THE RACK FACTOR FORECAST - AI predictions for next week
        forecast = await self._generate_forecast()

        # Detect market trends
        market_trends = await self._detect_market_trends()

        # Calculate stats
        all_losers = comic_movers.losers + funko_movers.losers

        avg_change = 0.0
        if all_winners or all_losers:
            all_changes = [m.change_percent for m in all_winners + all_losers]
            avg_change = sum(all_changes) / len(all_changes) if all_changes else 0.0

        return RackReportData(
            generated_at=now,
            week_number=week_number,
            year=year,
            portfolio=portfolio,
            flagrant_winner=flagrant_winner,
            comic_favorites=comic_favorites,
            funko_favorites=funko_favorites,
            comic_funk=comic_funk,
            funko_funk=funko_funk,
            comic_reliables=comic_reliables,
            funko_reliables=funko_reliables,
            new_arrivals=arrivals_report.items,
            editors_choice=editors_choice,
            featured_comic=featured_comic,
            featured_funko=featured_funko,
            forecast=forecast,
            market_trends=market_trends,
            total_price_increases=len(all_winners),
            total_price_decreases=len(all_losers),
            avg_change_percent=avg_change,
        )

    async def _get_reliable_performers(self, entity_type: str) -> List[Dict[str, Any]]:
        """Get stable performers with no significant price change (The Rack Reliables)."""
        try:
            if entity_type == "comic":
                result = await self.db.execute(text("""
                    SELECT
                        ci.id,
                        ci.issue_name as name,
                        ci.image as image_url,
                        COALESCE(ci.price_new, ci.price_cib, ci.price_loose) as value,
                        cs.name as series_name
                    FROM comic_issues ci
                    LEFT JOIN comic_series cs ON ci.series_id = cs.id
                    WHERE ci.pricecharting_id IS NOT NULL
                      AND COALESCE(ci.price_new, ci.price_cib, ci.price_loose) > 50
                      AND ci.id NOT IN (
                          SELECT DISTINCT entity_id FROM price_changelog
                          WHERE entity_type = 'comic'
                            AND changed_at >= NOW() - INTERVAL '7 days'
                            AND ABS((new_value - old_value) / NULLIF(old_value, 0) * 100) > 5
                      )
                    ORDER BY COALESCE(ci.price_new, ci.price_cib, ci.price_loose) DESC
                    LIMIT 10
                """))
            else:  # funko
                result = await self.db.execute(text("""
                    SELECT
                        id,
                        title as name,
                        image_url,
                        COALESCE(price_new, price_cib, price_loose) as value,
                        category as series_name
                    FROM funkos
                    WHERE pricecharting_id IS NOT NULL
                      AND COALESCE(price_new, price_cib, price_loose) > 25
                      AND id NOT IN (
                          SELECT DISTINCT entity_id FROM price_changelog
                          WHERE entity_type = 'funko'
                            AND changed_at >= NOW() - INTERVAL '7 days'
                            AND ABS((new_value - old_value) / NULLIF(old_value, 0) * 100) > 5
                      )
                    ORDER BY COALESCE(price_new, price_cib, price_loose) DESC
                    LIMIT 10
                """))

            reliables = []
            for row in result.fetchall():
                reliables.append({
                    "id": row[0],
                    "name": row[1],
                    "image_url": row[2],
                    "value": float(row[3]) if row[3] else 0,
                    "series": row[4],
                    "entity_type": entity_type,
                })
            return reliables
        except Exception as e:
            logger.error(f"Failed to get reliable performers for {entity_type}: {e}")
            return []

    async def _generate_forecast(self) -> List[Dict[str, Any]]:
        """
        Generate AI-powered forecast for next week (The Rack Factor Forecast).

        Uses volatility and trend data from price_snapshots to predict movers.
        """
        try:
            result = await self.db.execute(text("""
                WITH recent_volatility AS (
                    SELECT
                        entity_type,
                        entity_id,
                        AVG(volatility_7d) as avg_volatility,
                        AVG(trend_7d) as avg_trend,
                        AVG(momentum) as avg_momentum
                    FROM price_snapshots
                    WHERE snapshot_date >= CURRENT_DATE - INTERVAL '14 days'
                      AND volatility_7d IS NOT NULL
                    GROUP BY entity_type, entity_id
                    HAVING AVG(volatility_7d) > 0.05
                       AND AVG(trend_7d) > 0
                )
                SELECT
                    rv.entity_type,
                    rv.entity_id,
                    rv.avg_volatility,
                    rv.avg_trend,
                    rv.avg_momentum,
                    CASE
                        WHEN rv.entity_type = 'funko' THEN f.title
                        ELSE ci.issue_name
                    END as name,
                    CASE
                        WHEN rv.entity_type = 'funko' THEN f.image_url
                        ELSE ci.image
                    END as image_url,
                    CASE
                        WHEN rv.entity_type = 'funko' THEN COALESCE(f.price_new, f.price_cib, f.price_loose)
                        ELSE COALESCE(ci.price_new, ci.price_cib, ci.price_loose)
                    END as current_price
                FROM recent_volatility rv
                LEFT JOIN funkos f ON rv.entity_type = 'funko' AND rv.entity_id = f.id
                LEFT JOIN comic_issues ci ON rv.entity_type = 'comic' AND rv.entity_id = ci.id
                ORDER BY (rv.avg_volatility * rv.avg_trend * COALESCE(rv.avg_momentum, 1)) DESC
                LIMIT 5
            """))

            forecasts = []
            for row in result.fetchall():
                prediction = "bullish" if row[3] > 0 else "bearish"
                confidence = min(abs(row[3]) * 100, 95)  # Cap at 95%

                forecasts.append({
                    "entity_type": row[0],
                    "entity_id": row[1],
                    "name": row[5],
                    "image_url": row[6],
                    "current_price": float(row[7]) if row[7] else 0,
                    "volatility": float(row[2]) if row[2] else 0,
                    "trend": float(row[3]) if row[3] else 0,
                    "prediction": prediction,
                    "confidence": round(confidence, 1),
                    "reason": f"High volatility ({row[2]:.2f}) with {'upward' if row[3] > 0 else 'downward'} trend",
                })

            return forecasts
        except Exception as e:
            logger.warning(f"Failed to generate forecast: {e}")
            return []

    async def generate_daily_social_posts(
        self,
        platforms: List[str] = None,
    ) -> List[SocialPost]:
        """
        Generate daily social media posts (4:30 PM EST schedule).

        Creates posts for:
        - Top 5 daily price gainers
        - Top 5 daily price losers ("caught the funk")
        """
        if platforms is None:
            platforms = ["bluesky", "facebook"]

        posts = []
        today = utcnow().strftime("%Y-%m-%d")

        # Get daily movers (top 5 for social)
        comic_movers = await self.price_service.get_daily_movers(entity_type="comic", limit=5)
        funko_movers = await self.price_service.get_daily_movers(entity_type="funko", limit=5)

        # Combine and sort
        all_winners = comic_movers.winners + funko_movers.winners
        all_winners.sort(key=lambda x: x.change_percent, reverse=True)
        top_5_winners = all_winners[:5]

        all_losers = comic_movers.losers + funko_movers.losers
        all_losers.sort(key=lambda x: x.change_percent)
        top_5_losers = all_losers[:5]

        for platform in platforms:
            # Post for each top gainer
            for i, winner in enumerate(top_5_winners):
                posts.append(await self._create_price_winner_post(
                    winner, platform, f"daily-{today}", rank=i + 1
                ))

            # Post for each top loser (buy opportunity / caught the funk)
            for i, loser in enumerate(top_5_losers):
                posts.append(await self._create_price_loser_post(
                    loser, platform, f"daily-{today}", rank=i + 1
                ))

        return posts

    async def generate_weekly_social_posts(
        self,
        report: RackReportData,
        platforms: List[str] = None,
    ) -> List[SocialPost]:
        """
        Generate weekly social media posts from Rack Report data.

        Creates posts for:
        - The Rack Factor Flagrant (#1 top gainer)
        - Top price winners (Rack Factor Favorites)
        - Top price losers (Rack Factor Funk)
        - New arrivals highlights
        - Weekly recap
        """
        if platforms is None:
            platforms = ["bluesky", "facebook"]

        posts = []
        week_key = f"{report.year}-W{report.week_number:02d}"

        for platform in platforms:
            # THE RACK FACTOR FLAGRANT - #1 top gainer
            if report.flagrant_winner:
                posts.append(await self._create_flagrant_post(
                    report.flagrant_winner, platform, week_key
                ))

            # RACK FACTOR FAVORITES - Top comic gainers
            for i, winner in enumerate(report.comic_favorites[:5]):
                posts.append(await self._create_price_winner_post(
                    winner, platform, week_key, rank=i + 1
                ))

            # RACK FACTOR FAVORITES - Top funko gainers
            for i, winner in enumerate(report.funko_favorites[:5]):
                posts.append(await self._create_price_winner_post(
                    winner, platform, week_key, rank=i + 1
                ))

            # RACK FACTOR FUNK - Top losers (buy opportunity)
            all_funk = report.comic_funk + report.funko_funk
            all_funk.sort(key=lambda x: x.change_percent)
            for i, loser in enumerate(all_funk[:5]):
                posts.append(await self._create_price_loser_post(
                    loser, platform, week_key, rank=i + 1
                ))

            # New arrival highlight
            if report.new_arrivals:
                arrival = report.new_arrivals[0]
                posts.append(await self._create_new_arrival_post(
                    arrival, platform, week_key
                ))

            # Weekly recap
            posts.append(await self._create_weekly_recap_post(
                report, platform, week_key
            ))

        return posts

    async def _create_flagrant_post(
        self,
        mover: PriceMover,
        platform: str,
        week_key: str,
    ) -> SocialPost:
        """Create THE RACK FACTOR FLAGRANT post - the #1 top gainer."""
        base_content = (
            f"🏆 THE RACK FACTOR FLAGRANT 🏆\n\n"
            f"{mover.name}\n\n"
            f"This week's BIGGEST MOVER! 🚀\n"
            f"📈 Up {mover.change_percent:.1f}%\n"
            f"💰 ${mover.price_old:.2f} → ${mover.price_new:.2f}\n\n"
            f"The outlier. The can't-miss. THE FLAGRANT.\n\n"
            f"#RackFactorFlagrant #{'comics' if mover.entity_type == 'comic' else 'funkopop'} "
            f"#collectibles #mdmcomics"
        )

        result = await self.ai_service.enhance_blurb(
            base_content=base_content,
            tone="exciting",
            max_length=280 if platform == "bluesky" else 500,
        )

        return SocialPost(
            platform=platform,
            content_type="flagrant",
            content=result.content,
            image_url=mover.image_url,
            source_entity_type=mover.entity_type,
            source_entity_id=mover.entity_id,
            idempotency_key=self._generate_idempotency_key(
                f"flagrant-{mover.entity_type}-{mover.entity_id}-{week_key}-{platform}"
            ),
        )

    async def _create_price_winner_post(
        self,
        mover: PriceMover,
        platform: str,
        week_key: str,
        rank: int = None,
    ) -> SocialPost:
        """Create price winner social post (Rack Factor Favorites)."""
        direction_emoji = "🔥" if mover.change_percent > 20 else "📈"
        rank_str = f"#{rank} " if rank else ""

        base_content = (
            f"{direction_emoji} {rank_str}{mover.name} is making moves!\n\n"
            f"Up {mover.change_percent:.1f}%\n"
            f"${mover.price_old:.2f} → ${mover.price_new:.2f}\n\n"
            f"#RackFactorFavorites #{'comics' if mover.entity_type == 'comic' else 'funkopop'} "
            f"#collectibles #mdmcomics"
        )

        result = await self.ai_service.enhance_blurb(
            base_content=base_content,
            tone="exciting",
            max_length=280 if platform == "bluesky" else 500,
        )

        return SocialPost(
            platform=platform,
            content_type="price_winner",
            content=result.content,
            image_url=mover.image_url,
            source_entity_type=mover.entity_type,
            source_entity_id=mover.entity_id,
            idempotency_key=self._generate_idempotency_key(
                f"winner-{mover.entity_type}-{mover.entity_id}-{week_key}-{platform}-{rank or 0}"
            ),
        )

    async def _create_price_loser_post(
        self,
        mover: PriceMover,
        platform: str,
        week_key: str,
        rank: int = None,
    ) -> SocialPost:
        """Create price loser post (Rack Factor Funk - caught the funk)."""
        rank_str = f"#{rank} " if rank else ""

        base_content = (
            f"📉 RACK FACTOR FUNK 📉\n\n"
            f"{rank_str}{mover.name} caught the funk!\n\n"
            f"Down {abs(mover.change_percent):.1f}%\n"
            f"${mover.price_old:.2f} → ${mover.price_new:.2f}\n\n"
            f"Buying opportunity? 🤔\n\n"
            f"#RackFactorFunk #{'comics' if mover.entity_type == 'comic' else 'funkopop'} "
            f"#collectibles #mdmcomics"
        )

        result = await self.ai_service.enhance_blurb(
            base_content=base_content,
            tone="informative",
            max_length=280 if platform == "bluesky" else 500,
        )

        return SocialPost(
            platform=platform,
            content_type="price_loser",
            content=result.content,
            image_url=mover.image_url,
            source_entity_type=mover.entity_type,
            source_entity_id=mover.entity_id,
            idempotency_key=self._generate_idempotency_key(
                f"funk-{mover.entity_type}-{mover.entity_id}-{week_key}-{platform}-{rank or 0}"
            ),
        )

    async def _create_new_arrival_post(
        self,
        arrival: NewArrival,
        platform: str,
        week_key: str,
    ) -> SocialPost:
        """Create new arrival post."""
        price_str = f"\n💰 ${arrival.price:.2f}" if arrival.price else ""

        base_content = (
            f"🆕 Just Listed!\n\n"
            f"{arrival.name}{price_str}\n\n"
            f"Shop now at mdmcomics.com\n\n"
            f"#newarrival #{'comics' if arrival.entity_type == 'comic' else 'funkopop'} "
            f"#mdmcomics"
        )

        result = await self.ai_service.enhance_blurb(
            base_content=base_content,
            tone="engaging",
            max_length=280 if platform == "bluesky" else 500,
        )

        return SocialPost(
            platform=platform,
            content_type="new_arrival",
            content=result.content,
            image_url=arrival.image_url,
            source_entity_type=arrival.entity_type,
            source_entity_id=arrival.entity_id,
            idempotency_key=self._generate_idempotency_key(
                f"arrival-{arrival.entity_type}-{arrival.entity_id}-{week_key}-{platform}"
            ),
        )

    async def _create_weekly_recap_post(
        self,
        report: RackReportData,
        platform: str,
        week_key: str,
    ) -> SocialPost:
        """Create THE RACK FACTOR RECAP post - weekly summary."""
        change_str = ""
        if report.portfolio.weekly_change:
            change = report.portfolio.weekly_change
            direction = "📈" if change.change_percent > 0 else "📉"
            change_str = f"\nWeekly Change: {direction} {change.change_percent:+.1f}%"

        # Use flagrant winner as the hottest item
        hottest_str = ""
        if report.flagrant_winner:
            hottest_str = f"\n🏆 Flagrant: {report.flagrant_winner.name} (+{report.flagrant_winner.change_percent:.1f}%)"

        base_content = (
            f"📊 THE RACK FACTOR RECAP 📊\n"
            f"Week {report.week_number}, {report.year}\n\n"
            f"💰 Collection Value: ${report.portfolio.total_value:,.2f}"
            f"{change_str}\n"
            f"📈 Favorites: {report.total_price_increases} items up\n"
            f"📉 Funk: {report.total_price_decreases} items down"
            f"{hottest_str}\n\n"
            f"Full report in The Rack Factor newsletter! 📧\n"
            f"mdmcomics.com/newsletter\n\n"
            f"#RackFactorRecap #collectibles #mdmcomics"
        )

        result = await self.ai_service.enhance_blurb(
            base_content=base_content,
            tone="professional",
            max_length=280 if platform == "bluesky" else 500,
        )

        return SocialPost(
            platform=platform,
            content_type="weekly_recap",
            content=result.content,
            image_url=None,  # Could add a branded recap image
            source_entity_type=None,
            source_entity_id=None,
            idempotency_key=self._generate_idempotency_key(
                f"recap-{week_key}-{platform}"
            ),
        )

    async def _get_featured_item(self, entity_type: str) -> Optional[Dict[str, Any]]:
        """Get featured item with recent price movement."""
        try:
            if entity_type == "comic":
                result = await self.db.execute(text("""
                    SELECT
                        ci.id,
                        ci.issue_name as name,
                        ci.image as image_url,
                        COALESCE(ci.price_new, ci.price_cib, ci.price_loose) as value,
                        cs.name as series_name
                    FROM comic_issues ci
                    LEFT JOIN comic_series cs ON ci.series_id = cs.id
                    WHERE ci.pricecharting_id IS NOT NULL
                      AND COALESCE(ci.price_new, ci.price_cib, ci.price_loose) > 100
                    ORDER BY COALESCE(ci.price_new, ci.price_cib, ci.price_loose) DESC
                    LIMIT 1
                """))
            else:  # funko
                result = await self.db.execute(text("""
                    SELECT
                        id,
                        title as name,
                        image_url,
                        COALESCE(price_new, price_cib, price_loose) as value,
                        category as series_name
                    FROM funkos
                    WHERE pricecharting_id IS NOT NULL
                      AND COALESCE(price_new, price_cib, price_loose) > 50
                    ORDER BY COALESCE(price_new, price_cib, price_loose) DESC
                    LIMIT 1
                """))

            row = result.fetchone()
            if row:
                return {
                    "id": row[0],
                    "name": row[1],
                    "image_url": row[2],
                    "value": float(row[3]) if row[3] else 0,
                    "series": row[4],
                    "entity_type": entity_type,
                }
            return None
        except Exception as e:
            logger.error(f"Failed to get featured {entity_type}: {e}")
            return None

    async def _detect_market_trends(self) -> List[MarketTrend]:
        """Detect market trends from price data."""
        trends = []

        try:
            # Hot category detection
            result = await self.db.execute(text("""
                SELECT
                    entity_type,
                    COUNT(*) as change_count,
                    AVG(
                        CASE
                            WHEN old_value > 0
                            THEN ((new_value - old_value) / old_value) * 100
                            ELSE 0
                        END
                    ) as avg_change
                FROM price_changelog
                WHERE changed_at >= NOW() - INTERVAL '7 days'
                GROUP BY entity_type
                ORDER BY avg_change DESC
            """))

            rows = result.fetchall()
            for row in rows:
                if row[2] and row[2] > 5:  # >5% average increase
                    trends.append(MarketTrend(
                        trend_type="hot_category",
                        description=f"{row[0].title()}s are trending up with {row[2]:.1f}% avg increase",
                        supporting_data={
                            "category": row[0],
                            "change_count": row[1],
                            "avg_change_percent": float(row[2]),
                        }
                    ))
                elif row[2] and row[2] < -5:  # >5% average decrease
                    trends.append(MarketTrend(
                        trend_type="cooling_off",
                        description=f"{row[0].title()}s cooling off with {abs(row[2]):.1f}% avg decrease",
                        supporting_data={
                            "category": row[0],
                            "change_count": row[1],
                            "avg_change_percent": float(row[2]),
                        }
                    ))

        except Exception as e:
            logger.warning(f"Failed to detect trends: {e}")

        return trends[:3]  # Max 3 trends

    def _generate_idempotency_key(self, seed: str) -> str:
        """Generate consistent idempotency key from seed."""
        return hashlib.md5(seed.encode()).hexdigest()[:32]

    async def queue_social_posts(
        self,
        posts: List[SocialPost],
        schedule_offset_minutes: int = 60,
    ) -> int:
        """
        Add social posts to the content queue for approval.

        Returns number of posts queued.
        """
        from app.models.content_queue import ContentQueueItem, ContentStatus

        queued = 0
        base_time = utcnow()

        for i, post in enumerate(posts):
            try:
                # Check idempotency
                existing = await self.db.execute(text("""
                    SELECT id FROM content_queue
                    WHERE idempotency_key = :key
                """), {"key": post.idempotency_key})

                if existing.fetchone():
                    logger.debug(f"Post already queued: {post.idempotency_key}")
                    continue

                # Schedule posts with offset
                scheduled_for = base_time + timedelta(minutes=schedule_offset_minutes * i)

                item = ContentQueueItem(
                    content_type=post.content_type,
                    platform=post.platform,
                    original_content=post.content,
                    image_url=post.image_url,
                    source_type=post.source_entity_type,
                    source_id=post.source_entity_id,
                    status=ContentStatus.PENDING_REVIEW,
                    scheduled_for=scheduled_for,
                    idempotency_key=post.idempotency_key,
                )
                self.db.add(item)
                queued += 1

            except Exception as e:
                logger.error(f"Failed to queue post: {e}")

        if queued > 0:
            await self.db.commit()
            logger.info(f"Queued {queued} social posts for approval")

        return queued
