"""
Comic Cache Service
Wraps Metron API and caches ALL data to local database.
Every search = free data for our infrastructure.
"""
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.models.comic_data import (
    ComicPublisher,
    ComicSeries,
    ComicIssue,
    ComicCharacter,
    ComicCreator,
    ComicArc,
    MetronAPILog,
)
from app.services.metron import metron_service


class ComicCacheService:
    """
    Caches all Metron data locally.

    Strategy:
    1. Check local DB first
    2. If not found or stale, fetch from Metron
    3. Save EVERYTHING to local DB
    4. Return data
    """

    async def log_api_call(
        self,
        db: AsyncSession,
        endpoint: str,
        params: Dict,
        response_code: int,
        response_size: int,
        duration_ms: int,
        user_id: Optional[int] = None,
        ip_address: Optional[str] = None
    ):
        """Log every Metron API call for analytics and debugging."""
        log = MetronAPILog(
            endpoint=endpoint,
            params=params,
            response_code=response_code,
            response_size=response_size,
            duration_ms=duration_ms,
            user_id=user_id,
            ip_address=ip_address
        )
        db.add(log)
        await db.commit()

    async def cache_publisher(self, db: AsyncSession, publisher_data: Dict) -> ComicPublisher:
        """Cache a publisher from Metron response."""
        if not publisher_data or not publisher_data.get('id'):
            return None

        # Upsert - insert or update on conflict
        stmt = insert(ComicPublisher).values(
            metron_id=publisher_data['id'],
            name=publisher_data.get('name', 'Unknown'),
            founded=publisher_data.get('founded'),
            image=publisher_data.get('image'),
            raw_data=publisher_data,
            updated_at=datetime.utcnow()
        ).on_conflict_do_update(
            index_elements=['metron_id'],
            set_={
                'name': publisher_data.get('name', 'Unknown'),
                'founded': publisher_data.get('founded'),
                'image': publisher_data.get('image'),
                'raw_data': publisher_data,
                'updated_at': datetime.utcnow()
            }
        ).returning(ComicPublisher)

        result = await db.execute(stmt)
        await db.commit()
        return result.scalar_one_or_none()

    async def cache_series(self, db: AsyncSession, series_data: Dict) -> ComicSeries:
        """Cache a series from Metron response."""
        if not series_data or not series_data.get('id'):
            return None

        # Cache publisher first if present
        publisher_id = None
        if series_data.get('publisher'):
            publisher = await self.cache_publisher(db, series_data['publisher'])
            if publisher:
                publisher_id = publisher.id

        stmt = insert(ComicSeries).values(
            metron_id=series_data['id'],
            name=series_data.get('name', 'Unknown'),
            sort_name=series_data.get('sort_name'),
            volume=series_data.get('volume'),
            year_began=series_data.get('year_began'),
            year_ended=series_data.get('year_ended'),
            issue_count=series_data.get('issue_count'),
            image=series_data.get('image'),
            publisher_id=publisher_id,
            description=series_data.get('desc'),
            series_type=series_data.get('series_type', {}).get('name') if isinstance(series_data.get('series_type'), dict) else series_data.get('series_type'),
            raw_data=series_data,
            updated_at=datetime.utcnow()
        ).on_conflict_do_update(
            index_elements=['metron_id'],
            set_={
                'name': series_data.get('name', 'Unknown'),
                'sort_name': series_data.get('sort_name'),
                'volume': series_data.get('volume'),
                'year_began': series_data.get('year_began'),
                'year_ended': series_data.get('year_ended'),
                'issue_count': series_data.get('issue_count'),
                'image': series_data.get('image'),
                'publisher_id': publisher_id,
                'description': series_data.get('desc'),
                'raw_data': series_data,
                'updated_at': datetime.utcnow()
            }
        ).returning(ComicSeries)

        result = await db.execute(stmt)
        await db.commit()
        return result.scalar_one_or_none()

    async def cache_issue(self, db: AsyncSession, issue_data: Dict) -> ComicIssue:
        """Cache an issue from Metron response - THE MAIN EVENT."""
        if not issue_data or not issue_data.get('id'):
            return None

        # Cache series first if present
        series_id = None
        if issue_data.get('series'):
            series = await self.cache_series(db, issue_data['series'])
            if series:
                series_id = series.id

        # Parse cover date
        cover_date = None
        if issue_data.get('cover_date'):
            try:
                cover_date = datetime.strptime(issue_data['cover_date'], '%Y-%m-%d').date()
            except:
                pass

        store_date = None
        if issue_data.get('store_date'):
            try:
                store_date = datetime.strptime(issue_data['store_date'], '%Y-%m-%d').date()
            except:
                pass

        stmt = insert(ComicIssue).values(
            metron_id=issue_data['id'],
            series_id=series_id,
            number=issue_data.get('number'),
            issue_name=issue_data.get('name') or issue_data.get('issue_name'),
            cover_date=cover_date,
            store_date=store_date,
            image=issue_data.get('image'),
            price=issue_data.get('price'),
            page_count=issue_data.get('page_count'),
            upc=issue_data.get('upc'),
            sku=issue_data.get('sku'),
            isbn=issue_data.get('isbn'),
            description=issue_data.get('desc'),
            is_variant=issue_data.get('variant', False),
            variant_name=issue_data.get('variant_name'),
            rating=issue_data.get('rating', {}).get('name') if isinstance(issue_data.get('rating'), dict) else issue_data.get('rating'),
            raw_data=issue_data,
            updated_at=datetime.utcnow(),
            last_fetched=datetime.utcnow()
        ).on_conflict_do_update(
            index_elements=['metron_id'],
            set_={
                'series_id': series_id,
                'number': issue_data.get('number'),
                'issue_name': issue_data.get('name') or issue_data.get('issue_name'),
                'cover_date': cover_date,
                'store_date': store_date,
                'image': issue_data.get('image'),
                'price': issue_data.get('price'),
                'page_count': issue_data.get('page_count'),
                'upc': issue_data.get('upc'),
                'sku': issue_data.get('sku'),
                'isbn': issue_data.get('isbn'),
                'description': issue_data.get('desc'),
                'is_variant': issue_data.get('variant', False),
                'variant_name': issue_data.get('variant_name'),
                'raw_data': issue_data,
                'updated_at': datetime.utcnow(),
                'last_fetched': datetime.utcnow()
            }
        ).returning(ComicIssue)

        result = await db.execute(stmt)
        await db.commit()
        return result.scalar_one_or_none()

    async def cache_character(self, db: AsyncSession, char_data: Dict) -> ComicCharacter:
        """Cache a character from Metron response."""
        if not char_data or not char_data.get('id'):
            return None

        stmt = insert(ComicCharacter).values(
            metron_id=char_data['id'],
            name=char_data.get('name', 'Unknown'),
            alias=char_data.get('alias'),
            description=char_data.get('desc'),
            image=char_data.get('image'),
            raw_data=char_data,
            updated_at=datetime.utcnow()
        ).on_conflict_do_update(
            index_elements=['metron_id'],
            set_={
                'name': char_data.get('name', 'Unknown'),
                'alias': char_data.get('alias'),
                'description': char_data.get('desc'),
                'image': char_data.get('image'),
                'raw_data': char_data,
                'updated_at': datetime.utcnow()
            }
        ).returning(ComicCharacter)

        result = await db.execute(stmt)
        await db.commit()
        return result.scalar_one_or_none()

    async def cache_creator(self, db: AsyncSession, creator_data: Dict) -> ComicCreator:
        """Cache a creator from Metron response."""
        if not creator_data or not creator_data.get('id'):
            return None

        stmt = insert(ComicCreator).values(
            metron_id=creator_data['id'],
            name=creator_data.get('name', 'Unknown'),
            description=creator_data.get('desc'),
            image=creator_data.get('image'),
            raw_data=creator_data,
            updated_at=datetime.utcnow()
        ).on_conflict_do_update(
            index_elements=['metron_id'],
            set_={
                'name': creator_data.get('name', 'Unknown'),
                'description': creator_data.get('desc'),
                'image': creator_data.get('image'),
                'raw_data': creator_data,
                'updated_at': datetime.utcnow()
            }
        ).returning(ComicCreator)

        result = await db.execute(stmt)
        await db.commit()
        return result.scalar_one_or_none()

    # ==========================================================================
    # PUBLIC METHODS - These are what the API routes call
    # ==========================================================================

    async def search_issues(
        self,
        db: AsyncSession,
        series_name: Optional[str] = None,
        number: Optional[str] = None,
        publisher_name: Optional[str] = None,
        cover_year: Optional[int] = None,
        upc: Optional[str] = None,
        page: int = 1,
        user_id: Optional[int] = None,
        ip_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Search for issues - fetches from Metron and caches EVERYTHING.
        """
        start_time = time.time()

        # Fetch from Metron
        result = await metron_service.search_issues(
            series_name=series_name,
            number=number,
            publisher_name=publisher_name,
            cover_year=cover_year,
            upc=upc,
            page=page
        )

        duration_ms = int((time.time() - start_time) * 1000)

        # Log the API call
        await self.log_api_call(
            db=db,
            endpoint="issue",
            params={
                "series_name": series_name,
                "number": number,
                "publisher_name": publisher_name,
                "cover_year": cover_year,
                "upc": upc,
                "page": page
            },
            response_code=200,
            response_size=len(str(result)),
            duration_ms=duration_ms,
            user_id=user_id,
            ip_address=ip_address
        )

        # Cache ALL results
        for issue_data in result.get('results', []):
            await self.cache_issue(db, issue_data)

        return result

    async def get_issue_detail(
        self,
        db: AsyncSession,
        issue_id: int,
        user_id: Optional[int] = None,
        ip_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get detailed issue info - fetches ALL data points from Metron.
        """
        start_time = time.time()

        # Fetch full details from Metron
        result = await metron_service.get_issue(issue_id)

        duration_ms = int((time.time() - start_time) * 1000)

        # Log the API call
        await self.log_api_call(
            db=db,
            endpoint=f"issue/{issue_id}",
            params={},
            response_code=200,
            response_size=len(str(result)),
            duration_ms=duration_ms,
            user_id=user_id,
            ip_address=ip_address
        )

        # Cache the full issue data
        await self.cache_issue(db, result)

        # Cache all characters
        for char in result.get('characters', []):
            await self.cache_character(db, char)

        # Cache all creators
        for creator in result.get('credits', []):
            if creator.get('creator'):
                await self.cache_creator(db, creator['creator'])

        return result

    async def search_series(
        self,
        db: AsyncSession,
        name: Optional[str] = None,
        publisher_name: Optional[str] = None,
        year_began: Optional[int] = None,
        page: int = 1,
        user_id: Optional[int] = None,
        ip_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """Search for series and cache results."""
        start_time = time.time()

        result = await metron_service.search_series(
            name=name,
            publisher_name=publisher_name,
            year_began=year_began,
            page=page
        )

        duration_ms = int((time.time() - start_time) * 1000)

        await self.log_api_call(
            db=db,
            endpoint="series",
            params={"name": name, "publisher_name": publisher_name, "year_began": year_began, "page": page},
            response_code=200,
            response_size=len(str(result)),
            duration_ms=duration_ms,
            user_id=user_id,
            ip_address=ip_address
        )

        # Cache all series
        for series_data in result.get('results', []):
            await self.cache_series(db, series_data)

        return result

    async def get_publishers(
        self,
        db: AsyncSession,
        page: int = 1,
        user_id: Optional[int] = None,
        ip_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get publishers and cache them."""
        start_time = time.time()

        result = await metron_service.get_publishers(page=page)

        duration_ms = int((time.time() - start_time) * 1000)

        await self.log_api_call(
            db=db,
            endpoint="publisher",
            params={"page": page},
            response_code=200,
            response_size=len(str(result)),
            duration_ms=duration_ms,
            user_id=user_id,
            ip_address=ip_address
        )

        # Cache all publishers
        for pub_data in result.get('results', []):
            await self.cache_publisher(db, pub_data)

        return result


# Singleton instance
comic_cache = ComicCacheService()
