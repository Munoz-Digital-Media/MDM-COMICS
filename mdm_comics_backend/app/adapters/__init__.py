"""
Data Source Adapters v1.10.0

Adapters for external data sources per Data Acquisition Pipeline spec.
Each adapter handles authentication, pagination, normalization, and error handling.

Multi-Source Enrichment System adapters:
- MetronAdapter: Primary API for metadata (covers, descriptions, creators, characters)
- ComicVineAdapter: Secondary API for metadata enrichment
- ComicBookRealmAdapter: Scraper for covers, pricing, CGC grading data
- MyComicShopAdapter: Scraper for covers, inventory pricing
- GradingToolAdapter: Scraper for AI grading training data
"""
from app.adapters.pricecharting import PriceChartingAdapter
from app.adapters.metron_adapter import MetronAdapter
from app.adapters.gcd import GCDAdapter
from app.adapters.marvel_fandom import MarvelFandomAdapter
from app.adapters.comicvine_adapter import ComicVineAdapter, create_comicvine_adapter
from app.adapters.comicbookrealm_adapter import ComicBookRealmAdapter, create_comicbookrealm_adapter
from app.adapters.mycomicshop_adapter import MyComicShopAdapter, create_mycomicshop_adapter
from app.adapters.gradingtool_adapter import GradingToolAdapter, create_gradingtool_adapter
from app.adapters.robots_checker import RobotsTxtChecker, robots_checker

__all__ = [
    # Legacy adapters
    "PriceChartingAdapter",
    "MetronAdapter",
    "GCDAdapter",
    "MarvelFandomAdapter",
    # MSE v1.10.0 adapters
    "ComicVineAdapter",
    "create_comicvine_adapter",
    "ComicBookRealmAdapter",
    "create_comicbookrealm_adapter",
    "MyComicShopAdapter",
    "create_mycomicshop_adapter",
    "GradingToolAdapter",
    "create_gradingtool_adapter",
    # Robots compliance
    "RobotsTxtChecker",
    "robots_checker",
]
