"""
Data Source Adapters v1.0.0

Adapters for external data sources per Data Acquisition Pipeline spec.
Each adapter handles authentication, pagination, normalization, and error handling.
"""
from app.adapters.pricecharting import PriceChartingAdapter
from app.adapters.metron_adapter import MetronAdapter
from app.adapters.gcd import GCDAdapter
from app.adapters.marvel_fandom import MarvelFandomAdapter

__all__ = [
    "PriceChartingAdapter",
    "MetronAdapter",
    "GCDAdapter",
    "MarvelFandomAdapter",
]
