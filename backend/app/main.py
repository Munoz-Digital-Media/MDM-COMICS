"""
MDM Comics Backend
FastAPI application entry point
"""
import json
import logging
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, func

from app.api.routes import products, users, auth, cart, orders, grading, comics, checkout, funkos
from app.core.config import settings
from app.core.database import init_db, AsyncSessionLocal

# Import models to register them with SQLAlchemy
from app.models import (
    User, Product, CartItem, Order, OrderItem, GradeRequest,
    ComicPublisher, ComicSeries, ComicIssue, ComicCharacter,
    ComicCreator, ComicArc, MetronAPILog,
    Funko, FunkoSeriesName
)

logger = logging.getLogger(__name__)


async def import_funkos_if_needed():
    """One-time import of Funko data if database is empty."""
    async with AsyncSessionLocal() as db:
        # Check if we already have Funkos
        result = await db.execute(select(func.count(Funko.id)))
        count = result.scalar() or 0

        if count > 0:
            logger.info(f"Funko database already populated with {count} entries")
            return

        # Find the JSON file
        json_path = Path(__file__).parent.parent / "funko_data.json"
        if not json_path.exists():
            logger.warning(f"Funko data file not found at {json_path}")
            return

        logger.info("Starting one-time Funko import...")

        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        logger.info(f"Loading {len(data)} Funkos...")

        # Cache for series names
        series_cache = {}
        imported = 0
        batch_size = 500

        for i, item in enumerate(data):
            handle = item.get('handle', '')
            if not handle:
                continue

            # Get or create series
            series_list = item.get('series', [])
            funko_series = []

            for series_name in series_list:
                if series_name not in series_cache:
                    new_series = FunkoSeriesName(name=series_name)
                    db.add(new_series)
                    await db.flush()
                    series_cache[series_name] = new_series
                funko_series.append(series_cache[series_name])

            # Create Funko
            funko = Funko(
                handle=handle,
                title=item.get('title', ''),
                image_url=item.get('image', ''),
                series=funko_series
            )
            db.add(funko)
            imported += 1

            # Commit in batches
            if imported % batch_size == 0:
                await db.commit()
                logger.info(f"Imported {imported} Funkos...")

        await db.commit()
        logger.info(f"Funko import complete! {imported} entries added.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database tables on startup."""
    await init_db()
    # Import Funkos if database is empty
    await import_funkos_if_needed()
    yield


app = FastAPI(
    lifespan=lifespan,
    title="MDM Comics API",
    description="AI-powered comic book grading and e-commerce platform",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS - adjust origins for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(users.router, prefix="/api/users", tags=["Users"])
app.include_router(products.router, prefix="/api/products", tags=["Products"])
app.include_router(cart.router, prefix="/api/cart", tags=["Cart"])
app.include_router(orders.router, prefix="/api/orders", tags=["Orders"])
app.include_router(grading.router, prefix="/api/grading", tags=["AI Grading"])
app.include_router(comics.router, prefix="/api", tags=["Comics Database"])
app.include_router(checkout.router, prefix="/api", tags=["Checkout"])
app.include_router(funkos.router, prefix="/api", tags=["Funko Database"])


@app.get("/", tags=["Health"])
async def root():
    return {
        "message": "MDM Comics API",
        "version": "0.1.0",
        "status": "operational"
    }


@app.get("/health", tags=["Health"])
async def health_check():
    return {
        "status": "healthy",
        "database": "connected",
        "ml_model": "loaded"
    }
