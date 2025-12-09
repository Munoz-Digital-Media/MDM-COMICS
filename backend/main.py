"""
MDM Comics API

AI-powered comic book e-commerce platform with CGC grade estimation.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.api import api_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup - database auto-connects via SQLAlchemy engine
    print(f"🚀 {settings.app_name} started")
    yield
    # Shutdown
    print(f"👋 {settings.app_name} shutting down")


# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    description="AI-powered comic book e-commerce platform with CGC grade estimation",
    version=settings.api_version,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_url, "http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(api_router, prefix=f"/api/{settings.api_version}")


# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "app": settings.app_name,
        "version": settings.api_version,
    }


# Root redirect to docs
@app.get("/")
async def root():
    """Root endpoint - redirect to docs."""
    return {
        "message": f"Welcome to {settings.app_name}",
        "docs": "/docs",
        "health": "/health",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
    )
