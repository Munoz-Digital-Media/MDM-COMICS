# MDM Comics Backend

FastAPI backend with AI-powered comic grading and ML significance scoring.

## Setup

```bash
cd mdm_comics_backend
python -m venv .venv
. .venv/Scripts/Activate.ps1  # or source .venv/bin/activate on macOS/Linux
pip install -r requirements.txt
cp .env.example .env  # configure secrets
uvicorn app.main:app --reload --port 8000
```

## Environment variables (common)

| Name | Required | Notes |
| --- | --- | --- |
| `DATABASE_URL` | yes | Postgres connection string (asyncpg). |
| `SECRET_KEY` | yes | JWT/session signing. |
| `PRICECHARTING_API_TOKEN` | yes (price jobs) | Needed for PriceCharting match/sync. |
| `AWS_*` | if S3 used | Bucket/region/creds for assets. |
| `PORT` | no | Defaults to 8000. |

## Project Structure

```
mdm_comics_backend/
├─ app/                        # FastAPI application package
│  ├─ adapters/               # External API adapters (Marvel Fandom, etc.)
│  ├─ api/                    # Routers + dependencies
│  ├─ core/                   # Config, security, database helpers
│  ├─ jobs/                   # Pipeline + cron tasks
│  ├─ ml/                     # ML utilities
│  ├─ models/                 # SQLAlchemy models
│  ├─ schemas/                # Pydantic DTOs
│  └─ services/               # Business logic
├─ migrations/                 # Alembic migrations
├─ scripts/                    # One-off maintenance scripts
├─ tests/                      # Pytest suite
├─ requirements.txt
└─ run_cron.py
```

## Data Pipeline

The backend runs scheduled jobs to enrich comic data from multiple sources:

| Job | Schedule | Description |
|-----|----------|-------------|
| GCD Import | 15 min | Import issues from Grand Comics Database |
| Cover Enrichment | 30 min | Fetch cover images from Comic Vine |
| Marvel Fandom | 60 min | Extract story-level credits and character appearances |
| PriceCharting Match | 60 min | Match comics to price data |

### Data Sources

- **GCD (Grand Comics Database)**: Core issue metadata, series info, creator credits
- **Marvel Fandom**: Story-level credits, character appearances with event flags
- **Comic Vine**: Cover images, descriptions
- **PriceCharting**: Market pricing data

## ML Significance Scoring

The system uses machine learning to compute comic significance scores. See [ML Implementation Plan](../implementations/20251213_ml_significance_scoring_plan.md) for details.

### Architecture

```
Raw Features → ML Model → Significance Score
```

### Feature Columns

| Column | Description |
|--------|-------------|
| character_count | Total characters appearing |
| first_appearance_count | Number of first appearances |
| death_count | Number of character deaths |
| creator_count | Total unique creators |
| notable_creator_count | High-value creators |
| story_count | Number of stories |
| has_first_appearance | Contains any first appearance |
| has_death | Contains any character death |
| has_key_event | Contains significant event |

The significance_score is the **ML model's output**, not a hardcoded formula. This allows:
- Weights learned from actual market data
- Retraining as market dynamics shift
- SHAP values for prediction interpretability

## Key Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /api/auth/register | Create account |
| POST | /api/auth/login | Get tokens |
| GET | /api/products | List products |
| POST | /api/cart/items | Add to cart |
| POST | /api/orders | Create order |
| POST | /api/grading/estimate | AI grade estimate |
| GET | /api/admin/pipeline/gcd/status | Pipeline status |

## Cron Runner

run_cron.py powers the MDM-COMICS-CRON-JOBS Railway service. It imports app.jobs.pipeline_scheduler, so every cron deployment shares the exact same code as the API.

## API Docs

Once running, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
