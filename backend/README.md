# MDM Comics Backend

FastAPI backend with AI-powered comic grading.

## Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp .env.example .env
# Edit .env with your values

# Start PostgreSQL (via Docker or local install)
docker run -d \
  --name mdm-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=mdm_comics \
  -p 5432:5432 \
  postgres:16

# Run migrations
alembic upgrade head

# Start dev server
uvicorn app.main:app --reload --port 8000
```

## API Docs

Once running, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Project Structure

```
backend/
├── app/
│   ├── api/
│   │   ├── deps.py          # Dependencies (auth, db)
│   │   └── routes/          # API endpoints
│   │       ├── auth.py      # Login, register, refresh
│   │       ├── users.py     # User profile
│   │       ├── products.py  # Product CRUD
│   │       ├── cart.py      # Shopping cart
│   │       ├── orders.py    # Order management
│   │       └── grading.py   # AI grade estimation
│   ├── core/
│   │   ├── config.py        # Settings
│   │   ├── database.py      # DB connection
│   │   └── security.py      # Auth utilities
│   ├── models/              # SQLAlchemy models
│   ├── schemas/             # Pydantic schemas
│   ├── services/            # Business logic
│   ├── ml/                  # ML model & inference
│   │   └── grade_estimator.py
│   └── main.py              # FastAPI app
├── alembic/                 # DB migrations
├── tests/
├── requirements.txt
└── .env.example
```

## Key Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /api/auth/register | Create account |
| POST | /api/auth/login | Get tokens |
| GET | /api/products | List products |
| POST | /api/cart/items | Add to cart |
| POST | /api/orders | Create order |
| POST | /api/grading/estimate | AI grade estimate |

## AI Grading

The `/api/grading/estimate` endpoint analyzes comic book images and returns:

```json
{
  "grade": 9.4,
  "confidence": 0.87,
  "grade_label": "Near Mint (9.4)",
  "factors": {
    "corners": 9.2,
    "spine": 9.6,
    "pages": 9.4,
    "centering": 8.8
  }
}
```

Currently returns mock data. Replace `app/ml/grade_estimator.py` with actual model inference when trained.

## Next Steps

1. [ ] Set up PostgreSQL database
2. [ ] Run Alembic migrations
3. [ ] Seed initial product data
4. [ ] Train grade estimation model
5. [ ] Add payment processing (Stripe)
6. [ ] Deploy to production
