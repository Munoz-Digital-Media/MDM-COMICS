# MDM Comics Monorepo

AI-powered comic book and collectibles platform. Every deployable service now lives in a prefixed directory under `F:\apps\mdm_comics` so Railway stays in sync with the repo.

## Live Features

- AI-powered CGC grade estimates on ungraded books
- Production storefront with cart + checkout flows
- AuthN/AuthZ, analytics, and observability hooks
- Middleware layer for address normalization + header propagation
- Cron runner for enrichment, pricing, and DLQ recovery jobs

## Tech Stack

| Layer | Stack | Folder |
| --- | --- | --- |
| Frontend | React 18 + Vite + Tailwind | `mdm_comics_frontend/` |
| Backend API | FastAPI + PostgreSQL | `mdm_comics_backend/` |
| Cron Jobs | Pipeline scheduler runner (FastAPI jobs) | `mdm_comics_cron_jobs/` |
| Middleware | FastAPI service for cross-cutting utilities | `mdm_comics_middleware/` |

## Getting Started

### Frontend (Vite)
```bash
cd mdm_comics_frontend
npm install
npm run dev
# npm run build && npm run preview for production testing
```

### Backend API
```bash
cd mdm_comics_backend
python -m venv .venv
. .venv/Scripts/Activate.ps1  # PowerShell
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

### Cron Jobs
```bash
cd mdm_comics_cron_jobs
python -m venv .venv
. .venv/Scripts/Activate.ps1
pip install -r ..\mdm_comics_backend\requirements.txt
python run_cron.py
```

### Middleware Service
```bash
cd mdm_comics_middleware
python -m venv .venv
. .venv/Scripts/Activate.ps1
pip install -r requirements.txt
uvicorn app.main:app --reload --port 4100
```

## Repository Layout

```
mdm_comics/
├─ mdm_comics_frontend/      # React/Vite SPA + Dockerfile + railway.json
├─ mdm_comics_backend/       # FastAPI app, scripts, Dockerfile, requirements
├─ mdm_comics_cron_jobs/     # Dedicated cron runner + Dockerfile + config
├─ mdm_comics_middleware/    # Shared middleware FastAPI service
├─ docs/                     # Product/architecture docs
├─ implementations/          # Governance + design artifacts
└─ assets/                   # Local assets (gitignored)
```

## Railway Services

| Railway Service | Root Directory | Config |
| --- | --- | --- |
| `MDM-COMICS-FRONTEND` | `mdm_comics_frontend/` | `mdm_comics_frontend/railway.json` |
| `MDM-COMICS-BACKEND` | *(repo root)* | `mdm_comics_backend/railway.json` |
| `MDM-COMICS-CRON-JOBS` | `mdm_comics_cron_jobs/` | `mdm_comics_cron_jobs/railway.json` |
| `MDM-COMICS-MIDDLEWARE` | *(repo root)* | `mdm_comics_middleware/railway.json` |

**Important:** Backend and Middleware services must have their Root Directory set to the **repository root** (leave blank or `/`), not their service folder. Their Dockerfiles use absolute paths from the repo root for build context. Frontend and Cron Jobs use their respective folders as Root Directory.

## Brand Colors

- **Primary:** Orange (#F97316)
- **Background:** Zinc-950 (#09090b)
- **Surface:** Zinc-900 (#18181b)
- **Border:** Zinc-800 (#27272a)

## Demo Account

```
Email: demo@mdmcomics.com
Password: demo123
```
