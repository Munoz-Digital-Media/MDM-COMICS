# Repository Guidelines

## Project Structure & Module Organization
- Monorepo with four deployables: `mdm_comics_frontend/` (React 18 + Vite), `mdm_comics_backend/` (FastAPI API, jobs, Alembic), `mdm_comics_middleware/` (FastAPI utilities), and `mdm_comics_cron_jobs/` (Railway wrapper that runs backend cron). Shared datasets, CGC PDFs/labels, and ML artifacts live under `assets/` (gitignored for size/privacy).
- Backend ML helpers live in `mdm_comics_backend/app/ml/` with label registry, text encoders, and PDF/signature parsers; rerun `assets/cgc/update_cgc_labels.py`, `assets/cgc/build_pdf_corpus.py`, and `assets/cgc/signatures_2026/parse_signings.py` whenever source CGC files change.
- Infra: each service ships its own `railway.json` and Dockerfile; backend/middleware Docker contexts expect the repo root. Python is pinned to 3.12 via `.python-version`.

## Build, Test, and Development Commands
- Frontend: `cd mdm_comics_frontend && npm install && npm run dev` (Vite dev server); `npm run build && npm run preview` to smoke prod bundles; `npm run lint` plus `npm run test`/`test:unit`/`test:integration` for Vitest.
- Backend: `cd mdm_comics_backend && python -m venv .venv && . .venv/Scripts/Activate.ps1 && pip install -r requirements.txt && uvicorn app.main:app --reload --port 8000`; cron locally via `python run_cron.py`; migrations via `alembic upgrade head`.
- Middleware: `cd mdm_comics_middleware && python -m venv .venv && . .venv/Scripts/Activate.ps1 && pip install -r requirements.txt && uvicorn app.main:app --reload --port 4100`.
- Data/ML refresh: after adding CGC PDFs or label HTML, run the refresh scripts above; keep generated artifacts under `assets/**` (they stay out of git).

## Coding Style & Naming Conventions
- JavaScript/JSX: 2-space indent, components in `PascalCase`, hooks/functions `camelCase`; keep Tailwind class lists readable and colocate tests under `src/__tests__/unit|integration`. ESLint (`npm run lint`) is the authority.
- Python: PEP 8 with 4-space indent, snake_case modules/functions, type hints for new code, and docstrings on services/schemas. Place side-effectful scripts in `scripts/` or `assets/**` rather than `app/`.
- Config: copy `.env.example` when present; never commit secrets. Prefer Railway env overrides for deploys; keep large local-only assets in `assets/`.

## Testing Guidelines
- Frontend: Vitest + Testing Library; mock network calls. Use `npm run test:unit` for UI units and `npm run test:integration` for router/API hooks.
- Backend & Middleware: Pytest with `--cov-fail-under=100` and discovery under `tests/test_*.py`. Run `pytest` from each service root; coverage XML writes to `coverage/*.xml`.
- Add fixtures/mocks instead of hitting live services; seed data belongs in `mdm_comics_backend/data/` or temporary fixtures, not in tracked code.

## Commit & Pull Request Guidelines
- Use Conventional Commits with scoped prefixes per service/feature (e.g., `feat(backend): add label embeddings cache`, `fix(frontend): guard checkout totals`). Reference tickets when applicable.
- PRs: include a concise description, linked issues, screenshots for UI changes, and a checklist of commands/tests run. Call out infra changes (Dockerfile, Railway, migrations) and any updates to `assets/**` regeneration scripts.
