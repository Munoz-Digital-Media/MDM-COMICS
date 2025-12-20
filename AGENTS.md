# Repository Guidelines

## Project Structure & Module Organization
- Monorepo root contains four deployable services: `mdm_comics_frontend/` (React/Vite SPA), `mdm_comics_backend/` (FastAPI API + jobs), `mdm_comics_middleware/` (FastAPI utilities), and `mdm_comics_cron_jobs/` (Railway wrapper for backend cron runner). Shared docs live in `docs/` and governance artifacts in `implementations/`; `assets/` is local-only.
- Each service has its own `railway.json` and `Dockerfile*`; infra files and root service folders are CODEOWNERS-locked to @JD.

## Build, Test, and Development Commands
- Frontend: `cd mdm_comics_frontend && npm install && npm run dev` for Vite dev server; `npm run build` for production bundle; `npm run lint` for ESLint; `npm run test`, `test:unit`, or `test:integration` for Vitest suites; `npm run preview` to serve the built app.
- Backend: `cd mdm_comics_backend && python -m venv .venv && . .venv/Scripts/Activate.ps1 && pip install -r requirements.txt` then `uvicorn app.main:app --reload --port 8000`; run jobs via `python run_cron.py`.
- Middleware: `cd mdm_comics_middleware && python -m venv .venv && . .venv/Scripts/Activate.ps1 && pip install -r requirements.txt && uvicorn app.main:app --reload --port 4100`.
- Cron wrapper: `cd mdm_comics_backend && python run_cron.py` (shares backend codebase; deployment config sits in `mdm_comics_cron_jobs/`).

## Coding Style & Naming Conventions
- JavaScript/JSX: 2-space indent, React components in `PascalCase`, hooks/functions `camelCase`, tests in `src/__tests__`. Run `npm run lint` before PRs.
- Python: PEP 8 (4-space indent), type hints for new modules, snake_case for files/functions. Optional formatters (black/isort) are commented in `requirements.txt`—match existing import/grouping order when touching files.
- Keep configuration in `.env` (copy from `.env.example` when present); never commit secrets. Prefer `railway.json` overrides for service-specific env when deploying.

## Testing Guidelines
- Frontend uses Vitest + Testing Library; prefer colocated tests under `src/__tests__/unit|integration`. Aim for meaningful coverage of UI state, API hooks, and routing guards.
- Backend and middleware use Pytest with 100% coverage gates (`--cov-fail-under=100`) and `tests/test_*.py` naming. Run `pytest` from each service root; coverage reports write to `coverage/*.xml`.
- Add fixtures/mocks rather than hitting real APIs; seed data lives in `mdm_comics_backend/data/` when needed for tests.

## Commit & Pull Request Guidelines
- Follow Conventional Commits seen in history (e.g., `fix(bundles): move /featured route before /{slug}`); include a scope tied to the service or feature (`feat(homepage): ...`), and reference tickets like `GOLF-01` in the summary when applicable.
- PRs should include a short description, links to related issues, screenshots for UI changes, and a checklist of commands/tests executed. Call out infra changes (Dockerfile, `railway.json`) explicitly because they require CODEOWNERS review.
