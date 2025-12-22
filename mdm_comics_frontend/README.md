# MDM Comics Frontend (React + Vite)

## Stack
- React 18, Vite, Tailwind
- Node 18+ recommended
- Dockerfile + `railway.json` for Railway deploys

## Local Development
```bash
cd mdm_comics_frontend
npm install
npm run dev       # start Vite dev server
# npm run build && npm run preview for production check
```

## Scripts
- `npm run dev` — Vite dev server
- `npm run build` — Production bundle
- `npm run preview` — Serve built bundle
- `npm run lint` — ESLint
- `npm run test` / `test:unit` / `test:integration` — Vitest suites

## Deployment (Railway)
- Dockerfile at `mdm_comics_frontend/Dockerfile`
- Root Directory: `mdm_comics_frontend/`
- Healthcheck: `/` (configured in `railway.json`)

## Notes
- Conventions and other static data live in `src/config/` (dev-only copies); large assets remain in `assets/` which is gitignored.
- Ensure `index.css` allows page scrolling (no `overflow: hidden` on `html, body`).
- For admin console features, see `src/components/admin/`.
