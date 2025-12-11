# Backend Repository Migration

## Summary
The FastAPI/Railway backend has been moved out of this repository into its own repo located at `F:\apps\mdm_comics_backend` (pending push to its remote). Frontend (`mdm_comics_fresh`) now contains React/Vite assets only, eliminating the accidental cross-builds we saw on Railway.

## Rationale
- Aligns with governance (constitution.json §§1,2,12) calling for modular architecture and infra parity per deployable.
- Prevents Railway from applying `railway.json` meant for the frontend onto the backend service (root cause of the earlier mix-up).
- Simplifies CI/CD: each repo can now run its own lint/test/order_66 gates without interference.

## Files Moved
| Source (old repo) | Destination (backend repo) |
| --- | --- |
| `backend/**/*` | `mdm_comics_backend/**/*` |
| `analyze_gcd.py` | `mdm_comics_backend/analyze_gcd.py` |
| `check_jaime.py` | `mdm_comics_backend/check_jaime.py` |
| `check_user.py` | `mdm_comics_backend/check_user.py` |
| `fix_jaime.py` | `mdm_comics_backend/fix_jaime.py` |
| `promote_admin.py` | `mdm_comics_backend/promote_admin.py` |
| `register_user.py` | `mdm_comics_backend/register_user.py` |
| `reset_pw.py` | `mdm_comics_backend/reset_pw.py` |

## Next Steps
1. Initialize git inside `F:\apps\mdm_comics_backend` (if permissions allow) and push to a new remote, e.g., `github.com/<org>/mdm_comics_backend`.
2. Update Railway backend service to reference the new repo (or set `railway.json` overrides pointing at the new directory).
3. Wire CI/CD (lint/test/order_66) for both repos separately.

## Frontend Repo Expectations
- This repo now owns only the React storefront and related assets.
- Backend consumers should pull from the new repo and expose APIs separately; frontend communicates via the documented HTTP endpoints.
