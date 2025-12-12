import os
import sys
from pathlib import Path

# Ensure the project root is importable when executing as a script.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import uvicorn


def _read_port() -> int:
    """Fetch and validate the PORT environment variable."""
    value = os.environ.get("PORT", "8000")
    try:
        return int(value)
    except ValueError as exc:  # pragma: no cover - fatal configuration
        raise SystemExit(f"Invalid PORT '{value}': {exc}") from exc


def main() -> None:
    port = _read_port()
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover - last line of defense
        print(f"Failed to start API: {exc}", file=sys.stderr)
        raise
