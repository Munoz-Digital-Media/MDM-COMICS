"""
Deprecation Warning Module v1.0.0

Per constitution_db.json Section 1 (Change Control):
- Schema changes only via Alembic migrations
- Data operations via unified services, not standalone scripts

This module provides deprecation warnings for legacy scripts that:
1. Perform direct database schema modifications (ALTER TABLE, CREATE TABLE)
2. Use standalone data import logic instead of the DataIngestionService
3. Bypass the admin API for operations

Usage:
    # At the top of any legacy script
    from scripts._deprecated import deprecated_script
    deprecated_script(
        script_name="import_pricecharting_comics.py",
        replacement="python scripts/mdm_admin.py ingest csv --source pricecharting ...",
        reason="Use unified DataIngestionService for bulk imports"
    )

The warning will:
1. Print a clear deprecation notice
2. Log the usage for tracking
3. Require explicit confirmation to proceed
4. Eventually block execution entirely (after grace period)
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Grace period - after this date, scripts will refuse to run
DEPRECATION_HARD_BLOCK_DATE = datetime(2025, 3, 1)  # March 1, 2025

# Color codes for terminal output
RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"


def deprecated_script(
    script_name: str,
    replacement: str,
    reason: str,
    hard_block: bool = False,
    bypass_env_var: Optional[str] = None,
) -> None:
    """
    Display deprecation warning for a legacy script.

    Args:
        script_name: Name of the deprecated script
        replacement: The new command/method to use instead
        reason: Why this script is deprecated
        hard_block: If True, refuse to run regardless of date
        bypass_env_var: Environment variable that bypasses warning (for CI/CD)
    """
    # Check for bypass
    if bypass_env_var and os.environ.get(bypass_env_var):
        logger.warning(f"[DEPRECATED] {script_name} - bypassed via {bypass_env_var}")
        return

    now = datetime.now()

    # Build warning message
    border = "=" * 70
    warning_lines = [
        "",
        f"{RED}{border}{RESET}",
        f"{RED}{BOLD}  DEPRECATION WARNING{RESET}",
        f"{RED}{border}{RESET}",
        "",
        f"  {YELLOW}Script:{RESET} {script_name}",
        f"  {YELLOW}Status:{RESET} DEPRECATED",
        "",
        f"  {YELLOW}Reason:{RESET}",
        f"    {reason}",
        "",
        f"  {GREEN}Use instead:{RESET}",
        f"    {replacement}",
        "",
    ]

    # Check if past hard block date
    if hard_block or now >= DEPRECATION_HARD_BLOCK_DATE:
        warning_lines.extend([
            f"  {RED}This script has been DISABLED.{RESET}",
            f"  {RED}Please use the replacement command above.{RESET}",
            "",
            f"{RED}{border}{RESET}",
            "",
        ])
        print("\n".join(warning_lines), file=sys.stderr)
        logger.error(f"[DEPRECATED] Blocked execution of {script_name}")
        sys.exit(1)

    # Calculate days until hard block
    days_remaining = (DEPRECATION_HARD_BLOCK_DATE - now).days

    warning_lines.extend([
        f"  {YELLOW}This script will stop working in {days_remaining} days.{RESET}",
        f"  {YELLOW}Please migrate to the new method before {DEPRECATION_HARD_BLOCK_DATE.strftime('%Y-%m-%d')}.{RESET}",
        "",
        f"{RED}{border}{RESET}",
        "",
    ])

    print("\n".join(warning_lines), file=sys.stderr)

    # Log the usage
    logger.warning(f"[DEPRECATED] Script {script_name} invoked - {days_remaining} days until disabled")

    # Require confirmation
    try:
        response = input(f"{YELLOW}Continue anyway? [y/N]: {RESET}")
        if response.lower() != "y":
            print("Cancelled.")
            sys.exit(0)
        print()
    except (EOFError, KeyboardInterrupt):
        print("\nCancelled.")
        sys.exit(0)


def deprecated_function(
    func_name: str,
    replacement: str,
    reason: str,
) -> None:
    """
    Log deprecation warning for a function (non-blocking).

    Use this for internal functions that are being phased out.
    """
    logger.warning(
        f"[DEPRECATED] Function '{func_name}' is deprecated. "
        f"Reason: {reason}. Use: {replacement}"
    )


def check_for_ddl(script_path: str) -> list:
    """
    Check a script file for direct DDL statements.

    Per constitution_db.json - Schema changes must go through Alembic.

    Returns:
        List of (line_number, statement) tuples for DDL found
    """
    ddl_patterns = [
        "ALTER TABLE",
        "CREATE TABLE",
        "DROP TABLE",
        "ADD COLUMN",
        "DROP COLUMN",
        "CREATE INDEX",
        "DROP INDEX",
        "ALTER COLUMN",
    ]

    findings = []

    try:
        with open(script_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line_upper = line.upper()
                for pattern in ddl_patterns:
                    if pattern in line_upper:
                        # Skip comments
                        stripped = line.strip()
                        if not stripped.startswith("#") and not stripped.startswith("--"):
                            findings.append((line_num, pattern, line.strip()[:80]))
    except Exception as e:
        logger.error(f"Failed to check {script_path} for DDL: {e}")

    return findings


# =============================================================================
# REPLACEMENT MAPPINGS
# =============================================================================

SCRIPT_REPLACEMENTS = {
    "trigger_gcd_import.py": {
        "replacement": "python scripts/mdm_admin.py gcd import --max-records N",
        "reason": "Use unified MDM Admin CLI for all pipeline operations",
    },
    "trigger_mse.py": {
        "replacement": "python scripts/mdm_admin.py mse run --batch-size N",
        "reason": "Use unified MDM Admin CLI for all pipeline operations",
    },
    "upload_covers.py": {
        "replacement": "python scripts/mdm_admin.py covers ingest --folder /path",
        "reason": "Use CoverIngestionService via API for proper S3 handling",
    },
    "import_pricecharting_comics.py": {
        "replacement": "python scripts/mdm_admin.py ingest csv --source pricecharting ...",
        "reason": "Use DataIngestionService for bulk imports with proper validation",
    },
    "migrate_brand_assets_v1.py": {
        "replacement": "alembic revision --autogenerate -m 'add brand assets'",
        "reason": "Schema changes must use Alembic migrations per constitution_db.json",
    },
    "migrate_device_type_v1.py": {
        "replacement": "alembic revision --autogenerate -m 'add device type'",
        "reason": "Schema changes must use Alembic migrations per constitution_db.json",
    },
    "migrate_image_columns.py": {
        "replacement": "alembic revision --autogenerate -m 'add image columns'",
        "reason": "Schema changes must use Alembic migrations per constitution_db.json",
    },
    "migrate_match_review_v1.py": {
        "replacement": "alembic revision --autogenerate -m 'add match review'",
        "reason": "Schema changes must use Alembic migrations per constitution_db.json",
    },
    "migrate_multi_source_enrichment.py": {
        "replacement": "alembic revision --autogenerate -m 'add multi source enrichment'",
        "reason": "Schema changes must use Alembic migrations per constitution_db.json",
    },
}


def get_replacement(script_name: str) -> Optional[dict]:
    """Get replacement info for a deprecated script."""
    return SCRIPT_REPLACEMENTS.get(script_name)


# =============================================================================
# AUTO-CHECK ON IMPORT
# =============================================================================

def auto_deprecate():
    """
    Automatically check if the importing script is deprecated.

    Call this at the end of this module to auto-check.
    """
    # Get the calling script name
    import inspect
    frame = inspect.currentframe()
    if frame and frame.f_back:
        caller_file = frame.f_back.f_globals.get("__file__", "")
        if caller_file:
            script_name = Path(caller_file).name
            replacement_info = get_replacement(script_name)
            if replacement_info:
                deprecated_script(
                    script_name=script_name,
                    replacement=replacement_info["replacement"],
                    reason=replacement_info["reason"],
                )
