"""
User Management System v1.0.0 Migration Script

This script safely migrates the database schema to support the User Management System.
It adds new tables and columns without data loss.

Run: python scripts/migrate_user_management_v1.py

Per constitution_db.json: Safe migration with rollback support.
"""
import asyncio
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def migrate():
    """Run migration to add User Management System tables and columns."""
    engine = create_async_engine(settings.DATABASE_URL)

    async with engine.begin() as conn:
        # ============================================================
        # 1. Add new columns to users table (if they don't exist)
        # ============================================================
        logger.info("Checking users table for new columns...")

        user_columns = [
            ("email_verified_at", "TIMESTAMP WITH TIME ZONE"),
            ("failed_login_attempts", "INTEGER DEFAULT 0"),
            ("locked_until", "TIMESTAMP WITH TIME ZONE"),
            ("lockout_count", "INTEGER DEFAULT 0"),
            ("password_changed_at", "TIMESTAMP WITH TIME ZONE DEFAULT NOW()"),
            ("last_login_at", "TIMESTAMP WITH TIME ZONE"),
            ("last_login_ip_hash", "VARCHAR(64)"),
            ("deleted_at", "TIMESTAMP WITH TIME ZONE"),
        ]

        for col_name, col_type in user_columns:
            try:
                await conn.execute(text(f"""
                    ALTER TABLE users
                    ADD COLUMN IF NOT EXISTS {col_name} {col_type}
                """))
                logger.info(f"  ✓ users.{col_name}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.info(f"  - users.{col_name} (already exists)")
                else:
                    logger.warning(f"  ✗ users.{col_name}: {e}")

        # ============================================================
        # 2. Create roles table
        # ============================================================
        logger.info("Creating roles table...")
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS roles (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) UNIQUE NOT NULL,
                description TEXT,
                permissions JSONB NOT NULL DEFAULT '[]',
                is_system BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_roles_name ON roles(name)"))
        logger.info("  ✓ roles table")

        # ============================================================
        # 3. Create user_roles junction table
        # ============================================================
        logger.info("Creating user_roles table...")
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_roles (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                role_id INTEGER NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
                granted_by_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                granted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                expires_at TIMESTAMP WITH TIME ZONE,
                notes TEXT,
                UNIQUE(user_id, role_id)
            )
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_roles_user_id ON user_roles(user_id)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_roles_role_id ON user_roles(role_id)"))
        logger.info("  ✓ user_roles table")

        # ============================================================
        # 4. Create user_sessions table
        # ============================================================
        logger.info("Creating user_sessions table...")
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                session_token_hash VARCHAR(128) UNIQUE NOT NULL,
                ip_hash VARCHAR(64),
                user_agent_hash VARCHAR(64),
                device_info TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                last_activity_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                revoked_at TIMESTAMP WITH TIME ZONE,
                revoked_by_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                revoke_reason VARCHAR(100)
            )
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_sessions_user_id ON user_sessions(user_id)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_sessions_token_hash ON user_sessions(session_token_hash)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_sessions_active ON user_sessions(user_id) WHERE is_active = TRUE"))
        logger.info("  ✓ user_sessions table")

        # ============================================================
        # 5. Create user_audit_logs table
        # ============================================================
        logger.info("Creating user_audit_logs table...")
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_audit_logs (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                target_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                action VARCHAR(100) NOT NULL,
                resource_type VARCHAR(50),
                resource_id VARCHAR(100),
                details JSONB,
                ip_hash VARCHAR(64),
                user_agent_hash VARCHAR(64),
                session_id INTEGER REFERENCES user_sessions(id) ON DELETE SET NULL,
                prev_hash VARCHAR(64),
                entry_hash VARCHAR(64) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_logs_user_id ON user_audit_logs(user_id)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_logs_target_user_id ON user_audit_logs(target_user_id)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_logs_action ON user_audit_logs(action)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_logs_created_at ON user_audit_logs(created_at)"))
        logger.info("  ✓ user_audit_logs table")

        # ============================================================
        # 6. Create password_reset_tokens table
        # ============================================================
        logger.info("Creating password_reset_tokens table...")
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS password_reset_tokens (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                token_hash VARCHAR(128) UNIQUE NOT NULL,
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                used_at TIMESTAMP WITH TIME ZONE,
                ip_requested VARCHAR(64),
                ip_used VARCHAR(64),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_password_reset_user_id ON password_reset_tokens(user_id)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_password_reset_token_hash ON password_reset_tokens(token_hash)"))
        logger.info("  ✓ password_reset_tokens table")

        # ============================================================
        # 7. Create email_verification_tokens table
        # ============================================================
        logger.info("Creating email_verification_tokens table...")
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS email_verification_tokens (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                email VARCHAR(255) NOT NULL,
                token_hash VARCHAR(128) UNIQUE NOT NULL,
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                verified_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_email_verify_user_id ON email_verification_tokens(user_id)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_email_verify_token_hash ON email_verification_tokens(token_hash)"))
        logger.info("  ✓ email_verification_tokens table")

        # ============================================================
        # 8. Create dsar_requests table
        # ============================================================
        logger.info("Creating dsar_requests table...")
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dsar_requests (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                request_type VARCHAR(20) NOT NULL,
                status VARCHAR(20) DEFAULT 'pending',
                reason TEXT,
                processed_at TIMESTAMP WITH TIME ZONE,
                processed_by_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                export_file_path VARCHAR(500),
                export_expires_at TIMESTAMP WITH TIME ZONE,
                notes TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_dsar_user_id ON dsar_requests(user_id)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_dsar_status ON dsar_requests(status)"))
        logger.info("  ✓ dsar_requests table")

        # ============================================================
        # 9. Seed system roles
        # ============================================================
        logger.info("Seeding system roles...")

        system_roles = [
            ("customer", "Default role for registered customers",
             '["orders:read", "orders:create", "profile:read", "profile:update", "sessions:read"]'),
            ("admin", "Full system access", '["*"]'),
            ("support", "Customer support access",
             '["users:read", "users:unlock", "orders:read", "orders:update", "sessions:read"]'),
            ("inventory", "Inventory management access",
             '["products:*", "inventory:*"]'),
        ]

        for name, description, permissions in system_roles:
            await conn.execute(text("""
                INSERT INTO roles (name, description, permissions, is_system)
                VALUES (:name, :description, :permissions::jsonb, TRUE)
                ON CONFLICT (name) DO UPDATE SET
                    description = EXCLUDED.description,
                    permissions = EXCLUDED.permissions,
                    is_system = TRUE,
                    updated_at = NOW()
            """), {"name": name, "description": description, "permissions": permissions})
            logger.info(f"  ✓ {name} role")

        # ============================================================
        # 10. Assign admin role to existing admins
        # ============================================================
        logger.info("Assigning admin role to existing admin users...")
        await conn.execute(text("""
            INSERT INTO user_roles (user_id, role_id, notes)
            SELECT u.id, r.id, 'Migrated from is_admin flag'
            FROM users u
            CROSS JOIN roles r
            WHERE u.is_admin = TRUE AND r.name = 'admin'
            ON CONFLICT (user_id, role_id) DO NOTHING
        """))

        # ============================================================
        # 11. Assign customer role to non-admin users
        # ============================================================
        logger.info("Assigning customer role to existing non-admin users...")
        await conn.execute(text("""
            INSERT INTO user_roles (user_id, role_id, notes)
            SELECT u.id, r.id, 'Default role assigned during migration'
            FROM users u
            CROSS JOIN roles r
            WHERE u.is_admin = FALSE AND r.name = 'customer'
            ON CONFLICT (user_id, role_id) DO NOTHING
        """))

        logger.info("\n✅ Migration completed successfully!")
        logger.info("New tables: roles, user_roles, user_sessions, user_audit_logs,")
        logger.info("           password_reset_tokens, email_verification_tokens, dsar_requests")
        logger.info("Updated: users table with lockout, verification, and soft-delete fields")

    await engine.dispose()


async def rollback():
    """Rollback migration (use with caution - drops tables!)."""
    logger.warning("⚠️  ROLLBACK: This will DROP all User Management System tables!")
    logger.warning("    Data in these tables will be PERMANENTLY DELETED.")

    confirm = input("Type 'ROLLBACK' to confirm: ")
    if confirm != "ROLLBACK":
        logger.info("Rollback cancelled.")
        return

    engine = create_async_engine(settings.DATABASE_URL)

    async with engine.begin() as conn:
        # Drop in reverse order of creation (respecting foreign keys)
        tables = [
            "dsar_requests",
            "email_verification_tokens",
            "password_reset_tokens",
            "user_audit_logs",
            "user_sessions",
            "user_roles",
            "roles",
        ]

        for table in tables:
            try:
                await conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                logger.info(f"  ✓ Dropped {table}")
            except Exception as e:
                logger.warning(f"  ✗ {table}: {e}")

        # Remove columns from users table
        columns = [
            "email_verified_at",
            "failed_login_attempts",
            "locked_until",
            "lockout_count",
            "password_changed_at",
            "last_login_at",
            "last_login_ip_hash",
            "deleted_at",
        ]

        for col in columns:
            try:
                await conn.execute(text(f"ALTER TABLE users DROP COLUMN IF EXISTS {col}"))
                logger.info(f"  ✓ Dropped users.{col}")
            except Exception as e:
                logger.warning(f"  ✗ users.{col}: {e}")

        logger.info("\n✅ Rollback completed.")

    await engine.dispose()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="User Management System Migration")
    parser.add_argument("--rollback", action="store_true", help="Rollback migration (destructive!)")
    args = parser.parse_args()

    if args.rollback:
        asyncio.run(rollback())
    else:
        asyncio.run(migrate())
