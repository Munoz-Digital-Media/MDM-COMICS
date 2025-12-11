"""
Database migration script for User Management System
Adds missing tables and columns to production database.

Run via: railway run python migrate_schema.py
"""
import asyncio
import os
import sys

# Ensure app modules can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker


async def migrate():
    """Run all migrations."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL not set")
        return False

    # Convert postgres:// to postgresql+asyncpg://
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif database_url.startswith("postgresql://") and "+asyncpg" not in database_url:
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    print(f"Connecting to database...")
    engine = create_async_engine(database_url, echo=False)

    async with engine.begin() as conn:
        # ============================================================
        # 1. Add missing columns to users table
        # ============================================================
        print("\n=== Adding missing columns to users table ===")

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
                await conn.execute(text(f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {col_name} {col_type}"))
                print(f"  + Added/verified column: users.{col_name}")
            except Exception as e:
                print(f"  ! Error adding users.{col_name}: {e}")

        # ============================================================
        # 2. Create roles table
        # ============================================================
        print("\n=== Creating roles table ===")
        try:
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
            print("  + Created roles table")
        except Exception as e:
            print(f"  ! Error creating roles table: {e}")

        # Seed system roles
        print("\n=== Seeding system roles ===")
        system_roles = [
            ("customer", "Default customer role", '["orders:read", "orders:create", "profile:read", "profile:update"]'),
            ("admin", "Full administrative access", '["*"]'),
            ("support", "Customer support role", '["users:read", "orders:read", "orders:update"]'),
            ("inventory", "Inventory management", '["products:*", "inventory:*"]'),
        ]

        for name, desc, perms in system_roles:
            try:
                await conn.execute(text("""
                    INSERT INTO roles (name, description, permissions, is_system)
                    VALUES (:name, :desc, :perms::jsonb, TRUE)
                    ON CONFLICT (name) DO NOTHING
                """), {"name": name, "desc": desc, "perms": perms})
                print(f"  + Seeded role: {name}")
            except Exception as e:
                print(f"  ! Error seeding role {name}: {e}")

        # ============================================================
        # 3. Create user_roles junction table
        # ============================================================
        print("\n=== Creating user_roles table ===")
        try:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_roles (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    role_id INTEGER NOT NULL REFERENCES roles(id) ON DELETE RESTRICT,
                    assigned_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
                    assigned_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    expires_at TIMESTAMP WITH TIME ZONE,
                    UNIQUE(user_id, role_id)
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_roles_user_id ON user_roles(user_id)"))
            print("  + Created user_roles table")
        except Exception as e:
            print(f"  ! Error creating user_roles table: {e}")

        # ============================================================
        # 4. Create user_sessions table
        # ============================================================
        print("\n=== Creating user_sessions table ===")
        try:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_sessions (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    token_jti VARCHAR(36) UNIQUE NOT NULL,
                    refresh_jti VARCHAR(36),
                    device_fingerprint_hash VARCHAR(64),
                    user_agent_hash VARCHAR(64),
                    ip_address_hash VARCHAR(64),
                    device_type VARCHAR(50),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    last_activity_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    revoked_at TIMESTAMP WITH TIME ZONE,
                    revoke_reason VARCHAR(50)
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_sessions_token_jti ON user_sessions(token_jti)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_user_sessions_user_id ON user_sessions(user_id)"))
            print("  + Created user_sessions table")
        except Exception as e:
            print(f"  ! Error creating user_sessions table: {e}")

        # ============================================================
        # 5. Create user_audit_log table
        # ============================================================
        print("\n=== Creating user_audit_log table ===")
        try:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_audit_log (
                    id BIGSERIAL PRIMARY KEY,
                    ts TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
                    actor_type VARCHAR(20) NOT NULL,
                    actor_id_hash VARCHAR(64) NOT NULL,
                    action VARCHAR(100) NOT NULL,
                    resource_type VARCHAR(50) NOT NULL,
                    resource_id_hash VARCHAR(64),
                    before_hash VARCHAR(128),
                    after_hash VARCHAR(128),
                    outcome VARCHAR(20) NOT NULL,
                    ip_hash VARCHAR(64),
                    event_metadata JSONB DEFAULT '{}',
                    prev_hash VARCHAR(128),
                    entry_hash VARCHAR(128) NOT NULL
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_ts ON user_audit_log(ts)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_actor ON user_audit_log(actor_id_hash, ts)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_resource ON user_audit_log(resource_type, resource_id_hash)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_audit_action ON user_audit_log(action)"))
            print("  + Created user_audit_log table")
        except Exception as e:
            print(f"  ! Error creating user_audit_log table: {e}")

        # ============================================================
        # 6. Create dsar_requests table
        # ============================================================
        print("\n=== Creating dsar_requests table ===")
        try:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dsar_requests (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    request_type VARCHAR(20) NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'pending',
                    requested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    completed_at TIMESTAMP WITH TIME ZONE,
                    export_url_hash VARCHAR(128),
                    processed_by INTEGER REFERENCES users(id),
                    notes TEXT,
                    ledger_tx_id VARCHAR(128)
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_dsar_user ON dsar_requests(user_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_dsar_status ON dsar_requests(status)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_dsar_type ON dsar_requests(request_type)"))
            print("  + Created dsar_requests table")
        except Exception as e:
            print(f"  ! Error creating dsar_requests table: {e}")

        # ============================================================
        # 7. Create email_verifications table
        # ============================================================
        print("\n=== Creating email_verifications table ===")
        try:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS email_verifications (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    token_hash VARCHAR(64) UNIQUE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    verified_at TIMESTAMP WITH TIME ZONE
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_email_verifications_user ON email_verifications(user_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_email_verifications_token ON email_verifications(token_hash)"))
            print("  + Created email_verifications table")
        except Exception as e:
            print(f"  ! Error creating email_verifications table: {e}")

        # ============================================================
        # 8. Create password_resets table
        # ============================================================
        print("\n=== Creating password_resets table ===")
        try:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS password_resets (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    token_hash VARCHAR(64) UNIQUE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    used_at TIMESTAMP WITH TIME ZONE
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_password_resets_user ON password_resets(user_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS ix_password_resets_token ON password_resets(token_hash)"))
            print("  + Created password_resets table")
        except Exception as e:
            print(f"  ! Error creating password_resets table: {e}")

        print("\n=== Migration complete ===")

    await engine.dispose()
    return True


if __name__ == "__main__":
    success = asyncio.run(migrate())
    sys.exit(0 if success else 1)
