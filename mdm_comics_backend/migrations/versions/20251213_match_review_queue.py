"""Match Review Queue tables

Revision ID: 20251213_match_review
Revises:
Create Date: 2025-12-13

Per constitution_db.json:
- snake_case for all tables/columns
- PK, FK constraints, updated_at
- Append-only audit table with hash chains
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '20251213_match_review'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ============================================================
    # match_review_queue: Pending matches for human review
    # ============================================================
    op.create_table(
        'match_review_queue',
        sa.Column('id', sa.Integer(), primary_key=True),

        # Source record
        sa.Column('entity_type', sa.String(20), nullable=False),
        sa.Column('entity_id', sa.Integer(), nullable=False),

        # Match candidate
        sa.Column('candidate_source', sa.String(50), nullable=False),
        sa.Column('candidate_id', sa.String(100), nullable=False),
        sa.Column('candidate_name', sa.String(500)),
        sa.Column('candidate_data', postgresql.JSONB),

        # Matching metadata
        sa.Column('match_method', sa.String(50), nullable=False),
        sa.Column('match_score', sa.Integer()),
        sa.Column('match_details', postgresql.JSONB),

        # Queue status
        sa.Column('status', sa.String(20), nullable=False, server_default='pending'),
        sa.Column('is_escalated', sa.Boolean(), server_default='false'),

        # Resolution
        sa.Column('reviewed_by', sa.Integer(), sa.ForeignKey('users.id')),
        sa.Column('reviewed_at', sa.DateTime(timezone=True)),
        sa.Column('resolution_notes', sa.Text()),

        # Optimistic locking
        sa.Column('locked_by', sa.Integer(), sa.ForeignKey('users.id')),
        sa.Column('locked_at', sa.DateTime(timezone=True)),

        # Timestamps
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), onupdate=sa.func.now()),
        sa.Column('expires_at', sa.DateTime(timezone=True)),

        # Constraints
        sa.CheckConstraint("entity_type IN ('comic', 'funko')", name='ck_entity_type'),
        sa.CheckConstraint("status IN ('pending', 'approved', 'rejected', 'skipped', 'expired')", name='ck_status'),
        sa.UniqueConstraint('entity_type', 'entity_id', 'candidate_source', 'candidate_id', name='uq_match_candidate'),
    )

    op.create_index('idx_match_queue_status', 'match_review_queue', ['status'], postgresql_where=sa.text("status = 'pending'"))
    op.create_index('idx_match_queue_entity', 'match_review_queue', ['entity_type', 'entity_id'])
    op.create_index('idx_match_queue_escalated', 'match_review_queue', ['is_escalated', 'created_at'], postgresql_where=sa.text("status = 'pending'"))

    # ============================================================
    # match_audit_log: Immutable audit trail (hash-chained)
    # ============================================================
    op.create_table(
        'match_audit_log',
        sa.Column('id', sa.Integer(), primary_key=True),

        # Action details
        sa.Column('action', sa.String(50), nullable=False),
        sa.Column('entity_type', sa.String(20), nullable=False),
        sa.Column('entity_id', sa.Integer(), nullable=False),

        # Before/after state (hashed per constitution_logging.json)
        sa.Column('before_state_hash', sa.String(128)),
        sa.Column('after_state_hash', sa.String(128)),

        # Actor (pseudonymized)
        sa.Column('actor_type', sa.String(20), nullable=False),
        sa.Column('actor_id_hash', sa.String(128)),

        # Match details
        sa.Column('match_source', sa.String(50)),
        sa.Column('match_id', sa.String(100)),
        sa.Column('match_method', sa.String(50)),
        sa.Column('match_score', sa.Integer()),

        # Immutability - hash chain
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column('log_hash', sa.String(128)),
        sa.Column('previous_hash', sa.String(128)),
    )

    op.create_index('idx_audit_log_entity', 'match_audit_log', ['entity_type', 'entity_id'])
    op.create_index('idx_audit_log_created', 'match_audit_log', ['created_at'])

    # ============================================================
    # isbn_sources: Track ISBN provenance from multiple sources
    # ============================================================
    op.create_table(
        'isbn_sources',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('comic_issue_id', sa.Integer(), sa.ForeignKey('comic_issues.id', ondelete='CASCADE'), nullable=False),

        sa.Column('source_name', sa.String(50), nullable=False),
        sa.Column('source_id', sa.String(100)),

        sa.Column('isbn_raw', sa.String(50)),
        sa.Column('isbn_normalized', sa.String(13)),

        sa.Column('confidence', sa.Numeric(3, 2), server_default='1.00'),
        sa.Column('fetched_at', sa.DateTime(timezone=True), server_default=sa.func.now()),

        sa.UniqueConstraint('comic_issue_id', 'source_name', name='uq_isbn_source'),
    )

    op.create_index('idx_isbn_sources_normalized', 'isbn_sources', ['isbn_normalized'], postgresql_where=sa.text("isbn_normalized IS NOT NULL"))

    # ============================================================
    # Add match tracking columns to comic_issues
    # ============================================================
    op.add_column('comic_issues', sa.Column('pricecharting_match_method', sa.String(50)))
    op.add_column('comic_issues', sa.Column('pricecharting_match_score', sa.Integer()))
    op.add_column('comic_issues', sa.Column('pricecharting_matched_at', sa.DateTime(timezone=True)))
    op.add_column('comic_issues', sa.Column('pricecharting_matched_by', sa.Integer(), sa.ForeignKey('users.id')))

    # ============================================================
    # Add match tracking columns to funkos
    # ============================================================
    op.add_column('funkos', sa.Column('pricecharting_match_method', sa.String(50)))
    op.add_column('funkos', sa.Column('pricecharting_match_score', sa.Integer()))
    op.add_column('funkos', sa.Column('pricecharting_matched_at', sa.DateTime(timezone=True)))
    op.add_column('funkos', sa.Column('pricecharting_matched_by', sa.Integer(), sa.ForeignKey('users.id')))


def downgrade():
    # Remove columns from funkos
    op.drop_column('funkos', 'pricecharting_matched_by')
    op.drop_column('funkos', 'pricecharting_matched_at')
    op.drop_column('funkos', 'pricecharting_match_score')
    op.drop_column('funkos', 'pricecharting_match_method')

    # Remove columns from comic_issues
    op.drop_column('comic_issues', 'pricecharting_matched_by')
    op.drop_column('comic_issues', 'pricecharting_matched_at')
    op.drop_column('comic_issues', 'pricecharting_match_score')
    op.drop_column('comic_issues', 'pricecharting_match_method')

    # Drop tables
    op.drop_table('isbn_sources')
    op.drop_table('match_audit_log')
    op.drop_table('match_review_queue')
