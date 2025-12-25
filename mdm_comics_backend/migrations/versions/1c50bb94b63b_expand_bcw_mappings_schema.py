"""expand_bcw_mappings_schema

Revision ID: 1c50bb94b63b
Revises: 20251213_match_review
Create Date: 2025-12-24 19:13:28.142119

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '1c50bb94b63b'
down_revision: Union[str, None] = '20251213_match_review'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add new columns to bcw_product_mappings
    op.add_column('bcw_product_mappings', sa.Column('url', sa.String(length=500), nullable=True))
    op.add_column('bcw_product_mappings', sa.Column('upc', sa.String(length=50), nullable=True))
    op.add_column('bcw_product_mappings', sa.Column('case_quantity', sa.Integer(), nullable=True))
    op.add_column('bcw_product_mappings', sa.Column('weight', sa.Numeric(10, 2), nullable=True))
    op.add_column('bcw_product_mappings', sa.Column('dimensions', sa.String(length=100), nullable=True))
    op.add_column('bcw_product_mappings', sa.Column('description', sa.Text(), nullable=True))
    op.add_column('bcw_product_mappings', sa.Column('images', sa.JSON(), nullable=True))

    # Add indexes for frequent lookups
    op.create_index(op.f('ix_bcw_mappings_upc'), 'bcw_product_mappings', ['upc'], unique=False)


def downgrade() -> None:
    # Remove indexes
    op.drop_index(op.f('ix_bcw_mappings_upc'), table_name='bcw_product_mappings')

    # Drop columns
    op.drop_column('bcw_product_mappings', 'images')
    op.drop_column('bcw_product_mappings', 'description')
    op.drop_column('bcw_product_mappings', 'dimensions')
    op.drop_column('bcw_product_mappings', 'weight')
    op.drop_column('bcw_product_mappings', 'case_quantity')
    op.drop_column('bcw_product_mappings', 'upc')
    op.drop_column('bcw_product_mappings', 'url')
