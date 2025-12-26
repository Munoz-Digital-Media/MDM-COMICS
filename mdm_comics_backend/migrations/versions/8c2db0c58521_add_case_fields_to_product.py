"""add_case_fields_to_product

Revision ID: 8c2db0c58521
Revises: 1c50bb94b63b
Create Date: 2025-12-24 20:05:02.584961

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8c2db0c58521'
down_revision: Union[str, None] = '1c50bb94b63b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add case intelligence fields to products table
    op.add_column('products', sa.Column('case_quantity', sa.Integer(), nullable=True))
    op.add_column('products', sa.Column('case_weight', sa.Numeric(10, 2), nullable=True))
    op.add_column('products', sa.Column('case_dimensions', sa.String(length=100), nullable=True))


def downgrade() -> None:
    # Remove case intelligence fields
    op.drop_column('products', 'case_dimensions')
    op.drop_column('products', 'case_weight')
    op.drop_column('products', 'case_quantity')
