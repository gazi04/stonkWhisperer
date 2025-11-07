"""Increase author column on the article table to 500 characters.

Revision ID: 5e4ae4a1e6e8
Revises: 18bb12ac9627
Create Date: 2025-10-28 10:59:37.916496

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5e4ae4a1e6e8'
down_revision: Union[str, Sequence[str], None] = '18bb12ac9627'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
