"""Add relationship between articles and reddit_posts entities, and removed the 'article_' columns from reddit_posts

Revision ID: 79c98c387ae9
Revises: 5e4ae4a1e6e8
Create Date: 2025-10-28 17:44:30.236028

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '79c98c387ae9'
down_revision: Union[str, Sequence[str], None] = '5e4ae4a1e6e8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
