"""Removed redundant 'source_name' and 'article_url' from reddit_posts

Revision ID: 0760ed71f445
Revises: 79c98c387ae9
Create Date: 2025-10-28 17:47:49.049368

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0760ed71f445'
down_revision: Union[str, Sequence[str], None] = '79c98c387ae9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
