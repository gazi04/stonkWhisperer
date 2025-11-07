"""Implement 1-M relationship between Article and RedditPost and cleanup RedditPost columns

Revision ID: cfab4ab3c120
Revises: 0760ed71f445
Create Date: 2025-10-28 20:01:12.541676

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'cfab4ab3c120'
down_revision: Union[str, Sequence[str], None] = '0760ed71f445'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass

def downgrade() -> None:
    """Downgrade schema."""
    pass
