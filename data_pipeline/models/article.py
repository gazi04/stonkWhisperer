from datetime import datetime, timezone
from sqlalchemy import (
    String,
    DateTime,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing import List, Optional

from core.database import Base

import uuid

class Article(Base):
    __tablename__ = "articles"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), 
        primary_key=True, 
        default=uuid.uuid4,
        index=True
    )

    # --- Content & Author ---
    author: Mapped[Optional[str]] = mapped_column(
        String(500),
        default="No Author",
        comment="The article author (set to 'No Author' if missing)."
    )
    title: Mapped[str] = mapped_column(
        String(500),
        comment="The raw, uncleaned article headline."
    )
    content: Mapped[Optional[str]] = mapped_column(
        Text,
        comment="The raw, full article body (for reference)."
    )

    # --- ML/Cleaned Data ---
    title_cleaned: Mapped[str] = mapped_column(
        String(500),
        comment="The cleaned headline (lowercased, noise-removed)."
    )
    content_cleaned: Mapped[str] = mapped_column(
        Text,
        comment="The cleaned full article text (lowercased, noise-removed)."
    )
    sentiment_strategy: Mapped[str] = mapped_column(
        String(100),
        nullable=True,
        comment="The category tag used to segment the data."
    )

    # --- Source & Time Data ---
    published_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        comment="The article's publication date (standardized to UTC)."
    )
    source_name: Mapped[str] = mapped_column(
        String(255),
        comment="The publisher's name (e.g., 'The Verge')."
    )
    url: Mapped[str] = mapped_column(
        String(2048),
        unique=True,
        index=True,
        comment="The source URL of the article."
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc)
    )

    reddit_posts: Mapped[List["RedditPost"]] = relationship(back_populates="article")
