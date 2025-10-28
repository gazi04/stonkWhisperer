from datetime import datetime, timezone
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from typing import Optional

from core.database import Base

import uuid


class RedditPost(Base):
    __tablename__ = "reddit_posts"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True
    )

    # Reddit post information
    reddit_id: Mapped[str] = mapped_column(
        String(20),
        unique=True,
        index=True,
        comment="The unique Reddit post ID from PRAW",
    )
    subreddit: Mapped[str] = mapped_column(
        String(255), comment="The name of the subreddit where the post is made."
    )
    author: Mapped[Optional[str]] = mapped_column(
        String(255),
        default="No author",
        comment="The reddit post author (set to 'No Author' if missing).",
    )
    title: Mapped[str] = mapped_column(String(500), comment="The reddit post title.")
    body_text: Mapped[Optional[str]] = mapped_column(
        Text,
        default="No body text",
        comment="The raw, full reddit post body (for reference).",
    )
    score: Mapped[int] = mapped_column(
        Integer, comment="The net number of upvotes (upvotes - downvotes)."
    )
    number_of_comments: Mapped[int] = mapped_column(
        Integer, comment="The total number of comments on the post."
    )
    is_text_post: Mapped[bool] = mapped_column(
        Boolean,
        comment="A boolean flag that tells you if the post is a text post (`True`) or a link post (`False`).",
    )
    subreddit_category: Mapped[str] = mapped_column(
        String, comment="The category from the subreddit"
    )
    upvote_ratio: Mapped[float] = mapped_column(
        Float, comment="The percentage of total votes that are upvotes."
    )

    article_headline: Mapped[str] = mapped_column(
        String(500),
        comment="The fetched article headline, if is_text_post is false",
        nullable=True,
    )
    article_author: Mapped[str] = mapped_column(
        String(255),
        comment="The fetched article author, if is_text_post is false",
        nullable=True,
    )
    article_publisher: Mapped[str] = mapped_column(
        String(255),
        comment="The fetched article publisher, if is_text_post is false",
        nullable=True,
    )
    article_content: Mapped[str] = mapped_column(
        Text,
        comment="The fetched article content, if is_text_post is false",
        nullable=True,
    )

    article_headline_cleaned: Mapped[str] = mapped_column(
        String(500),
        comment="The cleaned article headline (lowercased, noise-removed).",
        nullable=True,
    )
    article_content_cleaned: Mapped[str] = mapped_column(
        Text,
        comment="The cleaned article content text (lowercased, noise-removed).",
        nullable=True,
    )

    # --- Source & Time Data ---
    published_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), comment="The time when the reddit post is created."
    )
    article_published_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="The fetched article published date, if is_text_post is false",
    )
    source_name: Mapped[str] = mapped_column(
        String(255), nullable=True, comment="The publisher's name (e.g., 'The Verge')."
    )
    reddit_post_url: Mapped[str] = mapped_column(
        Text, index=True, comment="The reddit post URL."
    )
    article_url: Mapped[str] = mapped_column(
        Text,
        index=True,
        nullable=True,
        comment="The URL that the post links to. If the post is a text post this is just the reddit post URL.",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
