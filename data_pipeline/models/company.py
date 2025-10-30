from datetime import datetime, timezone
from typing import List
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import String, DateTime
from core.database import Base

import uuid


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True
    )

    name: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False, comment="The name of the company."
    )
    ticker: Mapped[str] = mapped_column(
        String(50),
        unique=True,
        nullable=False,
        comment="Stock ticker symbol (e.g., TSLA, NVDA)",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    stock_bars: Mapped[List["StockBar"]] = relationship(
        back_populates="company",
        cascade="all, delete-orphan",
    )
