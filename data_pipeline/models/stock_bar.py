from datetime import datetime
from sqlalchemy import (
    ForeignKey,
    Integer,
    Numeric,
    DateTime,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from core.database import Base

import uuid


class StockBar(Base):
    __tablename__ = "stock_bars"

    __table_args__ = (
        UniqueConstraint("company_id", "timestamp", name="uq_symbol_timestamp"),
    )

    # Core Identifiers
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True
    )
    company_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("companies.id"),
        index=True,
        nullable=False,
        comment="Foreign key to the companies table (the symbol's primary key).",
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        index=True,
        nullable=False,
        comment="The minute or day timestamp of the bar.",
    )

    # OHLCV Data (Open, High, Low, Close, Volume)
    open_price: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    high_price: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    low_price: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    close_price: Mapped[float] = mapped_column(Numeric(12, 4), nullable=False)
    volume: Mapped[int] = mapped_column(Integer, nullable=False)

    # Additional Alpaca Data Fields
    trade_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="The number of trades that occurred during this bar interval.",
    )
    vwap: Mapped[float] = mapped_column(
        Numeric(12, 4), nullable=False, comment="Volume Weighted Average Price."
    )

    company: Mapped["Company"] = relationship(back_populates="stock_bars")
