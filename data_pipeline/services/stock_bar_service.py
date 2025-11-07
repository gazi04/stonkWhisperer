
from typing import List
from sqlalchemy.orm import Session

from models.stock_bar import StockBar


class StockBarService:
    @staticmethod
    def create_many(session: Session, stock_bars: List[StockBar]) -> None:
        session.add_all(stock_bars)

