
from typing import Any, Dict, List, Sequence
from sqlalchemy import select
from sqlalchemy.orm import Session

from models.company import Company


class CompanyService:
    @staticmethod 
    def get_names(db: Session) -> Sequence[str]:
        stmt = select(Company.name)
        result = db.execute(stmt).scalars()
        return result.all()

    @staticmethod 
    def get_tickers(db: Session) -> Sequence[str]:
        stmt = select(Company.ticker)
        result = db.execute(stmt).scalars()
        return result.all()

    @staticmethod
    def get_ids_by_tickers(db: Session, tickers: List[str]) -> Dict[str, Any]:
        stmt = select(Company.id, Company.ticker).where(
            Company.ticker.in_(tickers)
        )

        ticker_cache: Dict[str, Any] = {}

        for company_id, ticker in db.execute(stmt):
            ticker_cache[ticker] = company_id

        return ticker_cache

