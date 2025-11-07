
from typing import Dict, List, Sequence

from sqlalchemy import Row, select
from sqlalchemy.orm import Session

from models.article import Article


class ArticleService:
    @staticmethod
    def create_many(session: Session, articles: List[Article]) -> None:
        session.add_all(articles)

    @staticmethod
    def get_existing_urls(session: Session, urls: Dict) -> Sequence[Article]:
        return (
            session.execute(
                select(Article).where(
                    Article.url.in_(urls)
                )
            )
            .scalars()
            .all()
        )
