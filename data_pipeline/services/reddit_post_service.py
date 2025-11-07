
from typing import Dict, List, Sequence
from sqlalchemy import select
from sqlalchemy.orm import Session

from models.reddit_post import RedditPost


class RedditService:
    @staticmethod
    def create_many(session: Session, reddit_posts: List[RedditPost]) -> None:
        session.add_all(reddit_posts)

    @staticmethod
    def get_existing_posts(session: Session, reddit_ids: Dict) -> Sequence[str]:
        return (
            session.execute(
                select(RedditPost.reddit_id).where(
                    RedditPost.reddit_id.in_(reddit_ids)
                )
            )
            .scalars()
            .all()
        )

