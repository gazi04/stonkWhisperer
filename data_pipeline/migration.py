"""
Imported models are not used, in a traditional way, either way for SqlAlchemy they need to be imported to create the corresponding tables foreach model, for suppresing the lint error in ruff is used 'noqa'
"""

from core.database import Base, engine
from models.article import Article

def create_tables():
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("Tables created successfully.")


if __name__ == "__main__":
    create_tables()
