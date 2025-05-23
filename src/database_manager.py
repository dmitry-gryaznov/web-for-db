import os
from sqlalchemy import create_engine, inspect

from sqlalchemy.orm import sessionmaker, declarative_base
from contextlib import contextmanager
Base = declarative_base()


class DatabaseManager:
    def __init__(self):
        self.DATABASE_URL = f"postgresql://{os.getenv('DATABASE_USER')}:{os.getenv('DATABASE_PASSWORD')}@{os.getenv('DATABASE_HOST')}:{os.getenv('DATABASE_PORT')}/{os.getenv('DATABASE_NAME')}"
        self.engine = create_engine(self.DATABASE_URL)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

        Base.metadata.create_all(bind=self.engine)

    @contextmanager
    def get_db(self):
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def get_table_names(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()

db_manager = DatabaseManager()

def get_db():
    with db_manager.get_db() as db:
        yield db

def get_table_names():
    return db_manager.get_table_names()

engine = db_manager.engine