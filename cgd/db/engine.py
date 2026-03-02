from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cgd.core.settings import settings

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_timeout=60,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
