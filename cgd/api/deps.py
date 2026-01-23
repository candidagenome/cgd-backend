from __future__ import annotations

from typing import Generator
from sqlalchemy.orm import Session

# Adjust this import if your SessionLocal lives somewhere else
from cgd.db.database import SessionLocal


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
