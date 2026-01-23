from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    """
    SQLAlchemy declarative base for all CGD ORM models.

    All mapped classes should ultimately inherit from this Base.
    """
    pass
