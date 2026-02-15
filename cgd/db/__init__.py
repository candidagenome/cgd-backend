"""
CGD Database Package

This package provides database connection and session management
using SQLAlchemy.

Modules:
- engine: Database engine configuration and SessionLocal factory
- deps: Database session dependency for FastAPI

Usage:
    from cgd.db.engine import SessionLocal

    with SessionLocal() as session:
        # perform database operations
        pass

Environment Variables:
    DATABASE_URL: SQLAlchemy database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
"""
