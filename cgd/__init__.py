"""
CGD (Candida Genome Database) Backend

This is the FastAPI backend for the Candida Genome Database, providing
REST API endpoints for genomic data access, sequence analysis tools,
and curation operations.

Packages:
- api: FastAPI routers, services, and CRUD operations
- core: Configuration and shared utilities
- db: Database engine and session management
- models: SQLAlchemy ORM models
- schemas: Pydantic schemas for request/response validation
- cli: Command-line interface tools

Usage:
    # Run the API server
    uvicorn cgd.main:app --reload --port 8000

    # Import models
    from cgd.models import Base

Environment Variables:
    DATABASE_URL: Database connection URL
    DB_SCHEMA: Database schema name (default: MULTI)
    LOG_LEVEL: Logging level (default: INFO)
"""

__version__ = "0.1.0"
