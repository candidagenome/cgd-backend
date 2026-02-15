"""
CGD API CRUD Operations

This package contains database query modules that implement
Create, Read, Update, Delete operations for the API.

Modules:
- go_crud: Gene Ontology queries
- locus_crud: Locus/gene queries
- phenotype_crud: Phenotype queries
- search_crud: Search queries

These modules use SQLAlchemy text() for raw SQL queries or
SQLAlchemy ORM for more complex operations.
"""

from .go_crud import *  # noqa: F401, F403
from .locus_crud import *  # noqa: F401, F403
from .phenotype_crud import *  # noqa: F401, F403
from .search_crud import *  # noqa: F401, F403
