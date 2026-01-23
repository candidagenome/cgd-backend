from cgd.core.settings import settings


def schema_prefix() -> str:
    """Return 'schema.' or '' for building raw SQL statements."""
    if settings.db_schema:
        return f"{settings.db_schema}."
    return ""
