import pytest

from openalex_sqlite_cache.init_db import init_openalex_db

@pytest.fixture(scope="session")
def conn():
    """Fixture to provide a SQLite in-memory database connection."""
    conn = init_openalex_db(":memory:")    
    yield conn
    conn.close()