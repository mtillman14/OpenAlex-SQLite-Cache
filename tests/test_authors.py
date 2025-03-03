from typing import Union, List
import sqlite3
import json

import pytest

from openalex_sqlite_cache.author import Author

from fixtures.test_conn import conn

@pytest.fixture
def author_ids():
    """Fixture to provide a list of author IDs."""
    return ["https://openalex.org/A5023888391"]

@pytest.fixture
def mock_author_response_from_web_api():
    """Fixture to mock response from the OpenAlex API."""
    with open('tests/examples_from_web_API/example_author.json', 'r') as f:
        author_data = json.load(f)
    return author_data

@pytest.fixture
def mock_author_response_from_db():
    """Fixture to mock response from the database."""
    with open('tests/examples_from_db/example_author.json', 'r') as f:
        author_data = json.load(f)
    return author_data


def test_1_create_authors_from_web_api_by_ids(conn: sqlite3.Connection, author_ids: Union[List[str], str], mock_author_response_from_web_api) -> Author:
    """
    Query the OpenAlex web API for a particular author(s) to create the pyalex.Author dict. Insert the author(s) into the database.
    """

    authors = Author.create_authors_from_web_api_by_ids(conn, author_ids)
    assert len(authors) > 0
    assert mock_author_response_from_web_api == authors[0].data

    # Check if the authors were inserted into the database
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM authors")
    count = cursor.fetchone()[0]
    assert count == len(author_ids)

def test_2_read_authors_from_db_by_ids(conn: sqlite3.Connection, author_ids: Union[List[str], str], mock_author_response_from_db) -> Author:
    """
    Query the database for a particular author to create its pyalex.Author dict. 
    """

    authors = Author.read_authors_from_db_by_ids(conn, author_ids)
    assert len(authors) > 0
    assert mock_author_response_from_db == authors[0].data

    # Check if the authors were read from the database
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM authors")
    count = cursor.fetchone()[0]
    assert count == len(author_ids)

def test_3_delete_authors_from_db(conn: sqlite3.Connection, author_ids: Union[List[str], str]) -> None:
    """
    Delete authors from the database.
    """
    authors = Author.read_authors_from_db_by_ids(conn, author_ids)
    assert len(authors) > 0
    cursor = conn.cursor()
    # Delete the authors from the database
    [author.delete(conn) for author in authors]
    cursor.execute("SELECT COUNT(*) FROM authors")
    count = cursor.fetchone()[0]
    assert count == 0

if __name__=="__main__":
    pytest.main([__file__, "-s"])