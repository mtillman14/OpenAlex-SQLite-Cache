from typing import Union, List
import sqlite3
import json

import pytest

from openalex_sqlite_cache.publisher import Publisher

from fixtures.test_conn import conn

@pytest.fixture
def publisher_ids():
    """Fixture to provide a list of publisher IDs."""
    return ["https://openalex.org/P4310319965"]

@pytest.fixture
def mock_publisher_response_from_web_api():
    """Fixture to mock response from the OpenAlex API."""
    with open('tests/examples_from_web_API/example_publisher.json', 'r') as f:
        publisher_data = json.load(f)
    return publisher_data

@pytest.fixture
def mock_publisher_response_from_db():
    """Fixture to mock response from the database."""
    with open('tests/examples_from_db/example_publisher.json', 'r') as f:
        publisher_data = json.load(f)
    return publisher_data

def test_1_create_publishers_from_web_api_by_ids(conn: sqlite3.Connection, publisher_ids: Union[List[str], str], mock_publisher_response_from_web_api) -> Publisher:
    """
    Query the OpenAlex web API for a particular publisher(s) to create the pyalex.Publisher dict. Insert the publisher(s) into the database.
    """

    publishers = Publisher.create_publishers_from_web_api_by_ids(conn, publisher_ids)
    assert len(publishers) > 0
    assert mock_publisher_response_from_web_api == publishers[0].data

    # Check if the publishers were inserted into the database
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM publishers")
    count = cursor.fetchone()[0]
    assert count == len(publisher_ids)

def test_2_read_publishers_from_db_by_ids(conn: sqlite3.Connection, publisher_ids: Union[List[str], str], mock_publisher_response_from_db) -> Publisher:
    """
    Query the database for a particular publisher to create its pyalex.Publisher dict. 
    """

    publishers = Publisher.read_publishers_from_db_by_ids(conn, publisher_ids)
    assert len(publishers) > 0
    assert mock_publisher_response_from_db == publishers[0].data

    # Check if the publishers were read from the database
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM publishers")
    count = cursor.fetchone()[0]
    assert count == len(publisher_ids)

def test_3_delete_publishers_from_db(conn: sqlite3.Connection, publisher_ids: Union[List[str], str]) -> None:
    """
    Delete a publisher from the database.
    """

    publishers = Publisher.read_publishers_from_db_by_ids(conn, publisher_ids)
    assert len(publishers) > 0
    cursor = conn.cursor()
    # Delete the publishers from the database
    [publisher.delete(conn) for publisher in publishers]
    cursor.execute("SELECT COUNT(*) FROM publishers")
    count = cursor.fetchone()[0]
    assert count == 0

if __name__=="__main__":
    pytest.main([__file__, "-s"])