from typing import Union, List
import sqlite3
import json

import pytest

from openalex_sqlite_cache.institution import Institution

from fixtures.test_conn import conn

@pytest.fixture
def institution_ids():
    """Fixture to provide a list of institution IDs."""
    return ["https://openalex.org/I27837315"]

@pytest.fixture
def mock_institution_response_from_web_api():
    """Fixture to mock response from the OpenAlex API."""
    with open('tests/examples_from_web_API/example_institution.json', 'r') as f:
        institution_data = json.load(f)
    return institution_data

@pytest.fixture
def mock_institution_response_from_db():
    """Fixture to mock response from the database."""
    with open('tests/examples_from_db/example_institution.json', 'r') as f:
        institution_data = json.load(f)
    return institution_data


def test_1_create_institutions_from_web_api_by_ids(conn: sqlite3.Connection, institution_ids: Union[List[str], str], mock_institution_response_from_web_api) -> Institution:
    """
    Query the OpenAlex web API for a particular institution(s) to create the pyalex.Institution dict. Insert the institution(s) into the database.
    """
    
    institutions = Institution.create_institutions_from_web_api_by_ids(conn, institution_ids)
    assert len(institutions) > 0
    assert mock_institution_response_from_web_api == institutions[0].data

    # Check if the institutions were inserted into the database
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM institutions")
    count = cursor.fetchone()[0]
    assert count == len(institution_ids)

def test_2_read_institutions_from_db_by_ids(conn: sqlite3.Connection, institution_ids: Union[List[str], str], mock_institution_response_from_db) -> Institution:
    """
    Query the database for a particular institution to create its pyalex.Institution dict. 
    """

    institutions = Institution.read_institutions_from_db_by_ids(conn, institution_ids)
    assert len(institutions) > 0
    assert mock_institution_response_from_db == institutions[0].data

    # Check if the institutions were read from the database
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM institutions")
    count = cursor.fetchone()[0]
    assert count == len(institution_ids)

def test_3_delete_institutions_from_db(conn: sqlite3.Connection, institution_ids: Union[List[str], str]) -> None:
    """
    Delete a particular institution from the database.
    """
    
    institutions = Institution.read_institutions_from_db_by_ids(conn, institution_ids)
    assert len(institutions) > 0
    cursor = conn.cursor()
    # Delete the institutions from the database
    [institution.delete(conn) for institution in institutions]    
    cursor.execute("SELECT COUNT(*) FROM institutions")
    count = cursor.fetchone()[0]
    assert count == 0

if __name__=="__main__":
    pytest.main([__file__, "-s"])