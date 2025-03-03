from typing import Union, List
import sqlite3
import json

import pytest

from openalex_sqlite_cache.concept import Concept

from fixtures.test_conn import conn

@pytest.fixture
def concept_ids():
    """Fixture to provide a list of concept IDs."""
    return ["https://openalex.org/C71924100"]

@pytest.fixture
def mock_concept_response_from_web_api():
    """Fixture to mock response from the OpenAlex API."""
    with open('tests/examples_from_web_API/example_concept.json', 'r') as f:
        concept_data = json.load(f)
    return concept_data

@pytest.fixture
def mock_concept_response_from_db():
    """Fixture to mock response from the database."""
    with open('tests/examples_from_db/example_concept.json', 'r') as f:
        concept_data = json.load(f)
    return concept_data

def test_1_create_concepts_from_web_api_by_ids(conn: sqlite3.Connection, concept_ids: Union[List[str], str], mock_concept_response_from_web_api) -> Concept:
    """
    Query the OpenAlex web API for a particular concept(s) to create the pyalex.Concept dict. Insert the concept(s) into the database.
    """
    concepts = Concept.create_concepts_from_web_api_by_ids(conn, concept_ids)
    assert len(concepts) > 0
    assert mock_concept_response_from_web_api == concepts[0].data

    # Check if the concepts were inserted into the database
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM concepts")
    count = cursor.fetchone()[0]
    assert count == len(concept_ids)

def test_2_read_concepts_from_db_by_ids(conn: sqlite3.Connection, concept_ids: Union[List[str], str], mock_concept_response_from_db) -> Concept:
    """
    Query the database for a particular concept to create its pyalex.Concept dict. 
    """

    concepts = Concept.read_concepts_from_db_by_ids(conn, concept_ids)
    assert len(concepts) > 0
    assert mock_concept_response_from_db == concepts[0].data

    # Check if the concepts were read from the database
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM concepts")
    count = cursor.fetchone()[0]
    assert count == len(concept_ids)

def test_3_delete_concepts_from_db(conn: sqlite3.Connection, concept_ids: Union[List[str], str]) -> Concept:
    """
    Query the database for a particular concept that does not exist to create its pyalex.Concept dict. 
    """

    concepts = Concept.read_concepts_from_db_by_ids(conn, concept_ids)

    # Check if the concepts were read from the database
    [concept.delete(conn) for concept in concepts]
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM concepts")
    count = cursor.fetchone()[0]
    assert count == 0

if __name__=="__main__":
    pytest.main([__file__, "-s"])