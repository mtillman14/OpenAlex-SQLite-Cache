from typing import Union, List
import sqlite3
import json

import pytest

from openalex_sqlite_cache.topic import Topic

from fixtures.test_conn import conn

@pytest.fixture
def topic_ids():
    """Fixture to provide a list of topic IDs."""
    return ["https://openalex.org/T11636"]

@pytest.fixture
def mock_topic_response_from_web_api():
    """Fixture to mock response from the OpenAlex API."""
    with open('tests/examples_from_web_API/example_topic.json', 'r') as f:
        topic_data = json.load(f)
    return topic_data

@pytest.fixture
def mock_topic_response_from_db():
    """Fixture to mock response from the database."""
    with open('tests/examples_from_db/example_topic.json', 'r') as f:
        topic_data = json.load(f)
    return topic_data

def test_1_create_topics_from_web_api_by_ids(conn: sqlite3.Connection, topic_ids: Union[List[str], str], mock_topic_response_from_web_api) -> Topic:
    """
    Query the OpenAlex web API for a particular topic(s) to create the pyalex.Topic dict. Insert the topic(s) into the database.
    """

    topics = Topic.create_topics_from_web_api_by_ids(conn, topic_ids)
    assert len(topics) > 0
    assert mock_topic_response_from_web_api == topics[0].data

    # Check if the topics were inserted into the database
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM topics")
    count = cursor.fetchone()[0]
    assert count == len(topic_ids)

def test_2_read_topics_from_db_by_ids(conn: sqlite3.Connection, topic_ids: Union[List[str], str], mock_topic_response_from_db) -> Topic:
    """
    Query the database for a particular topic to create its pyalex.Topic dict. 
    """

    topics = Topic.read_topics_from_db_by_ids(conn, topic_ids)
    assert len(topics) > 0
    assert mock_topic_response_from_db == topics[0].data

    # Check if the topics were read from the database
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM topics")
    count = cursor.fetchone()[0]
    assert count == len(topic_ids)

def test_3_delete_topics_from_db(conn: sqlite3.Connection, topic_ids: Union[List[str], str]) -> None:
    """
    Delete a particular topic(s) from the database.
    """

    topics = Topic.read_topics_from_db_by_ids(conn, topic_ids)
    assert len(topics) > 0
    cursor = conn.cursor()
    # Delete the topics from the database
    [topic.delete(conn) for topic in topics]
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM topics")
    count = cursor.fetchone()[0]
    assert count == 0

if __name__=="__main__":
    pytest.main([__file__, "-s"])