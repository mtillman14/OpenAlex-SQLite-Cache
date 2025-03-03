import sqlite3
from typing import Union, List
import json

import pyalex

from .entity import Entity

class Topic(Entity):

    def __init__(self, topic: Union[pyalex.Topic, dict]):
        super().__init__(topic)

    @staticmethod
    def create_topics_from_web_api_by_ids(conn: sqlite3.Connection, topic_ids: Union[List[str], str]) -> "Topic":
        """
        Query the OpenAlex web API for a particular topic(s) to create the pyalex.Topic dict. Insert the topic(s) into the database.
        The only Topics returned are those that were successfully inserted into the database.
        If a topic already exists in the database, it will not be inserted again, and will not be returned here.
        """
        if not isinstance(topic_ids, list):
            topic_ids = [topic_ids]
        pyalexTopics = []
        for topic_id in topic_ids:
            tmpTopic = pyalex.Topics()[topic_id]
            pyalexTopics.append(tmpTopic)
        assert len(pyalexTopics) == len(topic_ids)
        topics = [Topic(t) for t in pyalexTopics]
        return_topics = []
        for topic in topics:
            try:
                topic.insert_or_replace_in_db(conn)
                return_topics.append(topic)
            except sqlite3.IntegrityError as e:
                # print(f"Error inserting topic {topic.topic_id} into database: {e}")
                pass
        return return_topics

    @staticmethod
    def read_topics_from_db_by_ids(conn: sqlite3.Connection, topic_ids: Union[List[str], str]) -> "Topic":
        """
        Query the database for a particular topic to create its pyalex.Topic dict. 
        """
        if not isinstance(topic_ids, list):
            topic_ids = [topic_ids]        
        cursor = conn.cursor()
        # TOPICS
        topic_ids_tuple = tuple([Topic._remove_base_url(topic_id) for topic_id in topic_ids])
        raw_sql = "SELECT id, display_name, subfield_id, subfield_display_name, field_id, field_display_name, domain_id, domain_display_name, description, keywords, wikipedia_id, works_count, cited_by_count, updated_date FROM topics WHERE id=?"
        cursor.execute(raw_sql, topic_ids_tuple)
        result_topics = cursor.fetchall()
        topics = []
        for i in range(len(result_topics)):
            topic_dict = {}
            # Build the topic_dict
            topic_dict["id"] = Topic._prepend_base_url(result_topics[i][0])
            topic_dict["display_name"] = result_topics[i][1]
            topic_dict["subfield"] = {}
            topic_dict["subfield"]["id"] = Topic._prepend_base_url(result_topics[i][2])
            topic_dict["subfield"]["display_name"] = result_topics[i][3]
            topic_dict["field"] = {}
            topic_dict["field"]["id"] = Topic._prepend_base_url(result_topics[i][4])
            topic_dict["field"]["display_name"] = result_topics[i][5]
            topic_dict["domain"] = {}
            topic_dict["domain"]["id"] = Topic._prepend_base_url(result_topics[i][6])
            topic_dict["domain"]["display_name"] = result_topics[i][7]
            topic_dict["description"] = result_topics[i][8]
            topic_dict["keywords"] = json.loads(result_topics[i][9])
            topic_dict["ids"] = {}
            topic_dict["ids"]["wikipedia"] = result_topics[i][10]
            topic_dict["ids"]["openalex"] = topic_dict["id"]
            topic_dict["works_count"] = result_topics[i][11]
            topic_dict["cited_by_count"] = result_topics[i][12]            
            topic_dict["updated_date"] = result_topics[i][13]
            topics.append(Topic(topic_dict))  
        return topics
    
    def delete(self, conn: sqlite3.Connection):
        """
        Delete the topic from the database.
        """
        topic_id = self.id
        conn.execute("DELETE FROM topics WHERE id=?", (topic_id,))

    def insert_or_replace_in_db(self, conn: sqlite3.Connection):
        """
        Insert the topic into the database.
        """
        topic = self.data
        # TOPICS
        insert_tuple = (Topic._remove_base_url(topic['id']),topic['display_name'], Topic._remove_base_url(topic['subfield']['id']), topic['subfield']['display_name'], Topic._remove_base_url(topic['field']['id']),topic['field']['display_name'], Topic._remove_base_url(topic['domain']['id']), topic['domain']['display_name'], topic['description'], json.dumps(topic['keywords']), topic['ids']['wikipedia'], topic['works_count'], topic['cited_by_count'], topic['updated_date'])
        question_marks = ', '.join(['?'] * len(insert_tuple))
        conn.execute(
            f"REPLACE INTO topics (id, display_name, subfield_id, subfield_display_name, field_id, field_display_name, domain_id, domain_display_name, description, keywords, wikipedia_id, works_count, cited_by_count, updated_date) VALUES ({question_marks})", insert_tuple
        )

        conn.commit()