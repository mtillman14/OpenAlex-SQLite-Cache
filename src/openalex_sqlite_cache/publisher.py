import sqlite3
from typing import Union, List
import json

import pyalex

from openalex_sqlite_cache.entity import Entity

class Publisher(Entity):

    def __init__(self, publisher: Union[pyalex.Publisher, dict]):
        super().__init__(publisher)

    @staticmethod
    def create_publishers_from_web_api_by_ids(conn: sqlite3.Connection, publisher_ids: Union[List[str], str]) -> "Publisher":
        """
        Query the OpenAlex web API for a particular publisher(s) to create the pyalex.Publisher dict. Insert the publisher(s) into the database.
        The only Publishers returned are those that were successfully inserted into the database.
        If a publisher already exists in the database, it will not be inserted again, and will not be returned here.
        """
        if not isinstance(publisher_ids, list):
            publisher_ids = [publisher_ids]
        pyalexPublishers = []
        for publisher_id in publisher_ids:
            tmpPublisher = pyalex.Publishers()[publisher_id]
            pyalexPublishers.append(tmpPublisher)
        assert len(pyalexPublishers) == len(publisher_ids)
        publishers = [Publisher(p) for p in pyalexPublishers]
        return_publishers = []
        for publisher in publishers:
            try:
                publisher.insert_or_replace_in_db(conn)
                return_publishers.append(publisher)
            except sqlite3.IntegrityError as e:
                # print(f"Error inserting publisher {publisher.publisher_id} into database: {e}")
                pass
        return return_publishers
    
    @staticmethod
    def read_publishers_from_db_by_ids(conn: sqlite3.Connection, publisher_ids: Union[List[str], str]) -> "Publisher":
        """
        Query the database for a particular publisher to create its pyalex.Publisher dict. 
        """
        if not isinstance(publisher_ids, list):
            publisher_ids = [publisher_ids]
        cursor = conn.cursor()
        # PUBLISHERS
        raw_sql = "SELECT id, display_name, alternate_titles, country_codes, hierarchy_level, parent_publisher, works_count, cited_by_count, sources_api_url, updated_date FROM publishers WHERE id IN ({})".format(','.join('?' * len(publisher_ids)))
        publisher_ids_tuple = tuple([Publisher._remove_base_url(publisher_id) for publisher_id in publisher_ids])
        cursor.execute(raw_sql, publisher_ids_tuple)
        result_publishers = cursor.fetchall()
        # PUBLISHERS_COUNTS_BY_YEAR
        raw_sql = "SELECT publisher_id, year, works_count, cited_by_count FROM publishers_counts_by_year WHERE publisher_id IN ({})".format(','.join('?' * len(publisher_ids)))
        cursor.execute(raw_sql, publisher_ids_tuple)
        result_publishers_counts_by_year = cursor.fetchall()
        # PUBLISHERS_IDS
        raw_sql = "SELECT publisher_id, openalex, ror, wikidata FROM publishers_ids WHERE publisher_id IN ({})".format(','.join('?' * len(publisher_ids)))
        cursor.execute(raw_sql, publisher_ids_tuple)
        result_publishers_ids = cursor.fetchall()        

        assert len(result_publishers) == len(publisher_ids)
        publishers = []
        for i in range(len(result_publishers)):
            publisher_dict = {}
            # Build the publisher_dict
            # PUBLISHERS
            publisher_dict['id'] = Publisher._prepend_base_url(result_publishers[i][0])        
            publisher_dict['display_name'] = result_publishers[i][1]
            publisher_dict['alternate_titles'] = json.loads(result_publishers[i][2])
            publisher_dict['country_codes'] = json.loads(result_publishers[i][3])
            publisher_dict['hierarchy_level'] = result_publishers[i][4]
            publisher_dict['parent_publisher'] = result_publishers[i][5]
            publisher_dict['works_count'] = result_publishers[i][6]
            publisher_dict['cited_by_count'] = result_publishers[i][7]
            publisher_dict['sources_api_url'] = result_publishers[i][8]
            publisher_dict['updated_date'] = result_publishers[i][9]

            # PUBLISHERS_COUNTS_BY_YEAR
            publisher_dict['counts_by_year'] = []
            for count in result_publishers_counts_by_year:
                year_dict = {}
                year_dict['year'] = count[1]
                year_dict['works_count'] = count[2]
                year_dict['cited_by_count'] = count[3]
                publisher_dict['counts_by_year'].append(year_dict)

            # PUBLISHERS_IDS
            publisher_dict['ids'] = {}
            publisher_dict['ids']['openalex'] = result_publishers_ids[i][1]
            publisher_dict['ids']['ror'] = result_publishers_ids[i][2]
            publisher_dict['ids']['wikidata'] = result_publishers_ids[i][3]

            publishers.append(Publisher(publisher_dict))
        assert len(publishers) == len(publisher_ids)

        return publishers
    
    def delete(self, conn: sqlite3.Connection):
        """
        Delete the publisher from the database.
        """
        publisher_id = self.id
        conn.execute("DELETE FROM publishers WHERE id=?", (publisher_id,))
        conn.execute("DELETE FROM publishers_counts_by_year WHERE publisher_id=?", (publisher_id,))           
        conn.execute("DELETE FROM publishers_ids WHERE publisher_id=?", (publisher_id,))              

    def insert_or_replace_in_db(self, conn: sqlite3.Connection):
        """
        Insert the publisher into the database.
        """
        publisher = self.data
        cursor = conn.cursor()
        # PUBLISHERS
        insert_tuple = (Publisher._remove_base_url(publisher['id']), publisher['display_name'], json.dumps(publisher['alternate_titles']), json.dumps(publisher['country_codes']), publisher['hierarchy_level'], publisher['parent_publisher'], publisher['works_count'], publisher['cited_by_count'], publisher['sources_api_url'], publisher['updated_date'])
        question_marks = ', '.join(['?'] * len(insert_tuple))
        cursor.execute(
            f"REPLACE INTO publishers (id, display_name, alternate_titles, country_codes, hierarchy_level, parent_publisher, works_count, cited_by_count, sources_api_url, updated_date) VALUES ({question_marks})", insert_tuple
        )        

        # PUBLISHERS COUNTS BY YEAR
        for count in publisher['counts_by_year']:
            insert_tuple = (Publisher._remove_base_url(publisher['id']), count['year'], count['works_count'], count['cited_by_count'])
            question_marks = ', '.join(['?'] * len(insert_tuple))
            cursor.execute(
                f"REPLACE INTO publishers_counts_by_year (publisher_id, year, works_count, cited_by_count) VALUES ({question_marks})", insert_tuple
            )

        # PUBLISHERS IDS
        insert_tuple = (Publisher._remove_base_url(publisher['id']), publisher['ids']['openalex'], publisher['ids']['ror'], publisher['ids']['wikidata'])
        question_marks = ', '.join(['?'] * len(insert_tuple))
        cursor.execute(
            f"REPLACE INTO publishers_ids (publisher_id, openalex, ror, wikidata) VALUES ({question_marks})", insert_tuple
        )
        
        conn.commit()