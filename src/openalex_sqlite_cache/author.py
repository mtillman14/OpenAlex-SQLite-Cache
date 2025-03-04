import sqlite3
from typing import Union, List
import json

import pyalex

from openalex_sqlite_cache.entity import Entity

class Author(Entity):

    # Only included here for type hinting
    def __init__(self, author: Union[pyalex.Author, dict]):
        super().__init__(author)

    @staticmethod
    def create_authors_from_web_api_by_ids(conn: sqlite3.Connection, author_ids: list) -> "Author":
        """
        Query the OpenAlex web API for a particular author(s) to create the pyalex.Author dict. Insert the author(s) into the database.
        The only Authors returned are those that were successfully inserted into the database.
        If an author already exists in the database, it will not be inserted again, and will not be returned here.
        """
        if not isinstance(author_ids, list):
            author_ids = [author_ids]
        pyalexAuthors = []
        for author_id in author_ids:
            tmpAuthor = pyalex.Authors()[author_id] # I think this may be the syntax for just one author. How to do this for a list of authors at once?
            pyalexAuthors.append(tmpAuthor)
        assert len(pyalexAuthors) == len(author_ids)
        authors = [Author(a) for a in pyalexAuthors]
        return_authors = []
        for author in authors:
            try:
                author.insert_or_replace_in_db(conn)
                return_authors.append(author)
            except sqlite3.IntegrityError as e:
                # print(f"Error inserting author {author.author_id} into database: {e}")
                pass            
        return return_authors

    @staticmethod
    def read_authors_from_db_by_ids(conn: sqlite3.Connection, author_ids: Union[List[str], str]) -> "Author":
        """
        Query the database for a particular author to create its pyalex.Author dict. 
        """
        if not isinstance(author_ids, list):
            author_ids = [author_ids]        
        cursor = conn.cursor()
        # AUTHORS
        raw_sql = "SELECT id, orcid, display_name, display_name_alternatives, works_count, cited_by_count, last_known_institution, works_api_url, updated_date FROM authors WHERE id IN ({})".format(','.join('?' * len(author_ids)))
        author_ids_tuple = tuple([Author._remove_base_url(author_id) for author_id in author_ids])
        cursor.execute(raw_sql, author_ids_tuple)
        result_authors = cursor.fetchall()

        # AUTHORS_COUNTS_BY_YEAR
        raw_sql = "SELECT author_id, year, works_count, cited_by_count FROM authors_counts_by_year WHERE author_id IN ({})".format(','.join('?' * len(author_ids)))
        cursor.execute(raw_sql, author_ids_tuple)
        result_authors_counts_by_year = cursor.fetchall()

        # AUTHORS_IDS        
        raw_sql = "SELECT author_id, openalex, orcid, scopus, twitter, wikipedia, mag FROM authors_ids WHERE author_id IN ({})".format(','.join('?' * len(author_ids)))
        cursor.execute(raw_sql, author_ids_tuple)
        result_authors_ids = cursor.fetchall()

        assert len(result_authors) == len(result_authors_ids)
        authors = []
        for i in range(len(result_authors)):
            author_dict = {}        
            # Build the author_dict
            # AUTHORS
            author_dict["id"] = Author._prepend_base_url(result_authors[i][0])
            author_dict["orcid"] = result_authors[i][1]
            author_dict["display_name"] = result_authors[i][2]
            author_dict["display_name_alternatives"] = json.loads(result_authors[i][3])
            author_dict["works_count"] = result_authors[i][4]
            author_dict["cited_by_count"] = result_authors[i][5]
            author_dict["last_known_institutions"] = []
            last_known_institution = {}
            last_known_institution["id"] = Author._prepend_base_url(result_authors[i][6])
            author_dict["last_known_institutions"].append(result_authors[i][6])
            author_dict["works_api_url"] = Author._prepend_base_url(result_authors[i][7])
            author_dict["updated_date"] = result_authors[i][8]

            # AUTHORS_IDS
            author_dict["ids"] = {}
            author_dict["ids"]["openalex"] = result_authors_ids[i][1]
            author_dict["ids"]["orcid"] = result_authors_ids[i][2]
            author_dict["ids"]["scopus"] = result_authors_ids[i][3]
            author_dict["ids"]["twitter"] = result_authors_ids[i][4]
            author_dict["ids"]["wikipedia"] = result_authors_ids[i][5]
            author_dict["ids"]["mag"] = result_authors_ids[i][6]            
        
            # AUTHORS_COUNTS_BY_YEAR
            author_dict["counts_by_year"] = []
            for j in range(len(result_authors_counts_by_year)):
                author_count_by_year = {
                    "year": result_authors_counts_by_year[j][1],
                    "works_count": result_authors_counts_by_year[j][2],
                    "cited_by_count": result_authors_counts_by_year[j][3]
                }
                author_dict["counts_by_year"].append(author_count_by_year)                        

            authors.append(Author(author_dict))
        assert len(authors) == len(author_ids)

        return authors
    
    def delete(self, conn: sqlite3.Connection):
        """
        Delete the author from the database.
        """
        author_id = self.id
        conn.execute("DELETE FROM authors WHERE id=?", (author_id,))
        conn.execute("DELETE FROM authors_counts_by_year WHERE author_id=?", (author_id,))
        conn.execute("DELETE FROM authors_ids WHERE author_id=?", (author_id,))

    def insert_or_replace_in_db(self, conn: sqlite3.Connection):
        """
        REPLACE the author in the database.
        """
        author = self.data
        cursor = conn.cursor()
        # AUTHORS
        insert_tuple = (Author._remove_base_url(author['id']), author['orcid'], author['display_name'], json.dumps(author['display_name_alternatives']), author['works_count'], author['cited_by_count'], Author._remove_base_url(author['last_known_institutions'][0]["id"]), author['works_api_url'], author['updated_date'])
        question_marks = ', '.join(['?'] * len(insert_tuple))
        cursor.execute(
            f"REPLACE INTO authors (id, orcid, display_name, display_name_alternatives, works_count, cited_by_count, last_known_institution, works_api_url, updated_date) VALUES ({question_marks})", insert_tuple
        )

        # AUTHORS_COUNTS_BY_YEAR
        for count in author['counts_by_year']:
            insert_tuple = (Author._remove_base_url(author['id']), count['year'], count['works_count'], count['cited_by_count'])
            question_marks = ', '.join(['?'] * len(insert_tuple))
            cursor.execute(
                f"REPLACE INTO authors_counts_by_year (author_id, year, works_count, cited_by_count) VALUES ({question_marks})", insert_tuple
            )

        # AUTHORS_IDS
        author_ids = author['ids']
        openalex_id = author_ids['openalex']
        orcid_id = author_ids.get('orcid')
        scopus_id = author_ids.get('scopus')
        twitter_id = author_ids.get('twitter')
        wikipedia_id = author_ids.get('wikipedia')
        mag_id = author_ids.get('mag')
        insert_tuple = (Author._remove_base_url(author['id']), openalex_id, orcid_id, scopus_id, twitter_id, wikipedia_id, mag_id)
        question_marks = ', '.join(['?'] * len(insert_tuple))
        cursor.execute(
            f"REPLACE INTO authors_ids (author_id, openalex, orcid, scopus, twitter, wikipedia, mag) VALUES ({question_marks})", insert_tuple
        )

        conn.commit()