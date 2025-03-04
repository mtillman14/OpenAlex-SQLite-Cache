import sqlite3
from typing import Union, List
import json

import pyalex

from openalex_sqlite_cache.entity import Entity

class Funder(Entity):

    # Only included here for type hinting
    def __init__(self, funder: Union[pyalex.Funder, dict]):
        super().__init__(funder)

    @staticmethod
    def create_funders_from_web_api_by_ids(conn: sqlite3.Connection, funder_ids: Union[List[str], str]) -> "Funder":
        """
        Query the OpenAlex web API for a particular funder(s) to create the pyalex.Funder dict. Insert the funder(s) into the database.
        The only Funders returned are those that were successfully inserted into the database.
        If a funder already exists in the database, it will not be inserted again, and will not be returned here.
        """
        if not isinstance(funder_ids, list):
            funder_ids = [funder_ids]
        pyalexFunders = []
        for funder_id in funder_ids:
            tmpFunder = pyalex.Funders()[funder_id] # I think this may be the syntax for just one funder. How to do this for a list of funders at once?
            pyalexFunders.append(tmpFunder)
        assert len(pyalexFunders) == len(funder_ids)
        funders = [Funder(f) for f in pyalexFunders]
        return_funders = []
        for funder in funders:
            try:
                funder.insert_or_replace_in_db(conn)
                return_funders.append(funder)
            except sqlite3.IntegrityError as e:
                # print(f"Error inserting funder {funder.funder_id} into database: {e}")
                pass            
        return return_funders
    
    @staticmethod
    def read_funders_from_db_by_ids(conn: sqlite3.Connection, funder_ids: Union[List[str], str]) -> "Funder":
        """
        Query the database for a particular funder to create its pyalex.Funder dict. 
        """
        if not isinstance(funder_ids, list):
            funder_ids = [funder_ids]
        cursor = conn.cursor()
        # FUNDERS
        raw_sql = "SELECT id, display_name, alternate_names, country_codes, types, works_count, cited_by_count, sources_api_url, updated_date FROM funders WHERE id IN ({})".format(','.join('?' * len(funder_ids)))
        funder_ids_tuple = tuple([Funder._remove_base_url(funder_id) for funder_id in funder_ids])
        cursor.execute(raw_sql, funder_ids_tuple)
        result_funders = cursor.fetchall()

        # FUNDERS_COUNTS_BY_YEAR
        raw_sql = "SELECT funder_id, year, works_count, cited_by_count FROM funders_counts_by_year WHERE funder_id IN ({})".format(','.join('?' * len(funder_ids)))
        cursor.execute(raw_sql, funder_ids_tuple)
        result_funders_counts_by_year = cursor.fetchall()

        # FUNDERS_IDS
        raw_sql = "SELECT funder_id, openalex FROM funders_ids WHERE funder_id IN ({})".format(','.join('?' * len(funder_ids)))
        cursor.execute(raw_sql, funder_ids_tuple)
        result_funders_ids = cursor.fetchall()       

        assert len(result_funders) == len(funder_ids)
        funders = []
        for i in range(len(result_funders)):
            funder_dict = {}
            # Build the funder_dict
            # FUNDERS
            funder_dict['id'] = Funder._prepend_base_url(result_funders[i][0])
            funder_dict['display_name'] = result_funders[i][1]
            funder_dict['alternate_names'] = result_funders[i][2]
            funder_dict['country_codes'] = json.loads(result_funders[i][3])
            funder_dict['types'] = json.loads(result_funders[i][4])
            funder_dict['works_count'] = result_funders[i][5]
            funder_dict['cited_by_count'] = result_funders[i][6]
            funder_dict['sources_api_url'] = result_funders[i][7]
            funder_dict['updated_date'] = result_funders[i][8]

            # FUNDERS_COUNTS_BY_YEAR
            funder_dict['counts_by_year'] = []
            for count in result_funders_counts_by_year:
                year_dict = {}
                year_dict['year'] = count[1]
                year_dict['works_count'] = count[2]
                year_dict['cited_by_count'] = count[3]
                funder_dict['counts_by_year'].append(year_dict)

            # FUNDERS_IDS
            funder_dict['ids'] = {}
            funder_dict['ids']['openalex'] = result_funders_ids[i][1]
            
            funders.append(Funder(funder_dict))
        assert len(funders) == len(funder_ids)

        return funders
    
    def delete(self, conn: sqlite3.Connection):
        """
        Delete funders from the database.
        """
        funder_id = self.id
        cursor = conn.cursor()
        # Delete the funder from the database
        cursor.execute("DELETE FROM funders WHERE id=?", (funder_id,))
        cursor.execute("DELETE FROM funders_counts_by_year WHERE funder_id=?", (funder_id,))
        cursor.execute("DELETE FROM funders_ids WHERE funder_id=?", (funder_id,))
        conn.commit()

    def insert_or_replace_in_db(self, conn: sqlite3.Connection):
        """
        REPLACE the funder into the database.
        """
        funder = self.data
        cursor = conn.cursor()
        # FUNDERS
        insert_tuple = (Funder._remove_base_url(funder['id']), funder['display_name'], json.dumps(funder['alternate_names']), json.dumps(funder['country_codes']), json.dumps(funder['types']), funder['works_count'], funder['cited_by_count'], funder['sources_api_url'], funder['updated_date'])
        question_marks = ', '.join(['?'] * len(insert_tuple))
        cursor.execute(
            f"REPLACE INTO funders (id, display_name, alternate_names, country_codes, types, works_count, cited_by_count, sources_api_url, updated_date) VALUES ({question_marks})", insert_tuple
        )

        # FUNDERS_COUNTS_BY_YEAR
        for count in funder['counts_by_year']:
            insert_tuple = (Funder._remove_base_url(funder['id']), count['year'], count['works_count'], count['cited_by_count'])
            question_marks = ', '.join(['?'] * len(insert_tuple))
            cursor.execute(
                f"REPLACE INTO funders_counts_by_year (funder_id, year, works_count, cited_by_count) VALUES ({question_marks})", insert_tuple
            )

        # FUNDERS_IDS
        insert_tuple = (Funder._remove_base_url(funder['id']), funder['ids']['openalex'])
        question_marks = ', '.join(['?'] * len(insert_tuple))
        cursor.execute(
            f"REPLACE INTO funders_ids (funder_id, openalex) VALUES ({question_marks})", insert_tuple
        )
        
        conn.commit()