import sqlite3
from typing import Union, List
import json

import pyalex

from openalex_sqlite_cache.entity import Entity

class Institution(Entity):

    def __init__(self, institution: Union[pyalex.Institution, dict]):
        super().__init__(institution)

    @staticmethod
    def create_institutions_from_web_api_by_ids(conn: sqlite3.Connection, institution_ids: Union[List[str], str]) -> "Institution":
        """
        Query the OpenAlex web API for a particular institution(s) to create the pyalex.Institution dict. Insert the institution(s) into the database.
        """
        if not isinstance(institution_ids, list):
            institution_ids = [institution_ids]
        pyalexInstitutions = []
        for institution_id in institution_ids:
            tmpInstitution = pyalex.Institutions()[institution_id]
            pyalexInstitutions.append(tmpInstitution)
        assert len(pyalexInstitutions) == len(institution_ids)
        institutions = [Institution(i) for i in pyalexInstitutions]
        return_institutions = []
        for institution in institutions:
            try:
                institution.insert_or_replace_in_db(conn)
                return_institutions.append(institution)
            except sqlite3.IntegrityError as e:
                # print(f"Error inserting institution {institution.institution_id} into database: {e}")
                pass
        return return_institutions
    
    @staticmethod
    def read_institutions_from_db_by_ids(conn: sqlite3.Connection, institution_ids: Union[List[str], str]) -> "Institution":
        """
        Query the database for a particular institution to create its pyalex.Institution dict. 
        """
        if not isinstance(institution_ids, list):
            institution_ids = [institution_ids]        
        cursor = conn.cursor()
        # INSTITUTIONS
        raw_sql = "SELECT id, ror, display_name, country_code, type, homepage_url, image_url, image_thumbnail_url, display_name_acronyms, display_name_alternatives, works_count, cited_by_count, works_api_url, updated_date FROM institutions WHERE id IN ({})".format(','.join('?' * len(institution_ids)))
        institution_ids_tuple = tuple([Institution._remove_base_url(institution_id) for institution_id in institution_ids])
        cursor.execute(raw_sql, institution_ids_tuple)
        result_institutions = cursor.fetchall()        

        # INSTITUTIONS_ASSOCIATED_INSTITUTIONS
        raw_sql = "SELECT institution_id, associated_institution_id, relationship FROM institutions_associated_institutions WHERE institution_id IN ({})".format(','.join('?' * len(institution_ids)))
        cursor.execute(raw_sql, institution_ids_tuple)
        result_institutions_associated_institutions = cursor.fetchall()

        # INSTITUTIONS_COUNTS_BY_YEAR
        raw_sql = "SELECT institution_id, year, works_count, cited_by_count FROM institutions_counts_by_year WHERE institution_id IN ({})".format(','.join('?' * len(institution_ids)))
        cursor.execute(raw_sql, institution_ids_tuple)
        result_institutions_counts_by_year = cursor.fetchall()

        # INSTITUTIONS_GEO
        raw_sql = "SELECT institution_id, city, geonames_city_id, region, country_code, country, latitude, longitude FROM institutions_geo WHERE institution_id IN ({})".format(','.join('?' * len(institution_ids)))
        cursor.execute(raw_sql, institution_ids_tuple)
        result_institutions_geo = cursor.fetchall()

        # INSTITUTIONS_IDS
        raw_sql = "SELECT institution_id, openalex, ror, grid, wikipedia, wikidata, mag FROM institutions_ids WHERE institution_id IN ({})".format(','.join('?' * len(institution_ids)))
        cursor.execute(raw_sql, institution_ids_tuple)
        result_institutions_ids = cursor.fetchall()

        assert len(result_institutions) == len(result_institutions_ids)
        institutions = []
        for i in range(len(result_institutions)):
            institution_dict = {}        
            # Build the institution_dict
            # INSTITUTIONS
            institution_dict["id"] = Institution._prepend_base_url(result_institutions[i][0])
            institution_dict["ror"] = result_institutions[i][1]
            institution_dict["display_name"] = result_institutions[i][2]
            institution_dict["country_code"] = result_institutions[i][3]
            institution_dict["type"] = result_institutions[i][4]
            institution_dict["homepage_url"] = Institution._prepend_base_url(result_institutions[i][5])
            institution_dict["image_url"] = Institution._prepend_base_url(result_institutions[i][6])
            institution_dict["image_thumbnail_url"] = Institution._prepend_base_url(result_institutions[i][7])
            institution_dict["display_name_acronyms"] = json.loads(result_institutions[i][8])
            institution_dict["display_name_alternatives"] = json.loads(result_institutions[i][9])            
            institution_dict["works_count"] = result_institutions[i][10]
            institution_dict["cited_by_count"] = result_institutions[i][11]
            institution_dict["works_api_url"] = Institution._prepend_base_url(result_institutions[i][12])
            institution_dict["updated_date"] = result_institutions[i][13]

            # INSTITUTIONS_ASSOCIATED_INSTITUTIONS
            institution_dict["associated_institutions"] = []
            for j in range(len(result_institutions_associated_institutions)):                
                associated_institution = {}
                associated_institution["id"] = Institution._prepend_base_url(result_institutions_associated_institutions[j][1])
                associated_institution["relationship"] = result_institutions_associated_institutions[j][2]
                institution_dict["associated_institutions"].append(associated_institution)

            # INSTITUTIONS_COUNTS_BY_YEAR
            institution_dict["counts_by_year"] = []
            for j in range(len(result_institutions_counts_by_year)):
                institution_count_by_year = {
                    "year": result_institutions_counts_by_year[j][1],
                    "works_count": result_institutions_counts_by_year[j][2],
                    "cited_by_count": result_institutions_counts_by_year[j][3]
                }
                institution_dict["counts_by_year"].append(institution_count_by_year)

            # INSTITUTIONS_GEO
            institution_dict["geo"] = {
                "city": result_institutions_geo[i][1],
                "geonames_city_id": result_institutions_geo[i][2],
                "region": result_institutions_geo[i][3],
                "country_code": result_institutions_geo[i][4],
                "country": result_institutions_geo[i][5],
                "latitude": result_institutions_geo[i][6],
                "longitude": result_institutions_geo[i][7]
            }

            # INSTITUTIONS_IDS
            institution_dict["ids"] = {}
            institution_dict["ids"]["openalex"] = result_institutions_ids[i][1]
            institution_dict["ids"]["ror"] = result_institutions_ids[i][2]
            institution_dict["ids"]["grid"] = result_institutions_ids[i][3]
            institution_dict["ids"]["wikipedia"] = result_institutions_ids[i][4]
            institution_dict["ids"]["wikidata"] = result_institutions_ids[i][5]
            institution_dict["ids"]["mag"] = result_institutions_ids[i][6]

            institutions.append(Institution(institution_dict))
        assert len(institutions) == len(institution_ids)

        return institutions
    
    def delete(self, conn: sqlite3.Connection):
        """
        Delete the institution from the database.
        """
        institution_id = self.id
        conn.execute("DELETE FROM institutions WHERE id=?", (institution_id,))
        conn.execute("DELETE FROM institutions_associated_institutions WHERE institution_id=?", (institution_id,))
        conn.execute("DELETE FROM institutions_counts_by_year WHERE institution_id=?", (institution_id,))
        conn.execute("DELETE FROM institutions_geo WHERE institution_id=?", (institution_id,))
        conn.execute("DELETE FROM institutions_ids WHERE institution_id=?", (institution_id,))

    def insert_or_replace_in_db(self, conn: sqlite3.Connection):
        """
        Insert the institution into the database.
        """
        institution = self.data
        # INSTITUTIONS
        insert_tuple = (
            Institution._remove_base_url(institution['id']), 
            institution['ror'], 
            institution['display_name'], 
            institution['country_code'], 
            institution['type'], 
            institution['homepage_url'], 
            institution['image_url'], 
            institution['image_thumbnail_url'], 
            json.dumps(institution['display_name_acronyms']),
            json.dumps(institution['display_name_alternatives']),
            institution['works_count'],
            institution['cited_by_count'],
            institution['works_api_url'],
            institution['updated_date']
        )
        question_marks = ', '.join(['?'] * len(insert_tuple))
        conn.execute(
            f"REPLACE INTO institutions (id, ror, display_name, country_code, type, homepage_url, image_url, image_thumbnail_url, display_name_acronyms, display_name_alternatives, works_count, cited_by_count, works_api_url, updated_date) VALUES ({question_marks})", insert_tuple
        )

        # INSTITUTIONS_ASSOCIATED_INSTITUTIONS
        for associated_institution in institution['associated_institutions']:
            insert_tuple = (
                Institution._remove_base_url(institution['id']), 
                Institution._remove_base_url(associated_institution['id']),
                associated_institution['relationship']
            )
            conn.execute(
                "REPLACE INTO institutions_associated_institutions (institution_id, associated_institution_id, relationship) VALUES (?, ?, ?)", insert_tuple
            )

        # INSTITUTIONS_COUNTS_BY_YEAR
        for count_by_year in institution['counts_by_year']:
            insert_tuple = (
                Institution._remove_base_url(institution['id']), 
                count_by_year['year'],
                count_by_year['works_count'],
                count_by_year['cited_by_count']
            )
            conn.execute(
                "REPLACE INTO institutions_counts_by_year (institution_id, year, works_count, cited_by_count) VALUES (?, ?, ?, ?)", insert_tuple
            )

        # INSTITUTIONS_GEO
        insert_tuple = (
            Institution._remove_base_url(institution['id']), 
            institution['geo']['city'],
            institution['geo']['geonames_city_id'],
            institution['geo']['region'],
            institution['geo']['country_code'],
            institution['geo']['country'],
            institution['geo']['latitude'],
            institution['geo']['longitude']
        )
        conn.execute(
            "REPLACE INTO institutions_geo (institution_id, city, geonames_city_id, region, country_code, country, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", insert_tuple
        )

        # INSTITUTIONS_IDS
        insert_tuple = (
            Institution._remove_base_url(institution['id']), 
            institution['ids']['openalex'],
            institution['ids']['ror'],
            institution['ids']['grid'],
            institution['ids']['wikipedia'],
            institution['ids']['wikidata'],
            institution['ids']['mag']
        )
        conn.execute(
            "REPLACE INTO institutions_ids (institution_id, openalex, ror, grid, wikipedia, wikidata, mag) VALUES (?, ?, ?, ?, ?, ?, ?)", insert_tuple
        )

        conn.commit()