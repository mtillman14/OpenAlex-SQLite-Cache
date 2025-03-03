import sqlite3
from typing import Union, List
import json

import pyalex

from openalex_sqlite_cache.entity import Entity

class Concept(Entity):

    def __init__(self, concept: Union[pyalex.Concept, dict]):
        super().__init__(concept)

    @staticmethod
    def create_concepts_from_web_api_by_ids(conn: sqlite3.Connection, concept_ids: Union[List[str], str]) -> "Concept":
        """
        Query the OpenAlex web API for a particular concept(s) to create the pyalex.Concept dict. Insert the concept(s) into the database.
        The only Concepts returned are those that were successfully inserted into the database.
        If a concept already exists in the database, it will not be inserted again, and will not be returned here.
        """
        if not isinstance(concept_ids, list):
            concept_ids = [concept_ids]        
        pyAlexConcepts = []
        for concept_id in concept_ids:
            tmpConcept = pyalex.Concepts()[concept_id]
            pyAlexConcepts.append(tmpConcept)
        assert len(pyAlexConcepts) == len(concept_ids)
        concepts = [Concept(c) for c in pyAlexConcepts]
        return_concepts = []
        for concept in concepts:
            try:
                concept.insert_or_replace_in_db(conn)
                return_concepts.append(concept)
            except sqlite3.IntegrityError as e:
                # print(f"Error inserting concept {concept.concept_id} into database: {e}")
                pass
        return return_concepts

    @staticmethod
    def read_concepts_from_db_by_ids(conn: sqlite3.Connection, concept_ids: Union[List[str], str]) -> "Concept":
        """
        Query the database for a particular concept to create its pyalex.Concept dict. 
        """
        if not isinstance(concept_ids, list):
            concept_ids = [concept_ids]        
        cursor = conn.cursor()
        # CONCEPTS
        raw_sql = "SELECT id, wikidata, display_name, level, description, works_count, cited_by_count, image_url, image_thumbnail_url, works_api_url, updated_date FROM concepts WHERE id=?"
        concepts_ids_tuple = tuple([Concept._remove_base_url(concept_id) for concept_id in concept_ids])
        cursor.execute(raw_sql, concepts_ids_tuple)
        result_concepts = cursor.fetchall()

        # CONCEPTS_ANCESTORS
        raw_sql = "SELECT ancestor_id, concept_id FROM concepts_ancestors WHERE concept_id=?"
        cursor.execute(raw_sql, concepts_ids_tuple)
        result_concepts_ancestors = cursor.fetchall()

        # CONCEPTS_COUNTS_BY_YEAR
        raw_sql = "SELECT concept_id, year, works_count, cited_by_count FROM concepts_counts_by_year WHERE concept_id=?"
        cursor.execute(raw_sql, concepts_ids_tuple)
        result_concepts_counts_by_year = cursor.fetchall()        

        # CONCEPTS_IDS
        raw_sql = "SELECT concept_id, openalex, wikidata, wikipedia, umls_cui, mag FROM concepts_ids WHERE concept_id=?"
        cursor.execute(raw_sql, concepts_ids_tuple)
        result_concepts_ids = cursor.fetchall()

        # CONCEPTS_RELATED_CONCEPTS
        raw_sql = "SELECT related_concept_id, concept_id, score FROM concepts_related_concepts WHERE concept_id=?"
        cursor.execute(raw_sql, concepts_ids_tuple)
        result_concepts_related_concepts = cursor.fetchall()

        assert len(result_concepts) == len(result_concepts_ids)
        concepts = []
        for i in range(len(result_concepts)):
            concept_dict = {}        
            # Build the concept_dict
            # CONCEPTS
            concept_dict["id"] = Concept._prepend_base_url(result_concepts[i][0])
            concept_dict["wikidata"] = result_concepts[i][1]
            concept_dict["display_name"] = result_concepts[i][2]
            concept_dict["level"] = result_concepts[i][3]
            concept_dict["description"] = result_concepts[i][4]
            concept_dict["works_count"] = result_concepts[i][5]
            concept_dict["cited_by_count"] = result_concepts[i][6]
            concept_dict["image_url"] = result_concepts[i][7]
            concept_dict["image_thumbnail_url"] = result_concepts[i][8]
            concept_dict["works_api_url"] = result_concepts[i][9]
            concept_dict["updated_date"] = result_concepts[i][10]

            # CONCEPTS_ANCESTORS
            concept_dict["ancestors"] = []
            for ancestor in result_concepts_ancestors:
                if ancestor[1] == result_concepts[i][0]:
                    ancestor_dict = {}
                    ancestor_dict["id"] = Concept._prepend_base_url(ancestor[0])
                    concept_dict["ancestors"].append(ancestor_dict)

            # CONCEPTS_COUNTS_BY_YEAR
            concept_dict["counts_by_year"] = []
            for j in range(len(result_concepts_counts_by_year)):
                concept_count_by_year = {
                    "year": result_concepts_counts_by_year[j][1],
                    "works_count": result_concepts_counts_by_year[j][2],
                    "cited_by_count": result_concepts_counts_by_year[j][3]
                }
                concept_dict["counts_by_year"].append(concept_count_by_year)

            # CONCEPTS_IDS
            concept_dict["ids"] = {}
            concept_dict["ids"]["openalex"] = result_concepts_ids[i][1]
            concept_dict["ids"]["wikidata"] = result_concepts_ids[i][2]
            concept_dict["ids"]["wikipedia"] = result_concepts_ids[i][3]
            concept_dict["ids"]["umls_cui"] = json.loads(result_concepts_ids[i][4])
            concept_dict["ids"]["mag"] = result_concepts_ids[i][5]

            # CONCEPTS_RELATED_CONCEPTS
            concept_dict["related_concepts"] = []
            for related_concept in result_concepts_related_concepts:
                related_concept_dict = {}
                related_concept_dict["id"] = Concept._prepend_base_url(related_concept[0])
                related_concept_dict["score"] = related_concept[2]
                concept_dict["related_concepts"].append(related_concept_dict)
            
            concepts.append(Concept(concept_dict))
        assert len(concepts) == len(concept_ids)

        return concepts
    
    def delete(self, conn: sqlite3.Connection):
        """
        Delete the concept from the database.
        """
        concept_id = self.id
        cursor = conn.cursor()
        cursor.execute("DELETE FROM concepts WHERE id=?", (concept_id,))
        cursor.execute("DELETE FROM concepts_ancestors WHERE concept_id=?", (concept_id,))
        cursor.execute("DELETE FROM concepts_counts_by_year WHERE concept_id=?", (concept_id,))
        cursor.execute("DELETE FROM concepts_ids WHERE concept_id=?", (concept_id,))      
        cursor.execute("DELETE FROM concepts_related_concepts WHERE concept_id=?", (concept_id,))  
        conn.commit() 

    def insert_or_replace_in_db(self, conn: sqlite3.Connection):
        """
        REPLACE the concept in the database.
        """
        concept = self.data
        # CONCEPTS
        insert_tuple = (Concept._remove_base_url(concept['id']), concept['wikidata'], concept['display_name'], concept['level'], concept['description'], concept['works_count'], concept['cited_by_count'], concept['image_url'], concept['image_thumbnail_url'], concept['works_api_url'], concept['updated_date'])
        question_marks = ', '.join(['?'] * len(insert_tuple))
        conn.execute(
            f"REPLACE INTO concepts (id, wikidata, display_name, level, description, works_count, cited_by_count, image_url, image_thumbnail_url, works_api_url, updated_date) VALUES ({question_marks})", insert_tuple
        )

        # CONCEPTS_ANCESTORS
        for ancestor in concept['ancestors']:
            insert_tuple = (Concept._remove_base_url(ancestor['id']), Concept._remove_base_url(concept['id']))
            question_marks = ', '.join(['?'] * len(insert_tuple))
            conn.execute(
                f"REPLACE INTO concepts_ancestors (ancestor_id, concept_id) VALUES ({question_marks})", insert_tuple
            )
        
        # CONCEPTS_COUNTS_BY_YEAR
        for year in concept['counts_by_year']:
            insert_tuple = (Concept._remove_base_url(concept['id']), year['year'], year['works_count'], year['cited_by_count'])
            question_marks = ', '.join(['?'] * len(insert_tuple))
            conn.execute(
                f"REPLACE INTO concepts_counts_by_year (concept_id, year, works_count, cited_by_count) VALUES ({question_marks})", insert_tuple
            )

        # CONCEPTS_IDS
        insert_tuple = (Concept._remove_base_url(concept['id']), concept['ids']['openalex'], concept['ids']['wikidata'], concept['ids']['wikipedia'], json.dumps(concept['ids']['umls_cui']), concept['ids']['mag'])
        question_marks = ', '.join(['?'] * len(insert_tuple))
        conn.execute(
            f"REPLACE INTO concepts_ids (concept_id, openalex, wikidata, wikipedia, umls_cui, mag) VALUES ({question_marks})", insert_tuple
        )

        # CONCEPTS_RELATED_CONCEPTS
        for related_concept in concept['related_concepts']:
            insert_tuple = (Concept._remove_base_url(related_concept['id']), Concept._remove_base_url(concept['id']), related_concept['score'])
            question_marks = ', '.join(['?'] * len(insert_tuple))
            conn.execute(
                f"REPLACE INTO concepts_related_concepts (related_concept_id, concept_id, score) VALUES ({question_marks})", insert_tuple
            )

        conn.commit()