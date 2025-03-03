import json
from typing import Union
from abc import abstractmethod

from pyalex.api import OpenAlexEntity

REPLACEMENTS = {
    "'": "\"", 
    "True": ''' "True" ''',
      "False": ''' "False" ''', 
      "None": ''' "None" '''}

BASE_URL = "https://openalex.org/"

class Entity:
    """Base class for OpenAlex entities."""

    def __init__(self, data: Union[OpenAlexEntity, dict]):
        self.id = Entity._remove_base_url(data["id"])
        self.data = data
        if isinstance(data, OpenAlexEntity):
            self.origin = "web_api"
        elif isinstance(data, dict):
            self.origin = "db"

    def __repr__(self):
        return f"<{self.__class__.__name__} id={self.id}> data={json.dumps(self.data)}>"
    
    @abstractmethod
    def insert_or_replace_in_db(self, conn):
        """
        Insert or replace the entity in the database.
        """
        pass

    @abstractmethod
    def delete(self, conn):
        """
        Delete the entity from the database.
        """
        pass    

    
    @staticmethod
    def _clean_string(string: str) -> str:
        """
        Cleans a string returned from the API so that it is properly formed JSON.
        """
        for old, new in REPLACEMENTS.items():
            string = string.replace(old, new)
        return string

    @staticmethod
    def _remove_base_url(string: str, base_url: str = BASE_URL) -> str:
        """
        Removes the base URL from a string.
        """
        return string.replace(base_url, "")

    @staticmethod
    def _prepend_base_url(string: str, base_url: str = BASE_URL) -> str:
        """
        Prepends the base URL to a string.
        """
        return base_url + string