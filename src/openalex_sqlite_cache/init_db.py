import os
import sqlite3

INIT_DB_SQL_PATH = "init_db.sql"

# Schema from here: https://docs.openalex.org/download-all-data/upload-to-your-database/load-to-a-relational-database
#
# Other API docs:
# https://docs.openalex.org/api-entities/entities-overview
# https://docs.openalex.org/how-to-use-the-api/get-single-entities
def init_openalex_db(file_path: str) -> sqlite3.Connection:
    """Initialize the OpenAlex SQLite database"""
    if os.path.exists(file_path) and file_path is not ":memory:":
        os.remove(file_path)
    
    conn = sqlite3.connect(file_path)
    cursor = conn.cursor()

    # Execute the SQL commands in init_db.sql
    directory = os.path.dirname(os.path.abspath(__file__))
    init_db_sql_path = os.path.join(directory, INIT_DB_SQL_PATH)
    with open(init_db_sql_path, "r") as f:
        sql_commands = f.read()

    cursor.executescript(sql_commands)
    return conn