# OpenAlex-SQLite-Cache
A SQLite cache for OpenAlex API

## Installation

```bash
pip install openalex-sqlite-cache
```
## Usage

```python
from openalex_sqlite_cache import OpenAlexCache

# Create a cache object (creates a new SQLite database if it doesn't exist)
db_file = "openalex_cache.db"
cache = OpenAlexCache(db_file)

# Cache (insert/create) a record pulled from the OpenAlex web API
record_url = "https://api.openalex.org/works/10.1038/s41586-020-2649-2"
work = cache.create_works_from_web_api_by_ids(record_url)

# Get all of the record ID's of a certain type
work_ids = cache.get_all_ids("works")

# Get a record by ID from the SQLite database
work = cache.read_works_from_db_by_ids(work_ids[0])
```

## Implementation
Querying the OpenAlex web API returns a wealth of metadata for each record. [The official implementation of a SQL database](https://docs.openalex.org/download-all-data/upload-to-your-database/load-to-a-relational-database) for OpenAlex records abridges this metadata, keeping only certain fields. 

As such, querying the SQLite database will return only those subsets of metadata. The SQLite database is intended to be a cache for the OpenAlex web API, and currently it is not intended to be a complete replacement for the web API. 

Please open an issue or start a discussion if you'd like to see this feature in the future.