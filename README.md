# Database Stream

This package provides classes for treating database tables as file-like streams.
It includes an abstract base `DatabaseStream` and a SQLite implementation
`SqliteDatabaseStream`.

## Features

- Write text to a database table, capturing session metadata.
- Read unconsumed rows from a table as if reading lines from a file.
- Simple SQLite backend for demonstration and testing.

## Usage

Install using Poetry:

```bash
poetry install
```

Import the classes:

```python
from database_stream import SqliteDatabaseStream
```

See `tests/test_db_stream.py` for example usage.
