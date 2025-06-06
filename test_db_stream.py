import os
import sqlite3
import pytest
from sqlite_db_stream import SqliteDatabaseStream

@pytest.fixture
def temp_db(tmp_path):
    db_file = tmp_path / "test_db_stream.db"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    # Create tables
    cursor.executescript('''
    CREATE TABLE stdin_stream (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        content     TEXT NOT NULL,
        session_ts  TIMESTAMP,
        hostname    TEXT,
        pid         INTEGER,
        created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE stdout_stream (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        content     TEXT NOT NULL,
        session_ts  TIMESTAMP NOT NULL,
        hostname    TEXT NOT NULL,
        pid         INTEGER NOT NULL,
        created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE stderr_stream (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        content     TEXT NOT NULL,
        session_ts  TIMESTAMP NOT NULL,
        hostname    TEXT NOT NULL,
        pid         INTEGER NOT NULL,
        created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    ''')
    conn.commit()
    conn.close()
    return str(db_file)


def test_write_stdout_and_stderr(temp_db):
    # Test stdout write
    db_out = SqliteDatabaseStream(temp_db, "stdout_stream", mode='w')
    db_out.write("Line1 stdout\n")
    db_out.write("Line2 stdout\n")
    db_out.flush()
    db_out.close()

    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    cursor.execute("SELECT content, session_ts, hostname, pid FROM stdout_stream ORDER BY id;")
    rows = cursor.fetchall()
    assert len(rows) == 2
    # Check content and non-null metadata
    for content, session_ts, hostname, pid in rows:
        assert content in ("Line1 stdout\n", "Line2 stdout\n")
        assert session_ts is not None
        assert hostname is not None
        assert pid is not None

    # Test stderr write
    db_err = SqliteDatabaseStream(temp_db, "stderr_stream", mode='w')
    db_err.write("Error message\n")
    db_err.flush()
    db_err.close()

    cursor.execute("SELECT content, session_ts, hostname, pid FROM stderr_stream ORDER BY id;")
    rows_err = cursor.fetchall()
    assert len(rows_err) == 1
    content, session_ts, hostname, pid = rows_err[0]
    assert content == "Error message\n"
    assert session_ts is not None
    assert hostname is not None
    assert pid is not None

    conn.close()

def test_read_stdin_marks_consumed(temp_db):
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    # Insert two unconsumed stdin entries
    cursor.execute("INSERT INTO stdin_stream (content) VALUES (?);", ("Input1\n",))
    cursor.execute("INSERT INTO stdin_stream (content) VALUES (?);", ("Input2\n",))
    conn.commit()
    conn.close()

    # Read one line
    db_in = SqliteDatabaseStream(temp_db, "stdin_stream", mode='r')
    first_line = db_in.readline()
    assert first_line == "Input1\n"
    remaining = db_in.read()
    assert remaining == "Input2\n"
    db_in.close()

    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    # After reading, both rows should have non-null session_ts, hostname, pid
    cursor.execute("SELECT session_ts, hostname, pid FROM stdin_stream;")
    rows = cursor.fetchall()
    assert len(rows) == 2
    for session_ts, hostname, pid in rows:
        assert session_ts is not None
        assert hostname is not None
        assert pid is not None
    conn.close()
