import io
import datetime
import socket
import os

class DatabaseStream(io.TextIOBase):
    """
    An abstract file-like object that reads/writes to a database table.
    Subclasses must implement the methods marked with `raise NotImplementedError`.
    """

    def __init__(self, table_name: str, mode: str = 'r'):
        """
        Args:
            table_name (str): The name of the database table to use.
            mode (str): 'r' for read (stdin replacement), 'w' for write (stdout/stderr replacement).
        """
        # Common session info
        self._table_name = table_name
        self._mode = mode
        self._session_ts = datetime.datetime.now()
        self._hostname = socket.gethostname()
        self._pid = os.getpid()
        self._closed = False

        # 1) Connect to the database (subclass provides this)
        self._db_connect()

        # 2) If in read mode, fetch unconsumed rows and prepare an iterator
        if self.readable():
            rows = self._fetch_unconsumed()
            # Expect each row to be a dict-like object with at least 'id' and 'content'
            self._row_iterator = iter(rows)

    # ---------- Properties and status methods ----------

    @property
    def session_ts(self) -> datetime.datetime:
        """Timestamp representing this session (fixed at construction)."""
        return self._session_ts

    @property
    def hostname(self) -> str:
        """Hostname where this process is running."""
        return self._hostname

    @property
    def pid(self) -> int:
        """Process ID of this Python process."""
        return self._pid

    @property
    def closed(self) -> bool:
        """True if the stream has been closed."""
        return self._closed

    def readable(self) -> bool:
        """True if opened in read mode and not closed."""
        return (self._mode == 'r') and (not self._closed)

    def writable(self) -> bool:
        """True if opened in write mode and not closed."""
        return (self._mode == 'w') and (not self._closed)

    # ----------- Methods for write-mode (stdout/stderr) -----------

    def write(self, s: str):
        """
        Write a string `s` into the database table along with session_ts, hostname, and pid.
        Raises:
            ValueError: if the stream is closed.
            io.UnsupportedOperation: if not opened in write mode.
        """
        if self._closed:
            raise ValueError("I/O operation on closed stream.")
        if not self.writable():
            raise io.UnsupportedOperation("Stream is not writable.")
        self._write_record(s)

    def flush(self):
        """
        Flush any buffered data to the database.
        By default, delegates to subclass (_flush). Raises ValueError if closed.
        """
        if self._closed:
            raise ValueError("I/O operation on closed stream.")
        self._flush()

    # ----------- Methods for read-mode (stdin) -----------

    def read(self, size: int = -1) -> str:
        """
        Read up to `size` characters from the database for this session.
        If size < 0, consume all available rows.
        Raises:
            ValueError: if the stream is closed.
            io.UnsupportedOperation: if not opened in read mode.
        """
        if self._closed:
            raise ValueError("I/O operation on closed stream.")
        if not self.readable():
            raise io.UnsupportedOperation("Stream is not readable.")

        result_chunks = []
        total_chars = 0

        # Iterate through unconsumed rows until size is satisfied (or all if size < 0)
        for row in self._row_iterator:
            row_id = row['id']
            content = row['content']
            # Mark this row as consumed in the database
            self._mark_consumed(row_id)
            result_chunks.append(content)
            total_chars += len(content)
            # If size ≥ 0 and we've reached/exceeded that length, stop
            if size >= 0 and total_chars >= size:
                break

        return "".join(result_chunks)

    def readline(self, limit: int = -1) -> str:
        """
        Read a single “line” (one row) from the database, marking it consumed.
        If `limit` ≥ 0, truncate to at most `limit` characters.
        Raises:
            ValueError: if the stream is closed.
            io.UnsupportedOperation: if not opened in read mode.
        """
        if self._closed:
            raise ValueError("I/O operation on closed stream.")
        if not self.readable():
            raise io.UnsupportedOperation("Stream is not readable.")

        try:
            row = next(self._row_iterator)
        except StopIteration:
            return ""

        row_id = row['id']
        content = row['content']
        # Mark as consumed
        self._mark_consumed(row_id)

        if limit >= 0:
            return content[:limit]
        return content

    def readlines(self, hint: int = -1) -> list:
        """
        Read all remaining rows as a list of strings, marking each consumed.
        If `hint` ≥ 0, stop once accumulated length ≥ hint.
        Raises:
            ValueError: if the stream is closed.
            io.UnsupportedOperation: if not opened in read mode.
        """
        if self._closed:
            raise ValueError("I/O operation on closed stream.")
        if not self.readable():
            raise io.UnsupportedOperation("Stream is not readable.")

        lines = []
        total = 0
        for row in self._row_iterator:
            row_id = row['id']
            content = row['content']
            self._mark_consumed(row_id)
            lines.append(content)
            total += len(content)
            if 0 <= hint <= total:
                break

        return lines

    def __iter__(self):
        """
        Return an iterator over lines (rows). Enables: for line in db_stream: …
        """
        if not self.readable():
            raise io.UnsupportedOperation("Stream is not readable.")
        return self

    def __next__(self) -> str:
        """
        Return the next “line” (row) from the database, marking it consumed.
        Raises:
            ValueError: if the stream is closed.
            io.UnsupportedOperation: if not opened in read mode.
        """
        if self._closed:
            raise ValueError("I/O operation on closed stream.")
        if not self.readable():
            raise io.UnsupportedOperation("Stream is not readable.")

        try:
            row = next(self._row_iterator)
        except StopIteration:
            raise StopIteration

        row_id = row['id']
        content = row['content']
        self._mark_consumed(row_id)
        return content

    # ----------- Common methods for both modes -----------

    def close(self):
        """
        Close the stream and release any resources (cursor, connection) cleanly.
        After this call, any further read/write raises ValueError.
        """
        if self._closed:
            return
        self._closed = True
        # Delegate to subclass to close DB resources
        self._db_close()

    def __enter__(self):
        """
        Support for `with DatabaseStream(...) as ds: ...`.
        """
        if self._closed:
            raise ValueError("I/O operation on closed stream.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Ensure the stream is closed when exiting a with-block.
        """
        self.close()
        return False  # Propagate exceptions if any

    # ----------- Abstract methods to be implemented by subclasses -----------

    def _db_connect(self):
        """
        Connect to the database and set up any needed cursor/connection attributes.
        Subclass must implement.
        """
        raise NotImplementedError("Subclass must implement _db_connect()")

    def _db_close(self):
        """
        Close cursors and database connection.
        Subclass may override; base implementation does nothing.
        """
        pass

    def _write_record(self, content: str):
        """
        Insert a new record (content + session_ts + hostname + pid) into the table.
        Subclass must implement.
        """
        raise NotImplementedError("Subclass must implement _write_record()")

    def _fetch_unconsumed(self) -> list:
        """
        Fetch all rows that are unconsumed (e.g., session_ts IS NULL if using that convention).
        Return a list of row-objects (e.g., dict-like) containing at least 'id' and 'content'.
        Subclass must implement.
        """
        raise NotImplementedError("Subclass must implement _fetch_unconsumed()")

    def _mark_consumed(self, row_id: int):
        """
        Mark a row (by id) as consumed: update its session_ts, hostname, pid in the database.
        Subclass must implement.
        """
        raise NotImplementedError("Subclass must implement _mark_consumed()")

    def _flush(self):
        """
        Flush any buffered writes. Subclass should override if needed (e.g., commit).
        """
        pass
