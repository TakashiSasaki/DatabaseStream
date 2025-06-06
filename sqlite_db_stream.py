import sqlite3
from db_stream import DatabaseStream

class SqliteDatabaseStream(DatabaseStream):
    """
    SQLite のみを利用する DatabaseStream のサブクラス。
    - コンストラクタで SQLite ファイルに接続。
    - 書き込みモードでは INSERT を実行。
    - 読み込みモードでは session_ts IS NULL の行をフェッチし、
      読んだ行を消費済みとして UPDATE する。
    """

    def __init__(self,
                 db_path: str,
                 table_name: str,
                 mode: str = 'r'):
        """
        Args:
            db_path (str): SQLite の .db ファイルのパス。
            table_name (str): 使用するテーブル名。
            mode (str): 'r'（stdin 相当）または 'w'（stdout/stderr 相当）。
        """
        # SQLite 接続情報を先行して設定し、親クラスを初期化する
        self._db_path = db_path
        super().__init__(table_name, mode)

    def _db_connect(self):
        """
        SQLite データベースに接続し、カーソルを用意する。
        """
        # detect_types=sqlite3.PARSE_DECLTYPES により TIMESTAMP カラムを自動で Python の datetime にパース
        self._conn = sqlite3.connect(
            self._db_path,
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        # Row ファクトリを使うことで、行は dict 風に row['id'], row['content'] で参照できる
        self._conn.row_factory = sqlite3.Row
        self._cursor = self._conn.cursor()

    def _db_close(self):
        """
        カーソルと接続を閉じる。
        """
        try:
            self._cursor.close()
        except Exception:
            pass
        try:
            self._conn.close()
        except Exception:
            pass

    def _write_record(self, content: str):
        """
        INSERT クエリで content, session_ts, hostname, pid をテーブルに保存する。
        """
        insert_sql = (
            f"INSERT INTO {self._table_name} "
            "(content, session_ts, hostname, pid) "
            "VALUES (?, ?, ?, ?)"
        )
        self._cursor.execute(
            insert_sql,
            (content, self.session_ts, self.hostname, self.pid)
        )
        # パフォーマンスを気にする場合は毎回コミットしない設計もあるが、ここではシンプルに即コミット
        self._conn.commit()

    def _flush(self):
        """
        コミットを要求された場合に実行する。閉じられていなければコミットを実行。
        """
        if not self._closed:
            self._conn.commit()

    def _fetch_unconsumed(self) -> list:
        """
        session_ts が NULL の行をすべて取得する。
        RETURN:
            List of sqlite3.Row。各 Row は 'id' と 'content' キーを持つ。
        """
        select_sql = (
            f"SELECT id, content FROM {self._table_name} "
            "WHERE session_ts IS NULL "
            "ORDER BY id"
        )
        self._cursor.execute(select_sql)
        # fetchall() でリストとして返却。Row オブジェクトには row['id'], row['content'] がある。
        return self._cursor.fetchall()

    def _mark_consumed(self, row_id: int):
        """
        指定された ID の行に対して、session_ts, hostname, pid を更新し、消費済みにする。
        """
        update_sql = (
            f"UPDATE {self._table_name} "
            "SET session_ts = ?, hostname = ?, pid = ? "
            "WHERE id = ?"
        )
        self._cursor.execute(
            update_sql,
            (self.session_ts, self.hostname, self.pid, row_id)
        )
        self._conn.commit()
