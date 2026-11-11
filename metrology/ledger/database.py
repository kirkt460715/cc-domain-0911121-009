import contextlib
import sqlite3
import threading
from pathlib import Path
from ..errors import StateError, IntegrityError

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS units(symbol TEXT PRIMARY KEY, payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS instruments(id TEXT PRIMARY KEY, payload TEXT NOT NULL, active INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS certificates(id TEXT PRIMARY KEY, instrument TEXT NOT NULL, channel TEXT NOT NULL,
 start TEXT NOT NULL, end TEXT NOT NULL, payload TEXT NOT NULL, fingerprint TEXT NOT NULL, revoked INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS certificate_channel ON certificates(instrument,channel,start,end);
CREATE TABLE IF NOT EXISTS policies(id TEXT PRIMARY KEY, payload TEXT NOT NULL, fingerprint TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS bindings(instrument TEXT NOT NULL, channel TEXT NOT NULL, policy TEXT NOT NULL,
 PRIMARY KEY(instrument,channel), FOREIGN KEY(policy) REFERENCES policies(id));
CREATE TABLE IF NOT EXISTS sources(id TEXT PRIMARY KEY, high_sequence INTEGER NOT NULL DEFAULT -1,
 watermark TEXT, allowed_lateness INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS batches(id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, payload TEXT NOT NULL,
 result TEXT NOT NULL, created TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS observations(id TEXT PRIMARY KEY, source TEXT NOT NULL, sequence INTEGER NOT NULL,
 instrument TEXT NOT NULL, channel TEXT NOT NULL, event_time TEXT NOT NULL, raw TEXT NOT NULL,
 fingerprint TEXT NOT NULL, batch TEXT NOT NULL, UNIQUE(source,sequence));
CREATE INDEX IF NOT EXISTS observation_channel ON observations(instrument,channel,event_time,id);
CREATE TABLE IF NOT EXISTS results(observation TEXT NOT NULL, revision INTEGER NOT NULL, payload TEXT,
 error TEXT, active INTEGER NOT NULL, PRIMARY KEY(observation,revision),
 FOREIGN KEY(observation) REFERENCES observations(id));
CREATE UNIQUE INDEX IF NOT EXISTS one_active_result ON results(observation) WHERE active=1;
CREATE TABLE IF NOT EXISTS quarantines(batch TEXT NOT NULL, line INTEGER NOT NULL, payload TEXT NOT NULL,
 PRIMARY KEY(batch,line));
CREATE TABLE IF NOT EXISTS replays(id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, result TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS windows(id TEXT PRIMARY KEY, specification TEXT NOT NULL, generation INTEGER NOT NULL,
 payload TEXT NOT NULL, sealed INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS audit(sequence INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL,
 object_id TEXT NOT NULL, payload TEXT NOT NULL, previous TEXT NOT NULL, hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS snapshots(id TEXT PRIMARY KEY, payload TEXT NOT NULL, fingerprint TEXT NOT NULL);
"""


class Database:
    def __init__(self, path=":memory:"):
        self.path = str(path)
        if self.path != ":memory:":
            parent = Path(path).parent
            if not parent.is_dir():
                raise StateError("database parent directory does not exist")
            if Path(path).is_symlink():
                raise StateError("database path cannot be a symlink")
        self.connection = sqlite3.connect(
            self.path, timeout=5, isolation_level=None, check_same_thread=False
        )
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA busy_timeout=5000")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.RLock()
        self._depth = 0
        self._closed = False
        self.connection.executescript(SCHEMA)
        version = self.connection.execute(
            "SELECT value FROM meta WHERE key='schema'"
        ).fetchone()
        if version is not None and version["value"] != "1":
            self.connection.close()
            raise StateError("unsupported database schema")
        self.connection.execute("INSERT OR IGNORE INTO meta VALUES('schema','1')")

    @contextlib.contextmanager
    def transaction(self):
        with self._lock:
            if self._closed:
                raise StateError("database is closed")
            depth = self._depth
            savepoint = f"nested_{depth}"
            self.connection.execute(
                "BEGIN IMMEDIATE" if depth == 0 else f"SAVEPOINT {savepoint}"
            )
            self._depth += 1
            try:
                yield self.connection
            except BaseException:
                if depth == 0:
                    self.connection.execute("ROLLBACK")
                else:
                    self.connection.execute(f"ROLLBACK TO {savepoint}")
                    self.connection.execute(f"RELEASE {savepoint}")
                raise
            else:
                self.connection.execute(
                    "COMMIT" if depth == 0 else f"RELEASE {savepoint}"
                )
            finally:
                self._depth -= 1

    def rows(self, sql, parameters=()):
        with self._lock:
            if self._closed:
                raise StateError("database is closed")
            return [
                dict(row) for row in self.connection.execute(sql, parameters).fetchall()
            ]

    def one(self, sql, parameters=()):
        rows = self.rows(sql, parameters)
        return rows[0] if rows else None

    def integrity(self):
        with self._lock:
            check = self.connection.execute("PRAGMA integrity_check").fetchall()
            foreign = self.connection.execute("PRAGMA foreign_key_check").fetchall()
            messages = [row[0] for row in check]
            if messages != ["ok"] or foreign:
                raise IntegrityError(
                    "SQLite integrity failure",
                    details={"check": messages, "foreign_keys": len(foreign)},
                )
            return {"sqlite": "ok", "foreign_keys": "ok"}

    def backup(self, destination):
        destination = Path(destination)
        if destination.exists():
            raise StateError("backup destination already exists")
        if not destination.parent.is_dir():
            raise StateError("backup directory does not exist")
        with self._lock:
            if self._depth:
                raise StateError("cannot backup during a write transaction")
            target = sqlite3.connect(destination)
            try:
                self.connection.backup(target)
                result = target.execute("PRAGMA integrity_check").fetchone()[0]
                if result != "ok":
                    raise IntegrityError("backup verification failed")
            finally:
                target.close()
        return str(destination)

    def close(self):
        with self._lock:
            if self._depth:
                raise StateError("cannot close an active transaction")
            if not self._closed:
                self.connection.close()
                self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        self.close()
