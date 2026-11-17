import sqlite3
import time
from contextlib import closing
from pathlib import Path


class SqliteRateLimiter:
    def __init__(self, path: str, limit: int = 50_000):
        self.path = Path(path)
        self.limit = limit
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("PRAGMA journal_mode = WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS token_usage (
                    tenant TEXT NOT NULL,
                    used_at INTEGER NOT NULL,
                    tokens INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS token_usage_tenant_time ON token_usage (tenant, used_at);
                CREATE INDEX IF NOT EXISTS token_usage_time ON token_usage (used_at);
            """)

    def consume(self, tenant: str, tokens: int) -> bool:
        if tokens < 1:
            raise ValueError("Token reservation must be positive")
        if tokens > self.limit:
            return False
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            now = time.time_ns() // 1_000_000
            db.execute("DELETE FROM token_usage WHERE used_at <= ?", (now - 60_000,))
            used, = db.execute(
                "SELECT COALESCE(SUM(tokens), 0) FROM token_usage WHERE tenant = ?", (tenant,)
            ).fetchone()
            if used + tokens > self.limit:
                return False
            db.execute("INSERT INTO token_usage VALUES (?, ?, ?)", (tenant, now, tokens))
        return True
