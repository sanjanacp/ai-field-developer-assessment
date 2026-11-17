import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";

export class SqliteRateLimiter {
  constructor(path, limit = 50_000, windowMs = 60_000) {
    mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path);
    this.db.exec(`
      PRAGMA journal_mode = WAL;
      CREATE TABLE IF NOT EXISTS token_usage (
        tenant TEXT NOT NULL,
        used_at INTEGER NOT NULL,
        tokens INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS token_usage_tenant_time ON token_usage (tenant, used_at);
    `);
    this.limit = limit;
    this.windowMs = windowMs;
  }

  consume(tenant, tokens, now = Date.now()) {
    if (!Number.isSafeInteger(tokens) || tokens < 0) throw new TypeError("tokens must be a non-negative integer");
    this.db.exec("BEGIN IMMEDIATE");
    try {
      this.db.prepare("DELETE FROM token_usage WHERE used_at <= ?").run(now - this.windowMs);
      const row = this.db.prepare("SELECT COALESCE(SUM(tokens), 0) AS total FROM token_usage WHERE tenant = ?").get(tenant);
      const used = Number(row.total);
      const allowed = used + tokens <= this.limit;
      if (allowed) this.db.prepare("INSERT INTO token_usage (tenant, used_at, tokens) VALUES (?, ?, ?)").run(tenant, now, tokens);
      this.db.exec("COMMIT");
      return { allowed, remaining: Math.max(0, this.limit - used - (allowed ? tokens : 0)) };
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  close() {
    this.db.close();
  }
}
