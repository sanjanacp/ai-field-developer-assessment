import sqlite3
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.pii_redactor import PiiRedactor
from src.rate_limiter import SqliteRateLimiter


class GatewayCoreTests(unittest.TestCase):
    def test_pii_is_masked_at_every_chunk_boundary(self):
        text = "Hi José! Email jane@example.com, SSN 123-45-6789; card 4111 1111 1111 1111."
        expected = "Hi José! Email [REDACTED], SSN [REDACTED]; card [REDACTED]."
        for split in range(len(text) + 1):
            with self.subTest(split=split):
                guard = PiiRedactor()
                self.assertEqual(guard.push(text[:split]) + guard.push(text[split:]) + guard.push(final=True), expected)

    def test_redaction_is_incremental_and_pending_text_is_bounded(self):
        guard = PiiRedactor()
        self.assertEqual(guard.push("Ready now "), "Ready now ")
        self.assertEqual(guard.push("a" * 255), "[REDACTED]")
        self.assertEqual(guard.push("a" * 1000 + "@example.com! ") + guard.push(final=True), " ")

    def test_concurrent_quota_is_persistent_and_expires(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "quota.sqlite")
            limiter = SqliteRateLimiter(path)
            with ThreadPoolExecutor(max_workers=8) as pool:
                allowed = list(pool.map(lambda _: limiter.consume("tenant", 10_000), range(12)))
            self.assertEqual(sum(allowed), 5)
            limiter = SqliteRateLimiter(path)
            self.assertFalse(limiter.consume("tenant", 1))
            self.assertTrue(limiter.consume("another-tenant", 50_000))
            with sqlite3.connect(path) as db:
                expired = time.time_ns() // 1_000_000 - 60_000
                db.execute("UPDATE token_usage SET used_at = ? WHERE tenant = ?", (expired, "tenant"))
            self.assertTrue(limiter.consume("tenant", 50_000))
