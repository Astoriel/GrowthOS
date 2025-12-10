"""DuckDB-backed connector and read-only safety."""

from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import duckdb

from growth_os.config.settings import settings
from growth_os.query.safety import SQLSandboxError, validate_sql_ast

logger = logging.getLogger(__name__)

# Re-export so existing `from growth_os.connectors.duckdb import SQLSandboxError` still works
__all__ = ["GrowthConnector", "SQLSandboxError", "get_connector", "reset_connector"]


class GrowthConnector:
    """Manages DuckDB connections with source ingestion and safe reads."""

    def __init__(self, data_dir: str | None = None, postgres_url: str | None = None, db_path: str | None = None):
        db_target = db_path or settings.db_path or ":memory:"
        self.db = duckdb.connect(db_target)
        self._tables: list[str] = []

        # TTL query result cache — disable by setting GROWTH_QUERY_CACHE_TTL=0
        self._query_cache: dict[str, tuple[list[dict], float]] = {}
        self._cache_ttl: int = int(os.environ.get("GROWTH_QUERY_CACHE_TTL", "300"))
        self._cache_max_size: int = 100

        effective_dir = data_dir or settings.growth_data_dir
        if effective_dir:
            self._ingest_directory(effective_dir)

        effective_postgres = postgres_url or settings.postgres_url
        if effective_postgres:
            self._attach_postgres(effective_postgres)

    def _ingest_directory(self, dir_path: str) -> None:
        """Auto-ingest all CSV files from a directory."""
        path = Path(dir_path)
        if not path.exists():
            logger.warning("Data directory not found: %s", dir_path)
            return

        csv_files = list(path.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found in: %s", dir_path)
            return

        for csv_file in csv_files:
            table_name = self._sanitize_table_name(csv_file.stem)
            try:
                self.db.execute(
                    f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_csv_auto('{csv_file.as_posix()}')
                    """
                )
                if table_name not in self._tables:
                    self._tables.append(table_name)
                logger.info("Ingested %s -> %s", csv_file.name, table_name)
            except Exception as exc:  # pragma: no cover - logging path
                logger.error("Failed to ingest %s: %s", csv_file.name, exc)

    def _attach_postgres(self, url: str) -> None:
        """Attach PostgreSQL as a read-only source."""
        try:
            self.db.execute("INSTALL postgres; LOAD postgres;")
            self.db.execute(f"ATTACH '{url}' AS pg (TYPE POSTGRES, READ_ONLY)")
            logger.info("PostgreSQL attached as 'pg' (read-only)")
        except Exception as exc:  # pragma: no cover - optional path
            logger.error("Failed to attach PostgreSQL: %s", exc)

    def ingest_csv(self, filepath: str, table_name: str | None = None) -> str:
        """Ingest a single CSV file."""
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {filepath}")

        name = table_name or self._sanitize_table_name(path.stem)
        self.db.execute(
            f"""
            CREATE OR REPLACE TABLE {name} AS
            SELECT * FROM read_csv_auto('{path.as_posix()}')
            """
        )
        if name not in self._tables:
            self._tables.append(name)
        logger.info("Ingested %s -> %s", path.name, name)
        return name

    def query(self, sql: str) -> list[dict]:
        """Execute a read-only query with TTL caching and type normalization."""
        self._validate_sql(sql)

        # Cache lookup
        if self._cache_ttl > 0:
            cache_key = self._cache_key(sql)
            now = time.monotonic()
            if cache_key in self._query_cache:
                cached_rows, cached_at = self._query_cache[cache_key]
                if now - cached_at < self._cache_ttl:
                    return cached_rows

        try:
            result = self.db.execute(sql)
            columns = [desc[0] for desc in result.description]
            raw_rows = result.fetchmany(settings.max_query_rows)
            rows = [
                {col: self._normalize_value(val) for col, val in zip(columns, row)}
                for row in raw_rows
            ]
        except SQLSandboxError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Query execution failed: {exc}") from exc

        # Cache store — evict oldest entry when full
        if self._cache_ttl > 0:
            if len(self._query_cache) >= self._cache_max_size:
                oldest = min(self._query_cache, key=lambda k: self._query_cache[k][1])
                del self._query_cache[oldest]
            self._query_cache[cache_key] = (rows, time.monotonic())

        return rows

    def query_raw(self, sql: str) -> tuple[list[str], list[tuple]]:
        """Execute a query and return raw tuples."""
        self._validate_sql(sql)
        result = self.db.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchmany(settings.max_query_rows)
        return columns, rows

    def invalidate_cache(self) -> None:
        """Clear the entire query result cache."""
        self._query_cache.clear()

    def get_tables(self) -> list[str]:
        """Return table names from the main schema."""
        result = self.db.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
            """
        )
        return [row[0] for row in result.fetchall()]

    def _validate_sql(self, sql: str) -> None:
        """Validate that SQL is read-only using AST analysis."""
        validate_sql_ast(sql)

    def _cache_key(self, sql: str) -> str:
        return hashlib.md5(sql.strip().encode()).hexdigest()

    @staticmethod
    def _normalize_value(v: object) -> object:
        """Normalize DuckDB result values to JSON-safe Python types.

        Converts Decimal → float, datetime/date → ISO string, UUID → str,
        bytes → utf-8 decoded string. All other types pass through unchanged.
        """
        if isinstance(v, Decimal):
            return float(v)
        if isinstance(v, datetime):
            return v.isoformat()
        if isinstance(v, date):
            return v.isoformat()
        if isinstance(v, UUID):
            return str(v)
        if isinstance(v, bytes):
            return v.decode("utf-8", errors="replace")
        return v

    @staticmethod
    def _sanitize_table_name(name: str) -> str:
        """Convert filenames into safe SQL table names."""
        sanitized = "".join(char if char.isalnum() or char == "_" else "_" for char in name.lower())
        if sanitized and sanitized[0].isdigit():
            sanitized = f"t_{sanitized}"
        return sanitized


_connector: GrowthConnector | None = None


def get_connector() -> GrowthConnector:
    """Return a singleton connector."""
    global _connector
    if _connector is None:
        _connector = GrowthConnector()
    return _connector


def reset_connector() -> None:
    """Reset the singleton connector."""
    global _connector
    _connector = None
