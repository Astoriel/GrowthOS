"""CSV file connector for DuckDB ingestion."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    import duckdb as _duckdb

logger = logging.getLogger(__name__)


class CSVConnector:
    """Handles loading CSV files from a directory into DuckDB."""

    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir

    @property
    def configured(self) -> bool:
        """Return True if the data directory is set and exists."""
        return bool(self.data_dir)

    def load_into(
        self,
        conn: _duckdb.DuckDBPyConnection,
        sanitize: Callable[[str], str],
    ) -> list[str]:
        """Load all CSV files from data_dir into the DuckDB connection.

        Returns the list of table names that were successfully loaded.
        """
        loaded: list[str] = []
        path = Path(self.data_dir)

        if not path.exists():
            logger.warning("CSV data directory not found: %s", self.data_dir)
            return loaded

        csv_files = list(path.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found in: %s", self.data_dir)
            return loaded

        for csv_file in csv_files:
            table_name = sanitize(csv_file.stem)
            try:
                conn.execute(
                    f"""
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT * FROM read_csv_auto('{csv_file.as_posix()}')
                    """
                )
                loaded.append(table_name)
                logger.info("CSVConnector: loaded %s -> %s", csv_file.name, table_name)
            except Exception as exc:
                logger.error("CSVConnector: failed to load %s: %s", csv_file.name, exc)

        return loaded
