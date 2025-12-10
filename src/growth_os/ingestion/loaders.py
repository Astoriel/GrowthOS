"""Source registry for CSV and PostgreSQL data sources."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from growth_os.connectors.csv import CSVConnector
from growth_os.connectors.duckdb import GrowthConnector
from growth_os.connectors.postgres import PostgresConnector

logger = logging.getLogger(__name__)


@dataclass
class SourceRegistry:
    """Tracks and loads all registered data sources into a GrowthConnector."""

    csv_dirs: list[str] = field(default_factory=list)
    postgres_urls: list[str] = field(default_factory=list)

    def register_csv(self, path: str) -> None:
        """Register a CSV directory as a data source."""
        if path and path not in self.csv_dirs:
            self.csv_dirs.append(path)

    def register_postgres(self, url: str) -> None:
        """Register a PostgreSQL URL as a data source."""
        if url and url not in self.postgres_urls:
            self.postgres_urls.append(url)

    def load_all(self, connector: GrowthConnector) -> dict[str, list[str]]:
        """Load all registered sources into the connector.

        Returns a dict with keys 'csv' and 'postgres' listing loaded items.
        """
        results: dict[str, list[str]] = {"csv": [], "postgres": []}

        for csv_dir in self.csv_dirs:
            csv_conn = CSVConnector(csv_dir)
            loaded = csv_conn.load_into(connector.db, GrowthConnector._sanitize_table_name)
            results["csv"].extend(loaded)
            for table in loaded:
                if table not in connector._tables:
                    connector._tables.append(table)

        for pg_url in self.postgres_urls:
            pg_conn = PostgresConnector(pg_url)
            if pg_conn.attach_to(connector.db):
                results["postgres"].append(pg_url[:30])

        return results

    @classmethod
    def from_settings(cls) -> "SourceRegistry":
        """Build a SourceRegistry from the current application settings."""
        from growth_os.config.settings import settings

        registry = cls()
        if settings.growth_data_dir:
            registry.register_csv(settings.growth_data_dir)
        if settings.postgres_url:
            registry.register_postgres(settings.postgres_url)
        return registry
