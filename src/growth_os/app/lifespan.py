"""Application startup helpers."""

from __future__ import annotations

from pathlib import Path

from growth_os.config.settings import settings
from growth_os.demo.sample_generator import generate_all_sample_data


def ensure_sample_data() -> None:
    """Generate sample data when no source is configured."""
    if settings.growth_data_dir or settings.postgres_url:
        return

    sample_dir = settings.sample_data_dir
    sample_path = Path(sample_dir)
    sample_path.mkdir(parents=True, exist_ok=True)
    generate_all_sample_data(sample_dir)
    settings.growth_data_dir = sample_dir
