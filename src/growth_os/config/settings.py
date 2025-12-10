"""Typed application settings."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    growth_data_dir: str = ""
    postgres_url: str = ""
    stripe_api_key: str = ""
    stripe_base_url: str = "https://api.stripe.com/v1"
    meta_access_token: str = ""
    meta_ad_account_id: str = ""
    meta_api_version: str = "v21.0"
    meta_base_url: str = "https://graph.facebook.com"
    google_ads_developer_token: str = ""
    google_ads_customer_id: str = ""
    google_ads_login_customer_id: str = ""
    google_ads_access_token: str = ""
    google_ads_refresh_token: str = ""
    google_ads_client_id: str = ""
    google_ads_client_secret: str = ""
    google_ads_api_version: str = "v19"
    google_ads_base_url: str = "https://googleads.googleapis.com"
    google_ads_oauth_token_url: str = "https://oauth2.googleapis.com/token"
    hubspot_access_token: str = ""
    hubspot_base_url: str = "https://api.hubapi.com"
    mixpanel_api_secret: str = ""
    mixpanel_project_id: str = ""
    mixpanel_eu: bool = True
    amplitude_api_key: str = ""
    amplitude_secret_key: str = ""
    amplitude_eu: bool = True
    attribution_mapping_file: str = ""
    attribution_mapping_history_path: str = ""
    semantic_profile_path: str = ""
    server_name: str = "growth-os"
    max_query_rows: int = 10_000
    query_timeout_seconds: int = 30
    db_path: str = ""
    business_mode: bool = False
    default_transport: str = "stdio"
    sample_data_dir: str = str(Path(__file__).resolve().parents[1] / "data" / "sample")

    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False)


settings = Settings()
