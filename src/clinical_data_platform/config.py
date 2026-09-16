"""Runtime configuration for connected and generated-data execution modes.

Every environment-specific value enters through settings. Source paths are configurable because
real organizations expose different API resources, auth methods, Snowflake accounts, and storage
locations. The rest of the platform consumes this stable configuration contract rather than
embedding environment details in pipeline code.
"""
from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: str = "dev"
    demo_mode: bool = True
    log_level: str = "INFO"

    source_api_base_url: str = "https://api.example.internal"
    source_api_token: SecretStr = SecretStr("replace-me")
    source_api_health_path: str = "/health/time"
    source_product_events_path: str = "/v1/product-events"
    source_clinical_tools_path: str = "/v1/clinical-tools"
    source_content_path: str = "/v1/content"
    source_accounts_path: str = "/v1/accounts"
    source_payments_path: str = "/v1/payments"
    source_page_size: int = Field(default=500, ge=1, le=10_000)
    source_timeout_seconds: float = Field(default=30.0, gt=0, le=300)
    source_max_pages: int = Field(default=20_000, ge=1)

    snowflake_account: str = "org-account"
    snowflake_user: str = "service_user"
    snowflake_password: SecretStr = SecretStr("replace-me")
    snowflake_warehouse: str = "COMPUTE_WH"
    snowflake_database: str = "CLINICAL_ANALYTICS"
    snowflake_schema: str = "RAW"
    snowflake_role: str = "DATA_ENGINEER"
    snowflake_query_tag: str = "clinical-data-platform"

    raw_uri: str = "s3://clinical-platform-dev/raw"
    clean_uri: str = "s3://clinical-platform-dev/clean"
    quarantine_uri: str = "s3://clinical-platform-dev/quarantine"
    checkpoint_uri: str = "s3://clinical-platform-dev/checkpoints"

    stream_window_minutes: int = Field(default=10, ge=1, le=1440)
    stream_poll_seconds: int = Field(default=5, ge=1, le=300)
    demo_customer_count: int = Field(default=24, ge=1, le=10_000)
    demo_payment_failure_rate: float = Field(default=0.08, ge=0.0, le=1.0)

    @field_validator(
        "source_api_health_path",
        "source_product_events_path",
        "source_clinical_tools_path",
        "source_content_path",
        "source_accounts_path",
        "source_payments_path",
    )
    @classmethod
    def path_must_be_absolute(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("source API paths must begin with '/'")
        return value

    @property
    def mode(self) -> Literal["demo", "connected"]:
        return "demo" if self.demo_mode else "connected"

    def source_paths(self) -> dict[str, str]:
        return {
            "health": self.source_api_health_path,
            "product_events": self.source_product_events_path,
            "clinical_tools": self.source_clinical_tools_path,
            "content": self.source_content_path,
            "accounts": self.source_accounts_path,
            "payments": self.source_payments_path,
        }

    def connected_placeholders(self) -> list[str]:
        """Return values that still look like template defaults in connected mode."""
        if self.demo_mode:
            return []
        missing: list[str] = []
        if "example.internal" in self.source_api_base_url:
            missing.append("SOURCE_API_BASE_URL")
        if self.source_api_token.get_secret_value() == "replace-me":
            missing.append("SOURCE_API_TOKEN")
        if self.snowflake_account == "org-account":
            missing.append("SNOWFLAKE_ACCOUNT")
        if self.snowflake_password.get_secret_value() == "replace-me":
            missing.append("SNOWFLAKE_PASSWORD")
        return missing


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
