"""Domain-specific exceptions with user-facing vs internal split."""

from __future__ import annotations


class GrowthOSError(Exception):
    """Base application exception."""


class UserFacingError(GrowthOSError):
    """Safe to surface directly to the AI client or user."""


class InternalError(GrowthOSError):
    """Internal failure — log but do not expose raw details to the client."""


# --- User-facing errors ---

class DatasetValidationError(UserFacingError):
    """Raised when canonical dataset validation fails."""


class QuerySandboxError(UserFacingError):
    """Raised when a query violates read-only or safety rules."""


class ConnectorError(UserFacingError):
    """Raised when a data source connector fails to initialise or sync."""


class IngestionError(UserFacingError):
    """Raised when source ingestion fails in a way the user can fix."""


class ProfileError(UserFacingError):
    """Raised for workspace profile save/load errors."""


# --- Internal errors ---

class InternalQueryError(InternalError):
    """Unexpected low-level query execution failure."""


class InternalIngestionError(InternalError):
    """Unexpected failure during data ingestion."""
