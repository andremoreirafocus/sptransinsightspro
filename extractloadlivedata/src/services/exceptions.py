class IngestServiceError(RuntimeError):
    """Base exception for extractloadlivedata service-layer failures."""


class PositionsDownloadError(IngestServiceError):
    """Raised when positions download fails after retries."""


class LocalIngestBufferSaveError(IngestServiceError):
    """Raised when local ingest buffer persistence fails."""


class SavePositionsToRawError(IngestServiceError):
    """Raised when raw storage persistence fails after retries."""


class IngestNotificationError(IngestServiceError):
    """Raised when ingest notification/handoff fails."""
