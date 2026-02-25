from sqlalchemy import (
    create_engine,
    Column,
    BigInteger,
    String,
    Boolean,
    DateTime,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import diskcache as dc
import os
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)

# Global engine and session factory
_engine = None
_SessionLocal = None

# Global cache for pending processing requests
_processing_requests_cache = None

# Base class for declarative models
Base = declarative_base()


class ProcessingRequest(Base):
    """Model for storing processing requests in the database."""

    __tablename__ = "raw"
    __table_args__ = {"schema": "to_be_processed"}

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    filename = Column(String(255), nullable=False)
    logical_date = Column(DateTime(timezone=True), nullable=False)
    processed = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)


def get_config(config):
    """Extract database configuration from config object following trigger_airflow pattern."""
    db_host = config["DB_HOST"]
    db_port = config["DB_PORT"]
    db_database = config["DB_DATABASE"]
    db_user = config["DB_USER"]
    db_password = config["DB_PASSWORD"]
    db_sslmode = config["DB_SSLMODE"]

    # Build connection string
    database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}?sslmode={db_sslmode}"

    return database_url


def get_engine(config):
    """Get or initialize the database engine."""
    global _engine
    if _engine is None:
        database_url = get_config(config)
        _engine = create_engine(database_url, echo=False)
        logger.info("Database engine initialized successfully.")
    return _engine


def get_session(config):
    """Get a new database session."""
    global _SessionLocal
    if _SessionLocal is None:
        engine = get_engine(config)
        _SessionLocal = sessionmaker(bind=engine)
    return _SessionLocal()


def create_tables(config):
    """Create database tables if they don't exist."""
    try:
        engine = get_engine(config)
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully.")
    except SQLAlchemyError as e:
        logger.error(f"Error creating database tables: {e}", exc_info=True)
        raise


def get_processing_requests_cache(config):
    """Get or initialize the diskcache instance for pending processing requests."""

    def get_config(config):
        CACHE_DIR = config["PROCESSING_REQUESTS_CACHE_DIR"]
        return CACHE_DIR

    global _processing_requests_cache
    CACHE_DIR = get_config(config)
    if _processing_requests_cache is None:
        os.makedirs(CACHE_DIR, exist_ok=True)
        _processing_requests_cache = dc.Cache(CACHE_DIR)
    return _processing_requests_cache


def create_pending_processing_request(config, pending_marker):
    """Add a pending processing request to the cache."""
    logger.info(f"Creating pending processing request for marker '{pending_marker}'")
    cache = get_processing_requests_cache(config)

    # Use marker name without extension as key
    marker_name = f"{pending_marker.split('.')[0]}.pending"
    cache[marker_name] = pending_marker

    logger.info(
        f"Pending processing request created in cache with key '{marker_name}' and value '{pending_marker}'"
    )


def get_pending_processing_requests(config):
    """Retrieve all pending processing requests from the cache."""
    logger.info("Checking for pending processing requests...")
    cache = get_processing_requests_cache(config)
    pending_markers = sorted(list(cache))
    logger.info(f"Found {len(pending_markers)} pending processing request(s).")
    return pending_markers


def remove_pending_processing_request(config, marker_name):
    """Remove a pending processing request from the cache."""
    logger.info(f"Removing pending processing request marker '{marker_name}'")
    cache = get_processing_requests_cache(config)
    if marker_name in cache:
        del cache[marker_name]
        logger.info(
            f"Pending processing request marker '{marker_name}' removed successfully."
        )
    else:
        logger.warning(
            f"Pending processing request marker '{marker_name}' not found in cache."
        )


def get_utc_logical_date_from_file(pending_marker):
    """Extract logical date from filename and convert to UTC timezone-aware datetime."""
    try:
        logger.info(f"Extracting logical date from pending_marker: {pending_marker}")

        # Remove extension(s) to get the timestamp
        # e.g., "posicoes_onibus-202602150936.json" or "posicoes_onibus-202602150936.json.zst"
        filename_without_ext = pending_marker.split(".")[0]
        timestamp = filename_without_ext.split("-")[-1]

        # Parse timestamp: YYYYMMDDHHMM format
        year = int(timestamp[0:4])
        month = int(timestamp[4:6])
        day = int(timestamp[6:8])
        hour = int(timestamp[8:10])
        minute = int(timestamp[10:12])

        # Create datetime in São Paulo timezone
        dt_obj = datetime(
            year, month, day, hour, minute, tzinfo=ZoneInfo("America/Sao_Paulo")
        )

        # Convert to UTC
        dt_utc = dt_obj.astimezone(ZoneInfo("UTC"))

        logger.info(f"Logical date extracted: {dt_utc}")
        return dt_utc
    except Exception as e:
        logger.error(
            f"Error extracting logical date from file '{pending_marker}': {e}",
            exc_info=True,
        )
        raise


def save_processing_request(config, pending_marker):
    """
    Save a processing request to the database.

    Args:
        config: Configuration dictionary
        pending_marker: Filename/marker for the processing request (e.g., 'posicoes_onibus-202602150936.json')

    Returns:
        bool: True if save was successful, False otherwise
    """
    session = None
    try:
        logger.info(f"Saving processing request for marker '{pending_marker}'")

        # Get logical date from filename
        logical_date = get_utc_logical_date_from_file(pending_marker)

        # Get current UTC time for created_at and updated_at
        now_utc = datetime.now(timezone.utc)

        # Create new processing request
        processing_request = ProcessingRequest(
            filename=pending_marker,
            logical_date=logical_date,
            processed=False,
            created_at=now_utc,
            updated_at=now_utc,
        )

        # Save to database
        session = get_session(config)
        session.add(processing_request)
        session.commit()

        logger.info(
            f"Processing request saved successfully for marker '{pending_marker}' with logical_date '{logical_date}'"
        )
        return True

    except SQLAlchemyError as e:
        if session:
            session.rollback()
        logger.error(
            f"Database error while saving processing request for marker '{pending_marker}': {e}",
            exc_info=True,
        )
        return False
    except Exception as e:
        if session:
            session.rollback()
        logger.error(
            f"Unexpected error while saving processing request for marker '{pending_marker}': {e}",
            exc_info=True,
        )
        return False
    finally:
        if session:
            session.close()


def trigger_pending_processing_requests(config):
    """
    Process all pending processing requests and save them to the database.
    Only remove from cache if the database save was successful.
    """
    pending_markers = get_pending_processing_requests(config)
    if pending_markers:
        for pending_marker_key in pending_markers:
            logger.info(f"Processing pending request: {pending_marker_key}")
            cache = get_processing_requests_cache(config)
            pending_marker_value = cache.get(pending_marker_key)

            if pending_marker_value:
                if save_processing_request(config, pending_marker_value):
                    remove_pending_processing_request(config, pending_marker_key)
                else:
                    logger.warning(
                        f"Failed to save processing request for marker '{pending_marker_value}'. Will retry on next execution."
                    )
    else:
        logger.info("No pending processing requests found.")
