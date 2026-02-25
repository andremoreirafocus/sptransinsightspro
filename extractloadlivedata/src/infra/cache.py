import diskcache as dc
import os
import logging

logger = logging.getLogger(__name__)

# Global cache instance
_cache = None


def get_cache(config):
    """
    Get or initialize the diskcache instance.

    Args:
        config: Configuration dictionary

    Returns:
        dc.Cache: Diskcache instance
    """

    def get_config(config):
        """Extract cache directory configuration from config object."""
        cache_dir = config["PROCESSING_REQUESTS_CACHE_DIR"]
        return cache_dir

    global _cache
    cache_dir = get_config(config)
    if _cache is None:
        os.makedirs(cache_dir, exist_ok=True)
        _cache = dc.Cache(cache_dir)
        logger.info(f"Cache initialized at {cache_dir}")
    return _cache


def add_to_cache(config, key, value):
    """
    Add an item to the cache.

    Args:
        config: Configuration dictionary
        key: Cache key
        value: Value to store
    """
    logger.info(f"Adding to cache with key '{key}'")
    cache = get_cache(config)
    cache[key] = value
    logger.info(f"Cache entry created with key '{key}' and value '{value}'")


def get_from_cache(config):
    """
    Retrieve all items from the cache.

    Args:
        config: Configuration dictionary

    Returns:
        list: Sorted list of all cache keys
    """
    logger.info("Retrieving all items from cache...")
    cache = get_cache(config)
    items = sorted(list(cache))
    logger.info(f"Found {len(items)} item(s) in cache.")
    return items


def get_cache_value(config, key):
    """
    Retrieve a specific value from the cache.

    Args:
        config: Configuration dictionary
        key: Cache key to retrieve

    Returns:
        Value associated with key, or None if not found
    """
    cache = get_cache(config)
    return cache.get(key)


def remove_from_cache(config, key):
    """
    Remove an item from the cache.

    Args:
        config: Configuration dictionary
        key: Cache key to remove
    """
    logger.info(f"Removing cache entry with key '{key}'")
    cache = get_cache(config)
    if key in cache:
        del cache[key]
        logger.info(f"Cache entry '{key}' removed successfully.")
    else:
        logger.warning(f"Cache entry '{key}' not found in cache.")
