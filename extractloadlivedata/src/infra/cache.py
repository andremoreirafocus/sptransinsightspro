import diskcache as dc
import os
import logging

logger = logging.getLogger(__name__)

# Global cache instance
_cache = None


def get_cache(cache_dir, cache_factory=None):
    """
    Get or initialize the diskcache instance.

    Args:
        cache_dir: Cache directory path

    Returns:
        dc.Cache: Diskcache instance
    """
    global _cache
    cache_factory = cache_factory or dc.Cache
    if _cache is None:
        os.makedirs(cache_dir, exist_ok=True)
        _cache = cache_factory(cache_dir)
        logger.info(f"Cache initialized at {cache_dir}")
    return _cache


def add_to_cache(cache_dir, key, value, cache_factory=None):
    """
    Add an item to the cache.

    Args:
        cache_dir: Cache directory path
        key: Cache key
        value: Value to store
    """
    logger.info(f"Adding to cache with key '{key}'")
    cache = get_cache(cache_dir, cache_factory=cache_factory)
    cache[key] = value
    logger.info(f"Cache entry created with key '{key}' and value '{value}'")


def get_from_cache(cache_dir, cache_factory=None):
    """
    Retrieve all items from the cache.

    Args:
        cache_dir: Cache directory path

    Returns:
        list: Sorted list of all cache keys
    """
    logger.info("Retrieving all items from cache...")
    cache = get_cache(cache_dir, cache_factory=cache_factory)
    items = sorted(list(cache))
    logger.info(f"Found {len(items)} item(s) in cache.")
    return items


def get_cache_value(cache_dir, key, cache_factory=None):
    """
    Retrieve a specific value from the cache.

    Args:
        cache_dir: Cache directory path
        key: Cache key to retrieve

    Returns:
        Value associated with key, or None if not found
    """
    cache = get_cache(cache_dir, cache_factory=cache_factory)
    return cache.get(key)


def remove_from_cache(cache_dir, key, cache_factory=None):
    """
    Remove an item from the cache.

    Args:
        cache_dir: Cache directory path
        key: Cache key to remove
    """
    logger.info(f"Removing cache entry with key '{key}'")
    cache = get_cache(cache_dir, cache_factory=cache_factory)
    if key in cache:
        del cache[key]
        logger.info(f"Cache entry '{key}' removed successfully.")
    else:
        logger.warning(f"Cache entry '{key}' not found in cache.")
