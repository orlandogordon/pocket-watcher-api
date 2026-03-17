import os
import redis
from src.logging_config import get_logger

logger = get_logger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

_redis_client: redis.Redis | None = None


def get_redis_client() -> redis.Redis:
    """Get or create the Redis client singleton."""
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,
            socket_connect_timeout=5,
        )
        _redis_client.ping()
        logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    return _redis_client


def get_redis_dependency() -> redis.Redis:
    """FastAPI dependency for Redis client."""
    from fastapi import HTTPException
    try:
        return get_redis_client()
    except Exception:
        global _redis_client
        _redis_client = None
        raise HTTPException(status_code=503, detail="Redis unavailable. Preview flow requires Redis.")
