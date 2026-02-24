"""
Redis Connection Test Script
Connects to Redis (local or Docker Airflow redis service), stores/retrieves data.
Uses REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD from environment when set
(e.g. in Docker Airflow: REDIS_HOST=redis, REDIS_PORT=6379, REDIS_DB=0).

Run in Docker Airflow (connects to redis service):
  docker compose exec airflow-worker python /opt/airflow/utils/test_redis_connection.py
  docker compose exec airflow-scheduler python /opt/airflow/utils/test_redis_connection.py
"""

import os
import redis
import json
import logging
from typing import Dict, Any, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_redis_config() -> Dict[str, Any]:
    """Read Redis connection from environment (Docker Airflow) or use defaults (local)."""
    port = os.getenv("REDIS_PORT", "6379")
    return {
        "host": os.getenv("REDIS_HOST", "localhost"),
        "port": int(port) if isinstance(port, str) and port.isdigit() else 6379,
        "db": int(os.getenv("REDIS_DB", "0") or "0") or 0,
        "password": os.getenv("REDIS_PASSWORD") or None,
    }


def connect_to_redis(
    host: Optional[str] = None,
    port: Optional[int] = None,
    password: Optional[str] = None,
    db: Optional[int] = None,
) -> redis.Redis:
    """
    Connect to Redis server.
    When host/port/password/db are not provided, uses REDIS_* env vars (Docker Airflow).

    Args:
        host: Redis host (default: from REDIS_HOST or localhost)
        port: Redis port (default: from REDIS_PORT or 6379)
        password: Redis password (default: from REDIS_PASSWORD or None)
        db: Redis DB index (default: from REDIS_DB or 0)

    Returns:
        Redis client instance

    Raises:
        ConnectionError: If connection fails
    """
    cfg = get_redis_config()
    host = host if host is not None else cfg["host"]
    port = port if port is not None else cfg["port"]
    password = password if password is not None else cfg["password"]
    db = db if db is not None else cfg["db"]
    try:
        client = redis.Redis(
            host=host,
            port=port,
            password=password,
            db=db,
            decode_responses=True,  # Automatically decode responses to strings
            socket_connect_timeout=5,
            socket_timeout=5
        )
        
        # Test connection
        client.ping()
        logger.info(f"✓ Successfully connected to Redis at {host}:{port} db={db}")
        return client
    except redis.ConnectionError as e:
        logger.error(f"✗ Failed to connect to Redis: {e}")
        raise
    except Exception as e:
        logger.error(f"✗ Unexpected error connecting to Redis: {e}")
        raise


def store_dictionary(client: redis.Redis, key: str, data: Dict[str, Any]) -> None:
    """
    Store a dictionary in Redis as JSON.
    
    Args:
        client: Redis client instance
        key: Redis key to store the data under
        data: Dictionary to store
    """
    try:
        # Serialize dictionary to JSON string
        json_data = json.dumps(data)
        
        # Store in Redis
        client.set(key, json_data)
        logger.info(f"✓ Stored dictionary under key '{key}'")
        logger.info(f"  Data: {data}")
    except Exception as e:
        logger.error(f"✗ Failed to store dictionary: {e}")
        raise


def retrieve_value(client: redis.Redis, key: str) -> Dict[str, Any]:
    """
    Retrieve a value from Redis and deserialize from JSON.
    
    Args:
        client: Redis client instance
        key: Redis key to retrieve
        
    Returns:
        Deserialized dictionary
    """
    try:
        # Get value from Redis
        json_data = client.get(key)
        
        if json_data is None:
            logger.warning(f"✗ Key '{key}' not found in Redis")
            return {}
        
        # Deserialize from JSON
        data = json.loads(json_data)
        logger.info(f"✓ Retrieved value for key '{key}'")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"✗ Failed to deserialize JSON: {e}")
        raise
    except Exception as e:
        logger.error(f"✗ Failed to retrieve value: {e}")
        raise


def retrieve_all_values(client: redis.Redis, pattern: str = "*") -> Dict[str, Any]:
    """
    Retrieve all values matching a pattern from Redis.
    
    Args:
        client: Redis client instance
        pattern: Pattern to match keys (default: "*" for all keys)
        
    Returns:
        Dictionary mapping keys to their deserialized values
    """
    try:
        # Get all keys matching pattern
        keys = client.keys(pattern)
        logger.info(f"Found {len(keys)} keys matching pattern '{pattern}'")
        
        results = {}
        for key in keys:
            try:
                value = client.get(key)
                if value:
                    # Try to deserialize as JSON, fallback to string if it fails
                    try:
                        results[key] = json.loads(value)
                    except json.JSONDecodeError:
                        results[key] = value
            except Exception as e:
                logger.warning(f"Failed to retrieve value for key '{key}': {e}")
        
        return results
    except Exception as e:
        logger.error(f"✗ Failed to retrieve all values: {e}")
        raise


def main():
    """Main function to test Redis connection and operations."""
    cfg = get_redis_config()
    logger.info("=" * 60)
    logger.info("Redis Connection Test")
    logger.info("=" * 60)
    logger.info(f"Using Redis: {cfg['host']}:{cfg['port']} db={cfg['db']}")
    
    # Connect to Redis (uses env in Docker Airflow)
    try:
        client = connect_to_redis()
    except Exception as e:
        logger.error(f"Could not connect to Redis. Make sure Redis is running: {e}")
        return
    
    # Create a simple test dictionary
    # test_data = ["shirts", "pants", "shoes"]
    
    # # Store the dictionary
    # test_key = "items"
    # try:
    #     store_dictionary(client, test_key, test_data)
    # except Exception as e:
    #     logger.error(f"Failed to store dictionary: {e}")
    #     return
    
    # Retrieve the specific value
    # logger.info("\n" + "-" * 60)
    # logger.info("Retrieving specific value:")
    # logger.info("-" * 60)
    # try:
    #     retrieved_data = retrieve_value(client, test_key)
    #     logger.info(f"Retrieved data: {json.dumps(retrieved_data, indent=2)}")
    # except Exception as e:
    #     logger.error(f"Failed to retrieve value: {e}")
    #     return
    
    # Retrieve all values
    logger.info("\n" + "-" * 60)
    logger.info("Retrieving all values from Redis:")
    logger.info("-" * 60)
    try:
        all_values = retrieve_all_values(client)
        logger.info(f"\nTotal keys found: {len(all_values)}")
        for key, value in all_values.items():
            logger.info(f"\nKey: {key}")
            if isinstance(value, dict):
                logger.info(f"Value: {json.dumps(value, indent=2)}")
            else:
                logger.info(f"Value: {value}")
    except Exception as e:
        logger.error(f"Failed to retrieve all values: {e}")
        return
    
    logger.info("\n" + "=" * 60)
    logger.info("✓ Test completed successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
