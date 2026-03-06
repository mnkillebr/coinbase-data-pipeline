"""
Pipeline state for crypto data pipeline.
Stores last processed row timestamps in Redis to avoid full-file reads.
Uses REDIS_CONFIG; fallback: read last line of CSV for last_start.
"""

import os
import subprocess
import logging
from typing import Any, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _state_key(product_id: str, granularity: str, suffix: str) -> str:
    """Redis key for pipeline state: crypto:{product}_{granularity}:{suffix}"""
    sanitized = product_id.replace("-", "_").replace("/", "_")
    return f"crypto:{sanitized}:{granularity.lower()}:{suffix}"


def get_last_start(
    redis_client: Optional[Any],
    product_id: str,
    granularity: str,
) -> Optional[int]:
    """Get last candle start timestamp from Redis. Returns None if missing or Redis unavailable."""
    if redis_client is None:
        return None
    try:
        key = _state_key(product_id, granularity, "last_start")
        raw = redis_client.get(key)
        if raw is None:
            return None
        return int(raw)
    except Exception as e:
        logger.warning(f"Redis get_last_start failed: {e}")
        return None


def set_last_start(
    redis_client: Optional[Any],
    product_id: str,
    granularity: str,
    timestamp: int,
) -> bool:
    """Set last candle start timestamp in Redis."""
    if redis_client is None:
        return False
    try:
        key = _state_key(product_id, granularity, "last_start")
        redis_client.set(key, str(timestamp))
        return True
    except Exception as e:
        logger.warning(f"Redis set_last_start failed: {e}")
        return False


def get_last_indicators_start(
    redis_client: Optional[Any],
    product_id: str,
    granularity: str,
) -> Optional[int]:
    """Get last indicators-processed start timestamp from Redis."""
    if redis_client is None:
        return None
    try:
        key = _state_key(product_id, granularity, "last_indicators_start")
        raw = redis_client.get(key)
        if raw is None:
            return None
        return int(raw)
    except Exception as e:
        logger.warning(f"Redis get_last_indicators_start failed: {e}")
        return None


def set_last_indicators_start(
    redis_client: Optional[Any],
    product_id: str,
    granularity: str,
    timestamp: int,
) -> bool:
    """Set last indicators-processed start timestamp in Redis."""
    if redis_client is None:
        return False
    try:
        key = _state_key(product_id, granularity, "last_indicators_start")
        redis_client.set(key, str(timestamp))
        return True
    except Exception as e:
        logger.warning(f"Redis set_last_indicators_start failed: {e}")
        return False


def get_last_risk_start(
    redis_client: Optional[Any],
    product_id: str,
    granularity: str,
) -> Optional[int]:
    """Get last risk-target-processed start timestamp from Redis."""
    if redis_client is None:
        return None
    try:
        key = _state_key(product_id, granularity, "last_risk_start")
        raw = redis_client.get(key)
        if raw is None:
            return None
        return int(raw)
    except Exception as e:
        logger.warning(f"Redis get_last_risk_start failed: {e}")
        return None


def set_last_risk_start(
    redis_client: Optional[Any],
    product_id: str,
    granularity: str,
    timestamp: int,
) -> bool:
    """Set last risk-target-processed start timestamp in Redis."""
    if redis_client is None:
        return False
    try:
        key = _state_key(product_id, granularity, "last_risk_start")
        redis_client.set(key, str(timestamp))
        return True
    except Exception as e:
        logger.warning(f"Redis set_last_risk_start failed: {e}")
        return False


def read_last_line_csv(filepath: str) -> Optional[int]:
    """
    Read only the last line of a CSV to get the last 'start' timestamp.
    O(1) memory: uses tail -n 1 or seek-from-end. Returns None if file missing or empty.
    """
    if not filepath or not os.path.isfile(filepath):
        return None
    try:
        result = subprocess.run(
            ["tail", "-n", "1", filepath],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None
        line = result.stdout.strip()
        # CSV: date,start,open,high,low,close,volume
        parts = line.split(",")
        if len(parts) >= 2:
            return int(parts[1].strip())
        return None
    except FileNotFoundError:
        # tail not available (e.g. Windows); fallback: read last ~512 bytes and parse last line
        try:
            with open(filepath, "rb") as f:
                f.seek(max(0, os.path.getsize(filepath) - 512))
                tail = f.read().decode("utf-8", errors="ignore")
            last_line = tail.strip().split("\n")[-1].strip()
            if not last_line:
                return None
            parts = last_line.split(",")
            if len(parts) >= 2:
                return int(parts[1].strip())
        except Exception as e:
            logger.warning(f"read_last_line_csv fallback failed for {filepath}: {e}")
        return None
    except Exception as e:
        logger.warning(f"read_last_line_csv failed for {filepath}: {e}")
        return None
