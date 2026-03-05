"""
Redis Storage Module for Crypto Insights
Extracts high-level insights from processed crypto data and stores them in Redis.
Uses the same Redis configuration as test_redis_connection / REDIS_CONFIG
(REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD from environment in Docker Airflow).
"""

import json
import pandas as pd
import logging
from typing import Dict, Optional, Any
from datetime import datetime
import pytz
import redis
from redis.exceptions import ConnectionError, TimeoutError, RedisError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_redis_client() -> Optional[redis.Redis]:
    """
    Initialize and return a Redis client using the same config as the pipeline
    (REDIS_CONFIG: host, port, db, password from environment / crypto_pipeline_config).

    Returns:
        Redis client instance or None if connection fails
    """
    try:
        from configs.crypto_pipeline_config import REDIS_CONFIG
    except Exception as e:
        logger.warning(f"Could not load REDIS_CONFIG from config, using env fallback: {e}")
        import os
        REDIS_CONFIG = {
            "host": os.getenv("REDIS_HOST", "localhost"),
            "port": int(os.getenv("REDIS_PORT", "6379") or "6379"),
            "db": int(os.getenv("REDIS_DB", "0") or "0"),
            "password": os.getenv("REDIS_PASSWORD") or None,
        }

    try:
        client = redis.Redis(
            host=REDIS_CONFIG["host"],
            port=REDIS_CONFIG["port"],
            db=REDIS_CONFIG["db"],
            password=REDIS_CONFIG.get("password"),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )
        client.ping()
        logger.info(
            f"Successfully connected to Redis at {REDIS_CONFIG['host']}:{REDIS_CONFIG['port']} db={REDIS_CONFIG['db']}"
        )
        return client
    except (ConnectionError, TimeoutError, RedisError) as e:
        logger.error(f"Failed to connect to Redis: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error connecting to Redis: {e}")
        return None


def extract_insights(df: pd.DataFrame, product_id: str, granularity: str) -> Dict[str, Any]:
    """
    Extract high-level insights from processed DataFrame.
    
    Args:
        df: DataFrame with processed crypto data (from risk_target_task)
        product_id: Product identifier (e.g., BTC-USD)
        granularity: Time granularity (e.g., ONE_DAY)
        
    Returns:
        Dictionary containing extracted insights
    """
    logger.info(f"Extracting insights for {product_id} {granularity} from {len(df)} records")
    
    # Ensure DataFrame is sorted by timestamp
    if 'timestamp' in df.columns:
        df = df.sort_values('timestamp').reset_index(drop=True)
    elif 'start' in df.columns:
        df = df.sort_values('start').reset_index(drop=True)
    elif 'date' in df.columns:
        df = df.sort_values('date').reset_index(drop=True)
    
    # Get the latest row (most recent data)
    latest = df.iloc[-1]
    print("Latest row: ", latest.to_dict())
    
    # Get previous row for comparison (for inflection points and crosses)
    prev = df.iloc[-2] if len(df) > 1 else None
    
    # Calculate summary statistics for recent periods
    # Determine period based on granularity
    if granularity == "FIFTEEN_MINUTE":
        periods_24h = 96  # 24 hours / 15 minutes
        periods_7d = 672  # 7 days / 15 minutes
    elif granularity == "ONE_HOUR":
        periods_24h = 24
        periods_7d = 168
    elif granularity == "FOUR_HOUR":
        periods_24h = 6
        periods_7d = 42
    elif granularity == "ONE_DAY":
        periods_24h = 1
        periods_7d = 7
    else:
        periods_24h = 24
        periods_7d = 168
    
    # Get recent data for statistics
    recent_24h = df.tail(min(periods_24h, len(df)))
    recent_7d = df.tail(min(periods_7d, len(df)))
    
    # 1. Close price
    close_val = latest.get('close', None)
    close_price = float(close_val) if close_val is not None and pd.notna(close_val) else None
    
    # 2. Risk level mapping
    risk_score_raw = latest.get('risk_score', None)
    risk_score = int(risk_score_raw) if risk_score_raw is not None and pd.notna(risk_score_raw) else None
    if risk_score is None:
        risk_level = None
    elif risk_score == 0:
        risk_level = "very low risk"
    elif risk_score == 1:
        risk_level = "low risk"
    else:
        risk_level = "high risk - avoid"
    
    # 3. RSI over/under condition
    rsi_val = latest.get('rsi', None)
    rsi = float(rsi_val) if rsi_val is not None and pd.notna(rsi_val) else None
    rsi_over_condition = False
    if rsi is not None:
        if rsi >= 80:
            rsi_over_condition = "very overbought"
        elif rsi >= 70:
            rsi_over_condition = "overbought"
        elif rsi <= 20:
            rsi_over_condition = "very oversold"
        elif rsi <= 30:
            rsi_over_condition = "oversold"
    
    # 4. Stochastic RSI condition
    stoch_rsi_k_val = latest.get('es_stoch_rsi_k', None)
    stoch_rsi_k = float(stoch_rsi_k_val) if stoch_rsi_k_val is not None and pd.notna(stoch_rsi_k_val) else None
    stoch_rsi_over_condition = False
    if stoch_rsi_k is not None:
        if stoch_rsi_k >= 80:
            stoch_rsi_over_condition = "overbought"
        elif stoch_rsi_k <= 20:
            stoch_rsi_over_condition = "oversold"
    
    # 5. Close price relative to kijun_v2_200_1
    kijun_val = latest.get('kijun_v2_200_1', None)
    kijun = float(kijun_val) if kijun_val is not None and pd.notna(kijun_val) else None
    close_vs_kijun = None
    if close_price is not None and kijun is not None:
        if close_price > kijun:
            close_vs_kijun = "above"
        elif close_price < kijun:
            close_vs_kijun = "below"
        else:
            close_vs_kijun = "equal"
    
    # 6. SMA_50 inflection point detection
    sma_50_current_val = latest.get('sma_50', None)
    sma_50_current = float(sma_50_current_val) if sma_50_current_val is not None and pd.notna(sma_50_current_val) else None
    sma_50_prev_val = prev.get('sma_50', None) if prev is not None else None
    sma_50_prev = float(sma_50_prev_val) if sma_50_prev_val is not None and pd.notna(sma_50_prev_val) else None
    sma_50_inflection = None
    if sma_50_current is not None and sma_50_prev is not None:
        if sma_50_current > sma_50_prev:
            sma_50_inflection = "bullish"  # Moving up
        elif sma_50_current < sma_50_prev:
            sma_50_inflection = "bearish"  # Moving down
        # If equal, no inflection (keep previous value)
    
    # 7. SMA_138 inflection point detection
    sma_138_current_val = latest.get('sma_138', None)
    sma_138_current = float(sma_138_current_val) if sma_138_current_val is not None and pd.notna(sma_138_current_val) else None
    sma_138_prev_val = prev.get('sma_138', None) if prev is not None else None
    sma_138_prev = float(sma_138_prev_val) if sma_138_prev_val is not None and pd.notna(sma_138_prev_val) else None
    sma_138_inflection = None
    if sma_138_current is not None and sma_138_prev is not None:
        if sma_138_current > sma_138_prev:
            sma_138_inflection = "bullish"  # Moving up
        elif sma_138_current < sma_138_prev:
            sma_138_inflection = "bearish"  # Moving down
        # If equal, no inflection (keep previous value)
    
    # 8. SMA_14 cross above/below SMA_50
    sma_14_current_val = latest.get('sma_14', None)
    sma_14_current = float(sma_14_current_val) if sma_14_current_val is not None and pd.notna(sma_14_current_val) else None
    sma_14_prev_val = prev.get('sma_14', None) if prev is not None else None
    sma_14_prev = float(sma_14_prev_val) if sma_14_prev_val is not None and pd.notna(sma_14_prev_val) else None
    sma_50_prev_val = prev.get('sma_50', None) if prev is not None else None
    sma_50_prev = float(sma_50_prev_val) if sma_50_prev_val is not None and pd.notna(sma_50_prev_val) else None
    sma_cross = None
    if sma_14_current is not None and sma_50_current is not None and sma_14_prev is not None and sma_50_prev is not None:
        # Check for cross
        if sma_14_prev <= sma_50_prev and sma_14_current > sma_50_current:
            sma_cross = "crossed above"  # Golden cross
        elif sma_14_prev >= sma_50_prev and sma_14_current < sma_50_current:
            sma_cross = "crossed below"  # Death cross

    # 9. Trending SMA cross above/below Support/Resistance Average
    sma_50_current_val = latest.get('sma_50', None)
    sma_50_current = float(sma_50_current_val) if sma_50_current_val is not None and pd.notna(sma_50_current_val) else None
    sma_138_prev_val = prev.get('sma_138', None) if prev is not None else None
    sma_138_prev = float(sma_138_prev_val) if sma_138_prev_val is not None and pd.notna(sma_138_prev_val) else None
    trending_cross = None
    if sma_50_current is not None and sma_138_prev is not None and sma_50_prev is not None:
        # Check for cross
        if sma_50_prev <= sma_138_prev and sma_50_current > sma_138_current:
            trending_cross = "crossed above"  # Golden cross
        elif sma_50_prev >= sma_138_prev and sma_50_current < sma_138_current:
            trending_cross = "crossed below"  # Death cross
    
    # 10. Standard deviation between close price and SMA_138
    close_std_vs_sma138 = None
    if close_price is not None and sma_138_current is not None:
        # Calculate standard deviation of (close - sma_138) over recent period
        if 'sma_138' in df.columns and 'close' in df.columns:
            # Get recent data (last 20 periods or available data)
            recent_data = df.tail(min(20, len(df)))
            if len(recent_data) > 1:
                diff = recent_data['close'] - recent_data['sma_138']
                close_std_vs_sma138 = float(diff.std()) if pd.notna(diff.std()) else None
    
    # 11. ATR and volume
    atr_val = latest.get('atr', None)
    atr = float(atr_val) if atr_val is not None and pd.notna(atr_val) else None
    volume_val = latest.get('volume', None)
    volume = float(volume_val) if volume_val is not None and pd.notna(volume_val) else None
    
    # 12. Summary statistics (24h and 7d)
    stats_24h = {
        "avg_price": float(recent_24h['close'].mean()) if len(recent_24h) > 0 else None,
        "avg_volume": float(recent_24h['volume'].mean()) if len(recent_24h) > 0 else None,
        "volatility": float(recent_24h['close'].std()) if len(recent_24h) > 0 else None,
        "price_change_pct": float((recent_24h['close'].iloc[-1] - recent_24h['close'].iloc[0]) / recent_24h['close'].iloc[0] * 100) if len(recent_24h) > 1 else None,
        "avg_rsi": float(recent_24h['rsi'].mean()) if 'rsi' in recent_24h.columns and len(recent_24h) > 0 else None,
        "avg_risk_score": float(recent_24h['risk_score'].mean()) if 'risk_score' in recent_24h.columns and len(recent_24h) > 0 else None,
    }
    
    stats_7d = {
        "avg_price": float(recent_7d['close'].mean()) if len(recent_7d) > 0 else None,
        "avg_volume": float(recent_7d['volume'].mean()) if len(recent_7d) > 0 else None,
        "volatility": float(recent_7d['close'].std()) if len(recent_7d) > 0 else None,
        "price_change_pct": float((recent_7d['close'].iloc[-1] - recent_7d['close'].iloc[0]) / recent_7d['close'].iloc[0] * 100) if len(recent_7d) > 1 else None,
        # "avg_rsi": float(recent_7d['rsi'].mean()) if 'rsi' in recent_7d.columns and len(recent_7d) > 0 else None,
        # "avg_risk_score": float(recent_7d['risk_score'].mean()) if 'risk_score' in recent_7d.columns and len(recent_7d) > 0 else None,
    }
    
    # Build simplified insights dictionary
    insights = {
        "product_id": product_id,
        "granularity": granularity,
        "date": latest.get('date') or latest.get('start') or latest.get('timestamp'),
        "last_updated": datetime.now(pytz.timezone('US/Eastern')).isoformat(),
        
        # 1. Close price
        "close_price": close_price,
        
        # 2. Risk level
        "risk_level": risk_level,
        
        # 3. RSI over/under condition
        "rsi_over_condition": rsi_over_condition,
        
        # 4. Stochastic RSI condition
        "stoch_rsi_over_condition": stoch_rsi_over_condition,
        
        # 5. Close vs Kijun
        "close_vs_kijun": close_vs_kijun,
        
        # 6. SMA_50 inflection
        "trending_sma_50_inflection": sma_50_inflection,
        
        # 7. SMA_138 inflection
        "support_resistance_sma_inflection": sma_138_inflection,
        
        # 8. SMA_14 cross
        "trading_trending_sma_cross": sma_cross,
        
        # 9. Trending SMA cross above/below Support/Resistance Average
        "trending_support_resistance_cross": trending_cross,
        
        # 10. Standard deviation between close and SMA_138
        "close_std_vs_sma138": close_std_vs_sma138,
        
        # 10. ATR and volume
        "atr": atr,
        "volume": volume,
        
        # 11. Summary statistics
        "stats_24h": stats_24h,
        "stats_7d": stats_7d,
    }
    
    logger.info(f"Extracted insights for {product_id} {granularity}")
    return insights


def get_insights_latest_key(product_id: str, granularity: str) -> str:
    """
    Return the Redis key used for the latest insights snapshot.
    Matches the key structure used in store_insights_to_redis.

    Args:
        product_id: Product identifier (e.g., BTC-USD)
        granularity: Time granularity (e.g., ONE_DAY)

    Returns:
        Redis key string, e.g. crypto:BTC_USD:one_day:latest
    """
    sanitized_product = product_id.replace("-", "_").replace("/", "_")
    base_key = f"crypto:{sanitized_product}:{granularity.lower()}"
    return f"{base_key}:latest"


def format_insights_for_discord(insights: Dict[str, Any]) -> str:
    """
    Format insights dict into a Discord-suitable message string.

    Args:
        insights: Dictionary from extract_insights / Redis latest snapshot

    Returns:
        Formatted string for posting to Discord
    """
    parts = [
        f"**🚨 {insights.get('product_id', 'N/A')}** ({insights.get('granularity', 'N/A')}) - Trade Signal\n",
        f"Date: {insights.get('date', 'N/A')}",
        f"Updated: {insights.get('last_updated', 'N/A')}",
        f"Close Price: {insights.get('close_price')}",
        f"Risk: {insights.get('risk_level', 'N/A')}",
    ]
    if insights.get("trending_sma_50_inflection"):
        parts.append(f"Trending Average: {insights['trending_sma_50_inflection']}")
    if insights.get("support_resistance_sma_inflection"):
        parts.append(f"Support/Resistance Average: {insights['support_resistance_sma_inflection']}")
    if insights.get("rsi_over_condition"):
        parts.append(f"RSI: {insights['rsi_over_condition']}")
    if insights.get("stoch_rsi_over_condition"):
        parts.append(f"Stoch RSI: {insights['stoch_rsi_over_condition']}")
    if insights.get("close_vs_kijun"):
        parts.append(f"Close vs Kijun: {insights['close_vs_kijun']}")
    if insights.get("trading_trending_sma_cross"):
        parts.append(f"SMA cross: {insights['trading_trending_sma_cross']}")
    if insights.get("trending_support_resistance_cross"):
        parts.append(f"Trending Support/Resistance Cross: {insights['trending_support_resistance_cross']}")
    parts.append("\n")
    return "\n".join(parts)


def store_insights_to_redis(
    insights: Dict[str, Any],
    product_id: str,
    granularity: str,
    redis_client: Optional[redis.Redis] = None,
    history_limit: int = 100
) -> bool:
    """
    Store insights in Redis with proper key structure.
    
    Args:
        insights: Dictionary containing extracted insights
        product_id: Product identifier (e.g., BTC-USD)
        granularity: Time granularity (e.g., ONE_DAY)
        redis_client: Redis client instance (if None, will create one)
        history_limit: Maximum number of historical entries to keep
        
    Returns:
        True if successful, False otherwise
    """
    if redis_client is None:
        redis_client = get_redis_client()
    
    if redis_client is None:
        logger.error("Cannot store insights: Redis client is None")
        return False
    
    try:
        # Sanitize product_id for Redis key
        sanitized_product = product_id.replace("-", "_").replace("/", "_")
        base_key = f"crypto:{sanitized_product}:{granularity.lower()}"
        
        # Store latest snapshot
        latest_key = f"{base_key}:latest"
        insights_json = json.dumps(insights, default=str)
        redis_client.set(latest_key, insights_json)
        logger.info(f"Stored latest insights to {latest_key}")
        
        # Add to history list (capped at history_limit)
        history_key = f"{base_key}:history"
        redis_client.lpush(history_key, insights_json)
        redis_client.ltrim(history_key, 0, history_limit - 1)
        
        # Store metadata
        meta_key = f"{base_key}:meta"
        metadata = {
            "last_updated": insights.get("last_updated"),
            "product_id": product_id,
            "granularity": granularity,
            "history_count": redis_client.llen(history_key)
        }
        redis_client.set(meta_key, json.dumps(metadata, default=str))
        
        logger.info(f"Successfully stored insights for {product_id} {granularity} in Redis")
        return True
        
    except (ConnectionError, TimeoutError, RedisError) as e:
        logger.error(f"Redis error storing insights: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error storing insights: {e}")
        return False


def store_insights_from_parquet(
    parquet_file: str,
    product_id: str,
    granularity: str,
    history_limit: int = 100
) -> bool:
    """
    Convenience function to read parquet file, extract insights, and store in Redis.
    
    Args:
        parquet_file: Path to parquet file from risk_target_task
        product_id: Product identifier (e.g., BTC-USD)
        granularity: Time granularity (e.g., ONE_DAY)
        history_limit: Maximum number of historical entries to keep
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Loading data from {parquet_file}")
        df = pd.read_parquet(parquet_file)
        logger.info(f"Loaded {len(df)} records")
        
        # Extract insights
        insights = extract_insights(df, product_id, granularity)
        
        # Store in Redis
        success = store_insights_to_redis(insights, product_id, granularity, history_limit=history_limit)
        
        if success:
            logger.info(f"Successfully stored insights for {product_id} {granularity}")
        else:
            logger.warning(f"Failed to store insights for {product_id} {granularity} (Redis may be unavailable)")
        
        return success
        
    except FileNotFoundError:
        logger.error(f"Parquet file not found: {parquet_file}")
        return False
    except Exception as e:
        logger.error(f"Error processing insights from parquet: {e}")
        return False
