import os
import pandas as pd
import time
import math
import pytz
import logging
from datetime import datetime, timedelta
from coinbase.rest import RESTClient
from dotenv import dotenv_values

# Remove module-level client initialization - will be created in functions

rate_limit = 10000 # 10000 requests per hour
max_candles_per_request = 340
granularities = ["FIFTEEN_MINUTE", "ONE_HOUR", "FOUR_HOUR", "ONE_DAY"]

# Setup logging
def setup_logging():
    """Setup logging configuration for monitoring updates"""
    from airflow.sdk import Variable

    try:
      log_dir = Variable.get("LOG_DIR")
    except Exception as e:
      print(f"Error getting log directory from Airflow Variables: {e}")
      config = dotenv_values(".env")

      if not config.get("LOG_DIR"):
          raise ValueError("LOG_DIR not found in Airflow Variables or environment")

      log_dir = config["LOG_DIR"]
      print(f"Using log directory from environment variables: {log_dir}")
    os.makedirs(log_dir, exist_ok=True)
    
    log_filename = f"crypto_data_updates_{datetime.now().strftime('%Y%m%d')}.log"
    log_filepath = os.path.join(log_dir, log_filename)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filepath),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def get_coinbase_client():
    """
    Initialize and return a Coinbase REST client with proper error handling.
    This function should be called each time we need to make API calls.
    """
    try:
        from airflow.sdk import Variable
        
        # Try to get variables from Airflow Variables first
        try:
            api_key = Variable.get("COINBASE_API_KEY_NAME")
            api_secret = Variable.get("COINBASE_API_PRIVATE_KEY")
            logger.info("Successfully retrieved credentials from Airflow Variables")
        except Exception as e:
            logger.warning(f"Could not retrieve from Airflow Variables: {e}")
            # Fallback to environment variables for local development
            config = dotenv_values(".env")
            
            if not config.get("COINBASE_API_KEY_NAME"):
                raise ValueError("COINBASE_API_KEY_NAME not found in Airflow Variables or environment")
            if not config.get("COINBASE_API_PRIVATE_KEY"):
                raise ValueError("COINBASE_API_PRIVATE_KEY not found in Airflow Variables or environment")
            
            api_key = config["COINBASE_API_KEY_NAME"]
            api_secret = config["COINBASE_API_PRIVATE_KEY"]
            logger.info("Using credentials from environment variables")
        
        # Create a new client instance
        client = RESTClient(api_key=api_key, api_secret=api_secret)
        
        logger.info("Successfully initialized Coinbase client")
        return client
        
    except Exception as e:
        logger.error(f"Failed to initialize Coinbase client: {e}")
        raise

def get_product_candles(product_id: str, granularity: str, start: str, end: str):
    """Get product candles from Coinbase API"""
    try:
        client = get_coinbase_client()
        logger.info(f"Getting candles for {product_id} {granularity} from {start} to {end}")
        return client.get_candles(product_id=product_id, granularity=granularity, start=start, end=end)
    except Exception as e:
        logger.error(f"Failed to get product candles for {product_id}: {e}")
        raise

def get_last_closed_candle_time(granularity: str):
    """
    Calculate the timestamp of the last closed candle based on granularity.
    A candle is considered closed when enough time has passed since its start time.
    All returned datetimes are timezone-aware (UTC).
    """
    now = datetime.now(pytz.UTC)  # Make now timezone-aware
    est = pytz.timezone('US/Eastern')
    now_est = now.astimezone(est)
    
    if granularity == "ONE_DAY":
        # Daily candles close at 8 PM EST (1 AM UTC next day)
        # Always go back to yesterday's 8 PM close to ensure the candle is fully closed
        # This ensures we never collect today's candle until it's actually closed
        if now_est.hour < 20:
            last_close = now_est.replace(hour=20, minute=0, second=0, microsecond=0) - timedelta(days=1)
        else:
            last_close = now_est.replace(hour=20, minute=0, second=0, microsecond=0)
        return last_close
    
    elif granularity == "FOUR_HOUR":
        # 4-hour candles: ensure at least 4 hours have passed since the candle start
        # Find the last 4-hour boundary and ensure it's closed
        hours_back = now.hour % 4
        last_close = now_est.replace(minute=0, second=0, microsecond=0) - timedelta(hours=hours_back)
        return last_close
    
    elif granularity == "ONE_HOUR":
        # Hourly candles: ensure at least 1 hour has passed since the candle start
        # Find the last hour boundary and ensure it's closed
        last_close = now_est.replace(minute=0, second=0, microsecond=0)
        return last_close
    
    elif granularity == "FIFTEEN_MINUTE":
        # 15-minute candles: ensure at least 15 minutes have passed since the candle start
        # Find the last 15-minute boundary and ensure it's closed
        minutes_back = now_est.minute % 15
        last_close = now_est.replace(second=0, microsecond=0) - timedelta(minutes=(minutes_back))
        return last_close
    
    else:
        # Default to current time for unknown granularities
        return now_est

def get_last_candle_from_file(filepath: str):
    """Get the timestamp of the last candle from an existing CSV file"""
    try:
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            if not df.empty:
                # Ensure 'start' column is integer type and get the last timestamp
                df['start'] = df['start'].astype(int)
                last_timestamp = df['start'].iloc[-1]
                return int(last_timestamp)
        return None
    except Exception as e:
        logger.error(f"Error reading last candle from {filepath}: {e}")
        return None

def append_new_candles_to_file(filepath: str, new_candles: list, granularity: str):
    """Append new candles to an existing CSV file"""
    try:
        if not new_candles:
            logger.info("No new candles to append")
            return True
            
        # Read existing data
        existing_df = pd.read_csv(filepath)
        
        # Create DataFrame from new candles
        new_df = pd.DataFrame(new_candles)
        
        # Convert numeric columns to proper types
        numeric_columns = ['start', 'open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            if col in new_df.columns:
                new_df[col] = pd.to_numeric(new_df[col], errors='coerce')
        
        # Convert start timestamp to readable date
        # For daily candles, show the close time (start + 24 hours)
        # For other granularities, show the start time
        if granularity == "ONE_DAY":
            # Add 24 hours to show close time for daily candles
            new_df['date'] = pd.to_datetime(new_df['start'].astype(int) + 86400, unit='s', utc=True).dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d %H:%M:%S %Z')
        else:
            new_df['date'] = pd.to_datetime(new_df['start'].astype(int), unit='s', utc=True).dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d %H:%M:%S %Z')
        
        # Reorder columns to match existing format
        columns = ['date', 'start', 'open', 'high', 'low', 'close', 'volume']
        new_df = new_df[columns]
        
        # Combine existing and new data
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Ensure 'start' column is integer type for proper sorting
        combined_df['start'] = combined_df['start'].astype(int)
        
        # Remove duplicates based on 'start' timestamp
        combined_df = combined_df.drop_duplicates(subset=['start'], keep='first')
        
        # Sort by start time
        combined_df = combined_df.sort_values('start').reset_index(drop=True)
        
        # Save combined data
        combined_df.to_csv(filepath, index=False)
        # pq_filepath = filepath.split('.')[0] + '.parquet'
        # combined_df.to_parquet(pq_filepath, index=False)
        
        # logger.info(f"Successfully appended {len(new_candles)} new candles to {filepath} and {pq_filepath}")
        logger.info(f"Total candles in file: {len(combined_df)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error appending candles to {filepath}: {e}")
        return False

def update_existing_data(product_id: str, granularity: str, data_dir: str):
    """
    Update existing data file with new candles since the last update.
    This function is designed for scheduled runs to append new data.
    """
    import random
    
    # Add small random delay to prevent simultaneous file access
    time.sleep(random.uniform(0.1, 0.5))
    
    logger.info(f"Starting update for {product_id} {granularity}")
    
    # Find existing data file
    sanitized_product = product_id.replace("-", "_").replace("/", "_")
    
    # Look for existing files with this product and granularity
    existing_files = [f for f in os.listdir(data_dir) if f.startswith(f"{sanitized_product}_{granularity.lower()}")]
    
    if not existing_files:
        logger.warning(f"No existing data file found for {product_id} {granularity}. Run initial collection first.")
        return False
    
    # Use the most recent file (by date in filename)
    existing_files.sort(reverse=True)
    filepath = os.path.join(data_dir, existing_files[0])
    
    logger.info(f"Found existing file: {filepath}")
    
    # Get the last candle timestamp from the file
    last_timestamp = get_last_candle_from_file(filepath)
    if last_timestamp is None:
        logger.error(f"Could not read last timestamp from {filepath}")
        return False
    
    # Calculate the start time for new data (next period after last candle)
    # Convert to UTC timezone-aware datetime to match get_last_closed_candle_time output
    last_candle_time = datetime.fromtimestamp(last_timestamp, tz=pytz.UTC)
    
    if granularity == "ONE_DAY":
        start_time = last_candle_time + timedelta(days=1)
    elif granularity == "FOUR_HOUR":
        start_time = last_candle_time + timedelta(hours=4)
    elif granularity == "ONE_HOUR":
        start_time = last_candle_time + timedelta(hours=1)
    elif granularity == "FIFTEEN_MINUTE":
        start_time = last_candle_time + timedelta(minutes=15)
    else:
        start_time = last_candle_time + timedelta(days=1)
    
    # Get the current end time (last closed candle) - this returns UTC timezone-aware datetime
    end_time = get_last_closed_candle_time(granularity)
    
    # Check if we need to collect new data
    if start_time >= end_time:
        logger.info(f"No new data needed for {product_id} {granularity}. Last candle: {last_candle_time}, Current end: {end_time}")
        return True
    
    logger.info(f"Collecting new data from {start_time} to {end_time}")
    
    # Convert to Unix timestamps
    start_timestamp = str(int(start_time.timestamp()))
    end_timestamp = str(int(end_time.timestamp()))
    
    try:
        # Make API call to get new candles
        response = get_product_candles(product_id, granularity, start_timestamp, end_timestamp)
        # Convert response object to dictionary
        response_dict = response.to_dict() if hasattr(response, 'to_dict') else response
        
        if response_dict and 'candles' in response_dict:
            new_candles = response_dict['candles']
            logger.info(f"Collected {len(new_candles)} new candles")
            
            if new_candles:
                # Append new candles to existing file
                success = append_new_candles_to_file(filepath, new_candles, granularity)
                if success:
                    logger.info(f"Successfully updated {product_id} {granularity} with {len(new_candles)} new candles")
                    return True
                else:
                    logger.error(f"Failed to append new candles to {filepath}")
                    return False
            else:
                logger.info(f"No new candles found for {product_id} {granularity}")
                return True
        else:
            logger.warning(f"No response or candles in response for {product_id} {granularity}")
            return False
            
    except Exception as e:
        logger.error(f"Error collecting new data for {product_id} {granularity}: {e}")
        return False

def collect_and_save_candles(product_id: str, granularity: str, total_candles_needed: int):
    """
    Collects and saves candles for a given product and granularity.
    Uses multiple API calls with 340-period windows to collect all data.
    Respects rate limits and saves sorted data to CSV.
    """
    
    # Calculate how many requests we need
    num_requests = math.ceil(total_candles_needed / max_candles_per_request)
    
    # Calculate time windows - use the last closed candle as end time
    end_time = get_last_closed_candle_time(granularity)
    all_candles = []
    
    logger.info(f"Collecting {total_candles_needed} {granularity} candles for {product_id}")
    logger.info(f"Will make {num_requests} requests with 340-period windows")
    logger.info(f"Last closed candle time: {end_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    successful_requests = 0
    failed_requests = 0
    
    for i in range(num_requests):
        # Calculate start and end times for this request
        if i == 0:
            # First request: from last closed candle back max_candles_per_request periods
            request_end = end_time
            if granularity == "ONE_DAY":
                request_start = end_time - timedelta(days=max_candles_per_request)
            elif granularity == "FOUR_HOUR":
                request_start = end_time - timedelta(hours=4 * max_candles_per_request)
            elif granularity == "ONE_HOUR":
                request_start = end_time - timedelta(hours=max_candles_per_request)
            elif granularity == "FIFTEEN_MINUTE":
                request_start = end_time - timedelta(minutes=15 * max_candles_per_request)
            else:
                request_start = end_time - timedelta(days=max_candles_per_request)
        else:
            # Subsequent requests: slide the window back
            request_end = request_start
            if granularity == "ONE_DAY":
                request_start = request_end - timedelta(days=max_candles_per_request)
            elif granularity == "FOUR_HOUR":
                request_start = request_end - timedelta(hours=4 * max_candles_per_request)
            elif granularity == "ONE_HOUR":
                request_start = request_end - timedelta(hours=max_candles_per_request)
            elif granularity == "FIFTEEN_MINUTE":
                request_start = request_end - timedelta(minutes=15 * max_candles_per_request)
            else:
                request_start = request_end - timedelta(days=max_candles_per_request)
        
        # Convert to Unix timestamp (seconds since epoch)
        start_timestamp = str(int(request_start.timestamp()))
        end_timestamp = str(int(request_end.timestamp()))
        
        logger.info(f"Request {i+1}/{num_requests}: {request_start.strftime('%Y-%m-%d %H:%M:%S')} to {request_end.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Make API call
            response = get_product_candles(product_id, granularity, start_timestamp, end_timestamp)
            
            if response and 'candles' in response:
                candles = response['candles']
                all_candles.extend(candles)
                successful_requests += 1
                logger.info(f"  Collected {len(candles)} candles")
            else:
                logger.warning(f"  No candles returned for this request")
                failed_requests += 1
            
            # Rate limiting: wait if we're approaching the limit
            # 10,000 requests per hour = ~1 request every 0.36 seconds
            # Add a small buffer and wait 0.5 seconds between requests
            if i < num_requests - 1:  # Don't wait after the last request
                time.sleep(0.5)
                
        except Exception as e:
            logger.error(f"  Error collecting data for request {i+1}: {e}")
            failed_requests += 1
            continue
    
    logger.info(f"Collection complete. Successful requests: {successful_requests}, Failed requests: {failed_requests}")
    
    if all_candles:
        # Sort all candles by start time (ascending)
        all_candles.sort(key=lambda x: int(x['start']))
        
        # Filter out candles that are beyond our calculated end time
        end_timestamp = int(end_time.timestamp())
        filtered_candles = [candle for candle in all_candles if int(candle['start']) <= end_timestamp]
        
        if filtered_candles:
            logger.info(f"Total candles collected: {len(all_candles)}")
            logger.info(f"Total candles after filtering: {len(filtered_candles)}")
            logger.info(f"First candle: {datetime.fromtimestamp(int(filtered_candles[0]['start'])).strftime('%Y-%m-%d %H:%M:%S %Z')}")
            logger.info(f"Last candle: {datetime.fromtimestamp(int(filtered_candles[-1]['start'])).strftime('%Y-%m-%d %H:%M:%S %Z')}")
            logger.info(f"Calculated end time: {end_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            
            # Save to CSV
            df = pd.DataFrame(filtered_candles)
            
            # Convert numeric columns to proper types
            numeric_columns = ['start', 'open', 'high', 'low', 'close', 'volume']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Convert start timestamp to readable date for better CSV viewing
            # For daily candles, show the close time (start + 24 hours)
            # For other granularities, show the start time
            if granularity == "ONE_DAY":
                # Add 24 hours to show close time for daily candles
                df['date'] = pd.to_datetime(df['start'].astype(int) + 86400, unit='s', utc=True).dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d %H:%M:%S %Z')
            else:
                df['date'] = pd.to_datetime(df['start'].astype(int), unit='s', utc=True).dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d %H:%M:%S %Z')
            
            # Reorder columns to put date first
            columns = ['date', 'start', 'open', 'high', 'low', 'close', 'volume']
            df = df[columns]
            
            # Save to CSV
            # Create a sanitized product name for filename
            sanitized_product = product_id.replace("-", "_").replace("/", "_")
            csv_filename = f"{sanitized_product}_{granularity.lower()}.csv"
            csv_filepath = os.path.join("data", csv_filename)
            # pq_filename = f"{sanitized_product}_{granularity.lower()}.parquet"
            # pq_filepath = os.path.join("data", pq_filename)
            os.makedirs("data", exist_ok=True)
            
            df.to_csv(csv_filepath, index=False)
            # df.to_parquet(pq_filepath, index=False)
            logger.info(f"Data saved to: {csv_filepath}")
            # logger.info(f"Data saved to: {pq_filepath}")
            return df
        else:
            logger.warning("No candles remained after filtering")
            return None
    else:
        logger.warning("No candles were collected")
        return None
