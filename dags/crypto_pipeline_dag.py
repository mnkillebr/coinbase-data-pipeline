"""
Dynamic DAG generator for crypto data pipeline using TaskFlow API.
Creates separate DAGs for each granularity with appropriate schedules.
Each DAG processes all configured products for that granularity.
"""

import json
import os
import sys
from airflow.sdk import dag, task, get_current_context, TriggerRule
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import discord

# Import configuration from the root configs directory
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from configs.crypto_pipeline_config import (
    PRODUCTS, GRANULARITIES, AWS_CONFIG, PATHS, DAG_CONFIG,
    SPARK_CONFIG, REDIS_CONFIG, DISCORD_CONFIG
)

# Import the data collection functions from the project root
from utils.collect_coinbase_data import collect_and_save_candles, update_existing_data, logger
from utils.calculate_technical_indicators import calculate_and_save_indicators
from utils.calculate_risk_target import calculate_risk_target_and_save
from utils.store_crypto_insights import (
    store_insights_from_parquet,
    get_insights_latest_key,
    get_redis_client,
    format_insights_for_discord,
)

def get_environment_config():
    """Get configuration"""
    return {
        **PATHS,
        "aws_profile": AWS_CONFIG["aws_profile"],
        "s3_bucket": AWS_CONFIG["s3_bucket"],
        "s3_prefix": AWS_CONFIG["s3_prefix"],
        "s3_processed_prefix": AWS_CONFIG["s3_processed_prefix"]
    }

@task.branch
def check_data_exists_branch(product_id: str, granularity: str):
    """Branch function to determine if data exists and return appropriate task ID"""
    try:

        context = get_current_context()
        task_id = context["task_instance"].task_id
        task_suffix = f"__{task_id.split('__')[1]}" if "__" in task_id else None
   
        config = get_environment_config()
        sanitized_product = product_id.replace("-", "_").replace("/", "_")
        filename = f"{sanitized_product}_{granularity.lower()}.csv"
        filepath = os.path.join(config['data_dir'], filename)

        print("Checking filepath: ", filepath)

        exists = os.path.exists(filepath)
        logger.info(f"Data file check for {product_id} {granularity}: {'EXISTS' if exists else 'NOT FOUND'}")
        
        if exists:
            return f"update_data_task{task_suffix}" if task_suffix else "update_data_task"
        else:
            return f"collect_initial_data_task{task_suffix}" if task_suffix else "collect_initial_data_task"
    except Exception as e:
        logger.error(f"Error checking data existence for {product_id} {granularity}: {e}")
        return "collect_initial_data_task"  # Default to collect

@task
def collect_initial_data_task(product_id: str, granularity: str):
    """TaskFlow task to collect initial data for a specific product and granularity"""
    config = get_environment_config()
    try:
        logger.info(f"Starting initial data collection for {product_id} {granularity}")
        
        # Determine how many candles to collect based on granularity
        if granularity == "ONE_DAY":
            total_candles = 4100  # ~10 years of daily data
        elif granularity == "FOUR_HOUR":
            total_candles = 25000  # ~10 years of 4-hour data
        elif granularity == "ONE_HOUR":
            total_candles = 100000  # ~11 years of hourly data
        elif granularity == "FIFTEEN_MINUTE":
            total_candles = 360000  # ~10 years of 15-minute data
        else:
            total_candles = 4000  # Default to daily amount
        
        print(f"Collecting {total_candles} initial candles for {product_id} on {granularity}")
        result = collect_and_save_candles(product_id, granularity, total_candles, config['data_dir'])
        if result is not None:
            logger.info(f"Successfully collected initial data for {product_id} {granularity}")
            return f"Collected initial data for {product_id} {granularity}"
        else:
            raise Exception(f"Failed to collect initial data for {product_id} {granularity}")
    except Exception as e:
        logger.error(f"Error collecting initial data for {product_id} {granularity}: {e}")
        raise

@task
def update_data_task(product_id: str, granularity: str):
    """TaskFlow task to update data for a specific product and granularity"""
    config = get_environment_config()
    try:
        logger.info(f"Starting data update for {product_id} {granularity}")
        success = update_existing_data(product_id, granularity, config['data_dir'])
        if success:
            logger.info(f"Successfully updated {product_id} {granularity}")
            return f"Updated {product_id} {granularity}"
        else:
            raise Exception(f"Failed to update {product_id} {granularity}")
    except Exception as e:
        logger.error(f"Error updating {product_id} {granularity}: {e}")
        raise

@task.bash(trigger_rule=TriggerRule.ONE_SUCCESS)
def upload_to_s3_task(product_id: str, granularity: str):
    """TaskFlow task to upload updated CSV to S3"""
    config = get_environment_config()
    
    # Construct file paths
    sanitized_product = product_id.replace("-", "_").replace("/", "_")
    local_file = f"{config['data_dir']}/{sanitized_product}_{granularity.lower()}.csv"
    s3_path = f"s3://{config['s3_bucket']}/{config['s3_prefix']}/{sanitized_product}_{granularity.lower()}.csv"
    script_path = f"{config['scripts_dir']}/upload_to_s3.sh"

    # AWS profile is optional; only pass if set and not a comment (e.g. from .env)
    aws_profile = (config.get('aws_profile') or '').strip()
    if aws_profile and not aws_profile.startswith('#'):
        return f"{script_path} {local_file} {s3_path} {aws_profile}"
    return f"{script_path} {local_file} {s3_path}"


@task
def calculate_technical_indicators_task(product_id: str, granularity: str):
    """TaskFlow task to calculate technical indicators for a specific product and granularity"""
    config = get_environment_config()
    try:
        logger.info(f"Starting technical indicator calculation for {product_id} {granularity}")
        
        # Construct file paths
        sanitized_product = product_id.replace("-", "_").replace("/", "_")
        input_file = f"{config['data_dir']}/{sanitized_product}_{granularity.lower()}.csv"
        
        # Get output directory from appropriate config
        output_dir = SPARK_CONFIG.get('output_dir', config.get('data_dir', '/tmp'))
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = f"{output_dir}/{sanitized_product}_{granularity.lower()}.parquet"
        
        # Calculate and save indicators
        result_file = calculate_and_save_indicators(
            product_id=product_id,
            granularity=granularity,
            input_file=input_file,
            output_file=output_file
        )
        
        logger.info(f"Successfully calculated indicators for {product_id} {granularity}")
        return result_file
        
    except Exception as e:
        logger.error(f"Error calculating technical indicators for {product_id} {granularity}: {e}")
        raise

@task
def calculate_risk_target_task(product_id: str, granularity: str, indicators_output_file: str):
    """TaskFlow task to calculate risk target for a specific product and granularity
    
    Args:
        product_id: Product identifier (e.g., BTC-USD)
        granularity: Time granularity (e.g., ONE_DAY)
        indicators_output_file: Path to parquet file from technical indicators task
    """
    config = get_environment_config()
    try:
        logger.info(f"Starting risk target calculation for {product_id} {granularity}")
        logger.info(f"Using indicators file: {indicators_output_file}")
        
        # Construct file paths
        sanitized_product = product_id.replace("-", "_").replace("/", "_")
        
        # Get output directory from appropriate config
        output_dir = SPARK_CONFIG.get('output_dir', config.get('data_dir', '/tmp'))
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Output file will be the same name but with risk target suffix
        # Or we can overwrite the same file since it will have additional columns
        output_file = f"{output_dir}/{sanitized_product}_{granularity.lower()}.parquet"
        
        # Calculate and save risk target (input is the parquet file from indicators task)
        result_file = calculate_risk_target_and_save(
            product_id=product_id,
            granularity=granularity,
            input_file=indicators_output_file,
            output_file=output_file
        )
        
        logger.info(f"Successfully calculated risk target for {product_id} {granularity}")
        return result_file
    except Exception as e:
        logger.error(f"Error calculating risk target for {product_id} {granularity}: {e}")
        raise

@task
def store_insights_task(product_id: str, granularity: str, risk_target_output_file: str):
    """TaskFlow task to extract insights from processed data and store in Redis.

    Returns the Redis latest key so the Discord task can fetch insights.

    Args:
        product_id: Product identifier (e.g., BTC-USD)
        granularity: Time granularity (e.g., ONE_DAY)
        risk_target_output_file: Path to parquet file from risk_target_task

    Returns:
        Redis key for latest insights (e.g. crypto:BTC_USD:one_day:latest)
    """
    redis_key = get_insights_latest_key(product_id, granularity)
    try:
        logger.info(f"Starting insights storage for {product_id} {granularity}")
        logger.info(f"Using processed data file: {risk_target_output_file}")

        # Get history limit from config
        history_limit = REDIS_CONFIG.get('history_limit', 100)

        # Store insights in Redis
        success = store_insights_from_parquet(
            parquet_file=risk_target_output_file,
            product_id=product_id,
            granularity=granularity,
            history_limit=history_limit
        )

        if success:
            logger.info(f"Successfully stored insights for {product_id} {granularity} in Redis")
        else:
            logger.warning(f"Failed to store insights for {product_id} {granularity} - Redis may be unavailable")
    except Exception as e:
        logger.error(f"Error storing insights for {product_id} {granularity}: {e}")
        logger.warning(f"Continuing DAG execution despite Redis storage failure: {e}")
    return redis_key


def should_post_insights_to_discord(insights: dict) -> bool:
    """Criteria for whether to post insights to Discord. Tune rules here."""
    # Post if high risk, or RSI extreme (oversold/overbought), or stoch RSI extreme
    if insights.get("risk_level") == "low risk" or insights.get("risk_level") == "very low risk":
        return True
    # Option: always post for testing; set to False and rely on rules above in production
    return False


@task(trigger_rule=TriggerRule.ONE_SUCCESS)
def post_insights_to_discord_task(product_id: str, granularity: str, redis_key: str):
    """TaskFlow task to post insights to Discord for a specific product and granularity.

    Fetches insights from Redis by redis_key (returned by store_insights_task), formats them,
    checks criteria; only posts when criteria are met. Uses discord.py client with on_ready.
    """
    try:
        logger.info(f"Starting Discord post for {product_id} {granularity} (redis_key={redis_key})")

        channel_id = DISCORD_CONFIG.get(f"discord_{granularity.lower()}_channel_id") or ""
        token = (DISCORD_CONFIG.get("discord_token") or "").strip()
        if not token or not channel_id:
            logger.warning("Discord token or channel ID not set; skipping post")
            return "Skipped: Discord token or channel ID not configured"

        redis_client = get_redis_client()
        if redis_client is None:
            logger.warning("Redis client unavailable; cannot fetch insights for Discord")
            return "Skipped: Redis unavailable"

        raw = redis_client.get(redis_key)
        if not raw:
            logger.warning(f"No insights found at key {redis_key}; skipping post")
            return f"Skipped: No insights at {redis_key}"

        try:
            insights = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as e:
            logger.warning(f"Invalid JSON at {redis_key}: {e}; skipping post")
            return "Skipped: Invalid insights JSON"

        if not should_post_insights_to_discord(insights):
            logger.info(f"Criteria not met for {product_id} {granularity}; not posting to Discord")
            return "Skipped: Post criteria not met"

        message_to_send = format_insights_for_discord(insights)
        channel_id_int = int(channel_id)

        intents = discord.Intents.default()
        client = discord.Client(intents=intents)

        @client.event
        async def on_ready():
            channel = client.get_channel(channel_id_int)
            if channel is None:
                logger.warning(f"Discord channel {channel_id_int} not found")
                await client.close()
                return
            await channel.send(message_to_send)
            await client.close()

        client.run(token)
        logger.info(f"Posted insights to Discord for {product_id} {granularity}")
        return f"Posted insights to Discord for {product_id} {granularity}"

    except Exception as e:
        logger.error(f"Error posting insights to Discord for {product_id} {granularity}: {e}")
        logger.warning(f"Continuing DAG execution despite Discord posting failure: {e}")
        return f"Error posting insights to Discord: {str(e)}"
    
def create_spark_processing_task(product_id: str, granularity: str):
    """Create a SparkSubmitOperator task to process data with Spark"""
    config = get_environment_config()
    
    # Construct file paths
    sanitized_product = product_id.replace("-", "_").replace("/", "_")
    input_file = f"{config['data_dir']}/{sanitized_product}_{granularity.lower()}.csv"
    
    # Get output directory from appropriate config
    output_dir = f"{SPARK_CONFIG['output_dir']}/{sanitized_product}_{granularity.lower()}"
    spark_job_path = config['spark_job_path']
    spark_master = SPARK_CONFIG["master"]
        
    
    return SparkSubmitOperator(
        task_id=f"process_spark_{product_id.replace('-', '_').lower()}",
        application=spark_job_path,
        name=f"{SPARK_CONFIG['app_name']}_{sanitized_product}_{granularity.lower()}",
        conn_id=SPARK_CONFIG["conn_id"],
        application_args=[
            "--product_id", product_id,
            "--granularity", granularity,
            "--input_path", input_file,
            "--output_path", output_dir
        ],
        conf={
            "spark.master": spark_master,
            "spark.app.name": f"{SPARK_CONFIG['app_name']}_{sanitized_product}_{granularity.lower()}"
        }
    )

@task.bash
def upload_to_s3_processed_task(product_id: str, granularity: str, risk_target_output_file: str):
    """TaskFlow task to upload processed parquet file (with risk target) to S3
    
    Args:
        product_id: Product identifier (e.g., BTC-USD)
        granularity: Time granularity (e.g., ONE_DAY)
        risk_target_output_file: Path to parquet file from risk target task
    """
    config = get_environment_config()
    
    # Construct file paths
    sanitized_product = product_id.replace("-", "_").replace("/", "_")
    
    # Use the file from risk_target_task output
    local_file = risk_target_output_file
    s3_path = f"s3://{config['s3_bucket']}/{config['s3_processed_prefix']}/{sanitized_product}_{granularity.lower()}.parquet"
    script_path = f"{config['scripts_dir']}/upload_to_s3.sh"
    
    return f"{script_path} {local_file} {s3_path} {config['aws_profile']}"

# Generate DAGs dynamically
for granularity, granularity_config in GRANULARITIES.items():
    
    @dag(
        dag_id=f"crypto_pipeline_{granularity.lower()}",
        description=f"Crypto data pipeline for {granularity_config['description']}",
        schedule=granularity_config["schedule"],
        **DAG_CONFIG
    )
    def create_crypto_pipeline():
        """Create a crypto pipeline DAG for a specific granularity"""
        
        # Create tasks for each product
        for product_id in PRODUCTS:
            # Create branch task to check data existence
            branch_task = check_data_exists_branch(product_id, granularity)

            # Create conditional tasks
            update_result = update_data_task(product_id, granularity)
            collect_result = collect_initial_data_task(product_id, granularity)
            
            # Create S3 upload task
            upload_result = upload_to_s3_task(product_id, granularity)

            # Create technical indicators calculation task
            indicators_task = calculate_technical_indicators_task(product_id, granularity)
            
            # Create risk target calculation task (uses output from indicators_task)
            risk_target_task = calculate_risk_target_task(product_id, granularity, indicators_task)

            # Create Spark processing task
            # spark_task = create_spark_processing_task(product_id, granularity)
            
            # Create S3 upload task using TaskFlow for processed data (uses output from risk_target_task)
            upload_processed_result = upload_to_s3_processed_task(product_id, granularity, risk_target_task)

            # Create insights storage task (uses output from risk_target_task)
            store_insights_result = store_insights_task(product_id, granularity, risk_target_task)
            # Post insights to Discord (uses redis_key returned by store_insights_task)
            discord_result = post_insights_to_discord_task(product_id, granularity, store_insights_result)

            # Set up conditional dependencies:
            # Branch -> [update OR collect] -> upload -> spark
            branch_task >> [update_result, collect_result]
            # [update_result, collect_result] >> indicators_task >> risk_target_task
            [update_result, collect_result] >> upload_result >> indicators_task >> risk_target_task
            risk_target_task >> [upload_processed_result, store_insights_result]
            store_insights_result >> discord_result
    
    # Call to register the DAG with Airflow (2.4+ auto-registers; assigning to globals() for older discovery)
    dag_instance = create_crypto_pipeline()
    globals()[f"crypto_pipeline_{granularity.lower()}"] = dag_instance
