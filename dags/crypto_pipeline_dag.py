"""
Dynamic DAG generator for crypto data pipeline using TaskFlow API.
Creates separate DAGs for each granularity with appropriate schedules.
Each DAG processes all configured products for that granularity.
"""

import os
import sys
from airflow.sdk import dag, task, get_current_context, TriggerRule
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Import configuration from the root configs directory
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from configs.crypto_pipeline_config import (
    PRODUCTS, GRANULARITIES, AWS_CONFIG, PATHS, DAG_CONFIG, 
    SPARK_CONFIG
)

# Import the data collection functions from the project root
from utils.collect_coinbase_data import collect_and_save_candles, update_existing_data, logger

def get_environment_config():
    """Get configuration"""
    return {
        **PATHS,
        "aws_profile": AWS_CONFIG["aws_profile"],
        "s3_bucket": AWS_CONFIG["s3_bucket"],
        "s3_prefix": AWS_CONFIG["s3_prefix"]
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
        result = collect_and_save_candles(product_id, granularity, total_candles)
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

    return f"{script_path} {local_file} {s3_path} {config['aws_profile']}"


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
            
            # Create Spark processing task
            spark_task = create_spark_processing_task(product_id, granularity)
            
            # Set up conditional dependencies:
            # Branch -> [update OR collect] -> upload -> spark
            branch_task >> [update_result, collect_result]
            [update_result, collect_result] >> upload_result >> spark_task
    
    # Make the DAG available to Airflow
    globals()[f"dag_{granularity.lower()}"] = create_crypto_pipeline()
