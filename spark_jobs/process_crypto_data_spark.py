"""
PySpark job for processing crypto candle data.
This is a placeholder implementation that can be extended with your specific processing logic.

Usage:
    spark-submit process_candles.py --product_id BTC-USD --granularity ONE_DAY --input_path /path/to/input.csv --output_path /path/to/output
"""

import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour, minute, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

import pandas as pd
import numpy as np
import talib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Process crypto candle data with Spark')
    
    parser.add_argument('--product_id', required=True, help='Product identifier (e.g., BTC-USD)')
    parser.add_argument('--granularity', required=True, help='Time granularity (e.g., ONE_DAY)')
    parser.add_argument('--input_path', required=True, help='Path to input CSV file')
    parser.add_argument('--output_path', required=True, help='Path for output data')
    
    return parser.parse_args()


def create_spark_session(app_name):
    """Create and configure Spark session for EMR Serverless"""
    try:
        # For EMR Serverless, AWS credentials are handled by the execution role
        # No need to explicitly set S3 credentials
        spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
        
        # Set log level to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Successfully created Spark session for {app_name}")
        return spark

    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise


def define_schema():
    """Define the schema for crypto candle data"""
    return StructType([
        StructField("date", StringType(), True),
        StructField("start", LongType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True)
    ])


def load_data(spark, input_path, schema):
    """Load CSV data into Spark DataFrame"""
    logger.info(f"Loading data from: {input_path}")
    
    try:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .csv(input_path)
        
        logger.info(f"Successfully loaded {df.count()} records")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

## Define pandas udf for technical indicators
@pandas_udf(returnType=DoubleType())
def calculate_rsi(close_series: pd.Series) -> pd.Series:
    """Calculate RSI (Relative Strength Index) with 14-period default"""
    return pd.Series(talib.RSI(close_series.values, timeperiod=14))

@pandas_udf(returnType=DoubleType())
def calculate_kijun_v2_200_1(high_series: pd.Series, low_series: pd.Series, close_series: pd.Series) -> pd.Series:
	"""Calculate Kijun V2 with period=200, kidiv=1"""
	period, kidiv = 200, 1
	high_prices = high_series.values
	low_prices = low_series.values
	close_prices = close_series.values
	kijunv2 = np.full_like(close_prices, np.nan)
	
	for i in range(period, len(close_prices)):
		highest_high = np.max(high_prices[i-period:i+1])
		lowest_low = np.min(low_prices[i-period:i+1])
		conversion = int(period/kidiv)
		highest_high_conversion = np.max(high_prices[i-conversion:i+1])
		lowest_low_conversion = np.min(low_prices[i-conversion:i+1])
		kijun = (highest_high + lowest_low) / 2
		conversionLine = (highest_high_conversion + lowest_low_conversion) / 2
		kijunv2[i] = (kijun + conversionLine) / 2
	
	return pd.Series(kijunv2)

@pandas_udf(returnType=DoubleType())
def calculate_ehlers_smoothed_rsi(close_series: pd.Series) -> pd.Series:
	"""Calculate Ehlers Smoothed RSI"""
	pass

@pandas_udf(returnType=DoubleType())
def calculate_macd(close_series: pd.Series) -> pd.Series:
    """Calculate MACD line (12,26,9 default parameters)"""
    macd, signal, histogram = talib.MACDEXT(
			close_series.values,
			fastperiod=12, 
			fastmatype=0, 
			slowperiod=26, 
			slowmatype=0, 
			signalperiod=9, 
			signalmatype=0
		)
    return pd.Series(macd)

@pandas_udf(returnType=DoubleType())
def calculate_macd_signal(close_series: pd.Series) -> pd.Series:
    """Calculate MACD signal line"""
    macd, signal, histogram = talib.MACDEXT(
			close_series.values,
			fastperiod=12, 
			fastmatype=0, 
			slowperiod=26, 
			slowmatype=0, 
			signalperiod=9, 
			signalmatype=0
		)
    return pd.Series(signal)

@pandas_udf(returnType=DoubleType())
def calculate_macd_histogram(close_series: pd.Series) -> pd.Series:
    """Calculate MACD histogram"""
    macd, signal, histogram = talib.MACDEXT(
			close_series.values,
			fastperiod=12, 
			fastmatype=0, 
			slowperiod=26, 
			slowmatype=0, 
			signalperiod=9, 
			signalmatype=0
		)
    return pd.Series(histogram)

@pandas_udf(returnType=DoubleType())
def calculate_atr(high_series: pd.Series, low_series: pd.Series, close_series: pd.Series) -> pd.Series:
    """Calculate ATR (Average True Range) with 14-period default"""
    return pd.Series(talib.ATR(high_series.values, low_series.values, close_series.values, timeperiod=14))

@pandas_udf(returnType=DoubleType())
def calculate_sma_14(close_series: pd.Series) -> pd.Series:
    """Calculate SMA (Simple Moving Average) with 14-period default"""
    return pd.Series(talib.SMA(close_series.values, timeperiod=14))

@pandas_udf(returnType=DoubleType())
def calculate_sma_50(close_series: pd.Series) -> pd.Series:
    """Calculate SMA (Simple Moving Average) with 50-period default"""
    return pd.Series(talib.SMA(close_series.values, timeperiod=50))

@pandas_udf(returnType=DoubleType())
def calculate_sma_138(close_series: pd.Series) -> pd.Series:
    """Calculate SMA (Simple Moving Average) with 20-period default"""
    return pd.Series(talib.SMA(close_series.values, timeperiod=138))

@pandas_udf(returnType=DoubleType())
def calculate_ema(close_series: pd.Series) -> pd.Series:
    """Calculate EMA (Exponential Moving Average) with 20-period default"""
    return pd.Series(talib.EMA(close_series.values, timeperiod=20))

@pandas_udf(returnType=DoubleType())
def calculate_ehlers_stoch_rsi_k(close_series: pd.Series) -> pd.Series:
	"""
	Calculate Ehlers Smoothed Stochastic RSI K signal with hardcoded default parameters.
	Default parameters: K=10, D=3, rsi_length=14, stochastic_length=14
	"""
	# Hardcoded default parameters
	K = 20
	D = 6
	rsi_length = 70
	stochastic_length = 50
	PI = 3.14159265359
	
	# Helper functions
	def xlowest(src, length):
		"""Calculate the lowest value over a rolling window"""
		return src.rolling(window=length, min_periods=1).min()
	
	def xhighest(src, length):
		"""Calculate the highest value over a rolling window"""
		return src.rolling(window=length, min_periods=1).max()
	
	def xstoch(c, h, l, length):
		"""Calculate stochastic value"""
		xlow = xlowest(l, length)
		xhigh = xhighest(h, length)
		# Avoid division by zero
		diff = xhigh - xlow
		diff = diff.replace(0, 1e-10)  # Replace zeros with small value
		return 100 * (c - xlow) / diff
	
	def stochastic(c, h, l, length):
		"""Calculate stochastic with bounds"""
		rawsig = xstoch(c, h, l, length)
		return np.clip(rawsig, 0.0, 100.0)
	
	def xrma(src, length):
		"""Calculate RMA (Relative Moving Average)"""
		alpha = 1.0 / length
		return src.ewm(alpha=alpha, adjust=False).mean()
	
	def xrsi(src, length):
		"""Calculate RSI using RMA"""
		delta = src.diff()
		gain = delta.where(delta > 0, 0)
		loss = -delta.where(delta < 0, 0)
		
		avg_gain = xrma(gain, length)
		avg_loss = xrma(loss, length)
		
		# Avoid division by zero
		avg_loss = avg_loss.replace(0, 1e-10)
		rs = avg_gain / avg_loss
		rsi = 100.0 - (100.0 / (1.0 + rs))
		return rsi
	
	def ehlers_super_smoother(src, lower):
		"""Ehlers Super Smoother filter"""
		a1 = np.exp(-PI * np.sqrt(2) / lower)
		coeff2 = 2 * a1 * np.cos(np.sqrt(2) * PI / lower)
		coeff3 = -np.power(a1, 2)
		coeff1 = (1 - coeff2 - coeff3) / 2
		
		filt = np.zeros(len(src))
		
		for i in range(len(src)):
			if i == 0:
				filt[i] = src.iloc[i]
			elif i == 1:
				filt[i] = coeff1 * (src.iloc[i] + src.iloc[i-1]) + coeff2 * filt[i-1]
			else:
				filt[i] = coeff1 * (src.iloc[i] + src.iloc[i-1]) + coeff2 * filt[i-1] + coeff3 * filt[i-2]
		
		return pd.Series(filt, index=src.index)
	
	# Main calculation
	# Step 1: Calculate log price
	price = np.log(close_series)
	
	# Step 2: Calculate RSI of the log price
	rsi1 = xrsi(price, rsi_length)
	
	# Step 3: Calculate stochastic of the RSI
	rawsig = stochastic(rsi1, rsi1, rsi1, stochastic_length)
	
	# Step 4: Apply Ehlers Super Smoother
	sig = ehlers_super_smoother(rawsig, K)
	
	return sig

@pandas_udf(returnType=DoubleType())
def calculate_ehlers_stoch_rsi_d(close_series: pd.Series) -> pd.Series:
	"""
	Calculate Ehlers Smoothed Stochastic RSI D signal (moving average) with hardcoded default parameters.
	Default parameters: K=10, D=3, rsi_length=14, stochastic_length=14
	"""
	# Hardcoded default parameters
	K = 20
	D = 6
	rsi_length = 70
	stochastic_length = 50
	PI = 3.14159265359
	
	# Helper functions (same as above)
	def xlowest(src, length):
		return src.rolling(window=length, min_periods=1).min()
	
	def xhighest(src, length):
		return src.rolling(window=length, min_periods=1).max()
	
	def xstoch(c, h, l, length):
		xlow = xlowest(l, length)
		xhigh = xhighest(h, length)
		diff = xhigh - xlow
		diff = diff.replace(0, 1e-10)
		return 100 * (c - xlow) / diff
	
	def stochastic(c, h, l, length):
		rawsig = xstoch(c, h, l, length)
		return np.clip(rawsig, 0.0, 100.0)
	
	def xrma(src, length):
		alpha = 1.0 / length
		return src.ewm(alpha=alpha, adjust=False).mean()
	
	def xrsi(src, length):
		delta = src.diff()
		gain = delta.where(delta > 0, 0)
		loss = -delta.where(delta < 0, 0)
		
		avg_gain = xrma(gain, length)
		avg_loss = xrma(loss, length)
		
		avg_loss = avg_loss.replace(0, 1e-10)
		rs = avg_gain / avg_loss
		rsi = 100.0 - (100.0 / (1.0 + rs))
		return rsi
	
	def ehlers_super_smoother(src, lower):
		a1 = np.exp(-PI * np.sqrt(2) / lower)
		coeff2 = 2 * a1 * np.cos(np.sqrt(2) * PI / lower)
		coeff3 = -np.power(a1, 2)
		coeff1 = (1 - coeff2 - coeff3) / 2
		
		filt = np.zeros(len(src))
		
		for i in range(len(src)):
			if i == 0:
				filt[i] = src.iloc[i]
			elif i == 1:
				filt[i] = coeff1 * (src.iloc[i] + src.iloc[i-1]) + coeff2 * filt[i-1]
			else:
				filt[i] = coeff1 * (src.iloc[i] + src.iloc[i-1]) + coeff2 * filt[i-1] + coeff3 * filt[i-2]
		
		return pd.Series(filt, index=src.index)
	
	# Main calculation
	# Step 1: Calculate log price
	price = np.log(close_series)
	
	# Step 2: Calculate RSI of the log price
	rsi1 = xrsi(price, rsi_length)
	
	# Step 3: Calculate stochastic of the RSI
	rawsig = stochastic(rsi1, rsi1, rsi1, stochastic_length)
	
	# Step 4: Apply Ehlers Super Smoother
	sig = ehlers_super_smoother(rawsig, K)
	
	# Step 5: Calculate moving average
	ma = sig.rolling(window=D, min_periods=1).mean()
	
	return ma

# RSI Lagged Features UDFs
@pandas_udf(returnType=DoubleType())
def calculate_rsi_lag1(rsi_series: pd.Series) -> pd.Series:
	"""Calculate RSI lag 1 (shift by 1 period)"""
	return rsi_series.shift(1)

@pandas_udf(returnType=DoubleType())
def calculate_rsi_lag2(rsi_series: pd.Series) -> pd.Series:
	"""Calculate RSI lag 2 (shift by 2 periods)"""
	return rsi_series.shift(2)

@pandas_udf(returnType=DoubleType())
def calculate_rsi_lag3(rsi_series: pd.Series) -> pd.Series:
	"""Calculate RSI lag 3 (shift by 3 periods)"""
	return rsi_series.shift(3)

@pandas_udf(returnType=DoubleType())
def calculate_rsi_pct(rsi_series: pd.Series) -> pd.Series:
	"""
	Calculate RSI percentage rank with 365-bar lookback.
	Returns the percentile rank of current RSI value within the rolling 365-period window.
	"""
	def percentile_rank(x):
		if len(x) == 0 or x.max() == x.min():
			return 0.5
		return (x.iloc[-1] - x.min()) / (x.max() - x.min())
	
	return rsi_series.rolling(window=365, min_periods=1).apply(percentile_rank, raw=False)


## Process data
def process_data(df, product_id, granularity):
    """
    Process the crypto candle data.
    This is a placeholder implementation - replace with your specific logic.
    """
    logger.info(f"Processing {product_id} {granularity} data")
    
    # Store initial row count for validation
    initial_count = df.count()
    logger.info(f"Input data has {initial_count} records")
    
    # Convert start timestamp to proper timestamp type
    # The 'start' column contains Unix timestamps, so we need to convert them properly
    df_processed = df.withColumn(
        "timestamp", 
        to_timestamp(col("start"))
    )
    
    # Validate row count after timestamp conversion
    timestamp_count = df_processed.count()
    if timestamp_count != initial_count:
        logger.warning(f"Row count changed after timestamp conversion: {initial_count} -> {timestamp_count}")
    else:
        logger.info("Row count preserved after timestamp conversion")
    
    # Add time-based features (placeholder transformations)
    df_processed = df_processed.withColumn("year", year("timestamp")) \
                              .withColumn("month", month("timestamp")) \
                              .withColumn("day", dayofmonth("timestamp")) \
                              .withColumn("hour", hour("timestamp"))
    
    # Add minute column for granularities that have it
    if granularity in ["FIFTEEN_MINUTE", "ONE_HOUR"]:
        df_processed = df_processed.withColumn("minute", minute("timestamp"))
    
    # Calculate basic technical indicators (placeholder)
    # You can replace this with your actual processing logic
    df_processed = df_processed.withColumn("rsi", calculate_rsi(col("close"))) \
                       .withColumn("rsi_lag1", calculate_rsi_lag1(col("rsi"))) \
                       .withColumn("rsi_lag2", calculate_rsi_lag2(col("rsi"))) \
                       .withColumn("rsi_lag3", calculate_rsi_lag3(col("rsi"))) \
                       .withColumn("rsi_pct", calculate_rsi_pct(col("rsi"))) \
                       .withColumn("macd", calculate_macd(col("close"))) \
                       .withColumn("macd_signal", calculate_macd_signal(col("close"))) \
                       .withColumn("macd_histogram", calculate_macd_histogram(col("close"))) \
                       .withColumn("atr", calculate_atr(col("high"), col("low"), col("close"))) \
                       .withColumn("sma_14", calculate_sma_14(col("close"))) \
                       .withColumn("sma_50", calculate_sma_50(col("close"))) \
                       .withColumn("sma_138", calculate_sma_138(col("close"))) \
                       .withColumn("kijun_v2_200_1", calculate_kijun_v2_200_1(col("high"), col("low"), col("close"))) \
                       .withColumn("es_stoch_rsi_k", calculate_ehlers_stoch_rsi_k(col("close"))) \
                       .withColumn("es_stoch_rsi_d", calculate_ehlers_stoch_rsi_d(col("close"))) \
    
    # Final row count validation
    final_count = df_processed.count()
    if final_count != initial_count:
        logger.error(f"CRITICAL: Row count mismatch detected!")
        logger.error(f"Input records: {initial_count}")
        logger.error(f"Output records: {final_count}")
        logger.error(f"Difference: {final_count - initial_count}")
        raise ValueError(f"Row count validation failed: {initial_count} input rows -> {final_count} output rows")
    else:
        logger.info(f"✓ Row count validation passed: {final_count} records (same as input)")
    
    logger.info(f"Processing complete. Output will have {final_count} records")
    
    return df_processed


def save_data(df, output_path, output_format="parquet"):
    """Save processed data to output path"""
    logger.info(f"Saving data to: {output_path}")
    
    # Validate row count before saving
    pre_save_count = df.count()
    logger.info(f"Data to be saved contains {pre_save_count} records")
    
    try:
        if output_format.lower() == "parquet":
            # Save without partitioning first to avoid potential partitioning issues
            # Partitioning can sometimes cause row count discrepancies
            df.write \
              .mode("overwrite") \
              .option("compression", "snappy") \
              .parquet(output_path)
            
            # Alternative: If partitioning is required, use single partition column
            # df.write \
            #   .mode("overwrite") \
            #   .partitionBy("year", "month", "day", "hour") \
            #   .option("compression", "snappy") \
            #   .parquet(output_path)
            
        elif output_format.lower() == "csv":
            df.write \
              .mode("overwrite") \
              .option("header", "true") \
              .csv(output_path)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
        
        logger.info(f"Data successfully saved to {output_path}")
        
        # Optional: Validate saved data by reading it back
        # This is commented out to avoid performance impact, but can be enabled for debugging
        # logger.info("Validating saved data...")
        # saved_df = spark.read.parquet(output_path) if output_format.lower() == "parquet" else spark.read.csv(output_path, header=True)
        # saved_count = saved_df.count()
        # if saved_count != pre_save_count:
        #     logger.error(f"CRITICAL: Saved data validation failed!")
        #     logger.error(f"Expected: {pre_save_count}, Found: {saved_count}")
        #     raise ValueError(f"Saved data row count mismatch: {pre_save_count} -> {saved_count}")
        # else:
        #     logger.info(f"✓ Saved data validation passed: {saved_count} records")
        
    except Exception as e:
        logger.error(f"Error saving data: {e}")
        raise


def validate_data_quality(df, stage_name):
    """Validate data quality at different stages"""
    logger.info(f"=== Data Quality Validation: {stage_name} ===")
    
    total_count = df.count()
    logger.info(f"Total records: {total_count}")
    
    # Check for null values in key columns
    null_counts = {}
    key_columns = ["date", "start", "open", "high", "low", "close", "volume"]
    
    for col_name in key_columns:
        if col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            null_counts[col_name] = null_count
            if null_count > 0:
                logger.warning(f"Column '{col_name}' has {null_count} null values")
    
    # Check for duplicate timestamps
    if "start" in df.columns:
        duplicate_timestamps = df.groupBy("start").count().filter(col("count") > 1).count()
        if duplicate_timestamps > 0:
            logger.warning(f"Found {duplicate_timestamps} duplicate timestamps")
    
    logger.info(f"=== End {stage_name} Validation ===")
    return total_count


def main():
    """Main processing function"""
    args = parse_arguments()
    
    # Create Spark session
    spark = create_spark_session("CryptoDataProcessor")
    
    try:
        # Define schema and load data
        schema = define_schema()
        df = load_data(spark, args.input_path, schema)
        
        # Validate input data quality
        input_count = validate_data_quality(df, "INPUT")
        
        # Process data
        df_processed = process_data(df, args.product_id, args.granularity)
        
        # Validate processed data quality
        output_count = validate_data_quality(df_processed, "PROCESSED")
        
        # Final validation
        if input_count != output_count:
            logger.error(f"FINAL VALIDATION FAILED: Input={input_count}, Output={output_count}")
            raise ValueError("Row count mismatch detected in final validation")
        else:
            logger.info(f"✓ FINAL VALIDATION PASSED: {output_count} records preserved")
        
        # Save processed data
        save_data(df_processed, args.output_path)
        
        logger.info("Processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
