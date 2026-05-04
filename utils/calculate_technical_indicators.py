"""
Calculate technical indicators for crypto candle data.
This module processes CSV files and adds technical indicator columns using pandas and talib.
"""

import os
import pandas as pd
import numpy as np
import talib
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Engulfing patterns: allow current open to sit within this fractional distance of the
# previous close when the strict textbook rule would reject a visually similar bar.
# Example: 0.0005 means 0.05% — bullish allows open slightly above prev_close; bearish
# allows open slightly below prev_close. Change this single value to tune sensitivity.
ENGULFING_OPEN_TOLERANCE = 0.0005


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
    
    return pd.Series(kijunv2, index=close_series.index)


def calculate_ehlers_stoch_rsi_k(close_series: pd.Series) -> pd.Series:
    """
    Calculate Ehlers Smoothed Stochastic RSI K signal with hardcoded default parameters.
    Default parameters: K=20, D=6, rsi_length=70, stochastic_length=50
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


def calculate_ehlers_stoch_rsi_d(close_series: pd.Series) -> pd.Series:
    """
    Calculate Ehlers Smoothed Stochastic RSI D signal (moving average) with hardcoded default parameters.
    Default parameters: K=20, D=6, rsi_length=70, stochastic_length=50
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


def process_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add all technical indicators to the dataframe.
    
    Args:
        df: DataFrame with columns: date, start, open, high, low, close, volume
        
    Returns:
        DataFrame with technical indicators added
    """
    logger.info(f"Processing technical indicators for {len(df)} records")
    
    # Store initial row count for validation
    initial_count = len(df)
    logger.info(f"Input data has {initial_count} records")
    
    # Convert start timestamp to datetime if it's not already
    if 'timestamp' not in df.columns:
        df['timestamp'] = pd.to_datetime(df['start'], unit='s', )
    else:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Add time-based features
    df['year'] = df['timestamp'].dt.year
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    df['hour'] = df['timestamp'].dt.hour
    df['minute'] = df['timestamp'].dt.minute
    
    # Ensure data is sorted by timestamp
    df = df.sort_values('timestamp').reset_index(drop=True)

    # Calculate topping/bottoming tail candle patterns
    # Topping tail:
    #   1) bullish candle with upper wick > 50% of range, or
    #   2) bearish candle that makes a higher high than previous candle and
    #      has upper wick (high - open) > 50% of range
    # Bottoming tail:
    #   1) bearish candle with lower wick > 50% of range, or
    #   2) bullish candle that makes a lower low than previous candle and
    #      has lower wick (open - low) > 50% of range
    logger.info("Calculating topping/bottoming tail candle patterns...")
    candle_range = df['high'] - df['low']
    prev_high = df['high'].shift(1)
    prev_low = df['low'].shift(1)
    is_bullish = df['close'] > df['open']
    is_bearish = df['close'] < df['open']
    bullish_upper_wick = df['high'] - df['close']
    bearish_upper_wick = df['high'] - df['open']
    bearish_lower_wick = df['close'] - df['low']
    bullish_lower_wick = df['open'] - df['low']

    df['topping_tail'] = np.where(
        (
            (is_bullish & (bullish_upper_wick > 0.5 * candle_range)) |
            (is_bearish & (df['high'] > prev_high) & (bearish_upper_wick > 0.5 * candle_range))
        ),
        1,
        0
    )
    df['bottoming_tail'] = np.where(
        (
            (is_bearish & (bearish_lower_wick > 0.5 * candle_range)) |
            (is_bullish & (df['low'] < prev_low) & (bullish_lower_wick > 0.5 * candle_range))
        ),
        1,
        0
    )

    # Calculate bullish/bearish engulfing patterns
    # Bullish engulfing:
    # 1) close is greater than the open
    # 2) open is at or below previous close, or within ENGULFING_OPEN_TOLERANCE above it
    # 3) close is greater than the previous open
    # 4) previous candle is bearish
    # Bearish engulfing:
    # 1) close is less than the open
    # 2) open is at or above previous close, or within ENGULFING_OPEN_TOLERANCE below it
    # 3) close is less than the previous open
    # 4) previous candle is bullish
    logger.info("Calculating bullish/bearish engulfing candle patterns...")
    prev_close = df['close'].shift(1)
    prev_open = df['open'].shift(1)
    prev_is_bearish = df['close'].shift(1) < df['open'].shift(1)
    prev_is_bullish = df['close'].shift(1) > df['open'].shift(1)
    bullish_open_ok = df['open'] <= prev_close * (1.0 + ENGULFING_OPEN_TOLERANCE)
    bearish_open_ok = df['open'] >= prev_close * (1.0 - ENGULFING_OPEN_TOLERANCE)
    df['bullish_engulfing'] = np.where(
        (is_bullish & bullish_open_ok & (df['close'] > prev_open) & prev_is_bearish),
        1,
        0
    )
    df['bearish_engulfing'] = np.where(
        (is_bearish & bearish_open_ok & (df['close'] < prev_open) & prev_is_bullish),
        1,
        0
    )

    # Calculate RSI first (needed for lagged features)
    logger.info("Calculating RSI...")
    df['rsi'] = talib.RSI(df['close'].values, timeperiod=14)
    df['rsi_ma_14'] = talib.SMA(df['rsi'].values, timeperiod=14)
    
    # Calculate RSI lagged features
    logger.info("Calculating RSI lagged features...")
    df['rsi_lag1'] = df['rsi'].shift(1)
    df['rsi_lag2'] = df['rsi'].shift(2)
    df['rsi_lag3'] = df['rsi'].shift(3)
    df['rsi_pct'] = calculate_rsi_pct(df['rsi'])
    
    # Calculate MACD
    logger.info("Calculating MACD...")
    macd, signal, histogram = talib.MACDEXT(
        df['close'].values,
        fastperiod=12,
        fastmatype=0,
        slowperiod=26,
        slowmatype=0,
        signalperiod=9,
        signalmatype=0
    )
    df['macd'] = macd
    df['macd_signal'] = signal
    df['macd_histogram'] = histogram
    
    # Calculate ATR
    logger.info("Calculating ATR...")
    df['atr'] = talib.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)
    
    # Calculate SMAs
    logger.info("Calculating SMAs...")
    df['sma_14'] = talib.SMA(df['close'].values, timeperiod=14)
    df['sma_50'] = talib.SMA(df['close'].values, timeperiod=50)
    df['sma_138'] = talib.SMA(df['close'].values, timeperiod=138)
    
    # Calculate EMA
    logger.info("Calculating EMAs...")
    df['ema_20'] = talib.EMA(df['close'].values, timeperiod=20)
    df['ema_50'] = talib.EMA(df['close'].values, timeperiod=50)
    
    # Calculate Kijun V2
    logger.info("Calculating Kijun V2...")
    df['kijun_v2_200_1'] = calculate_kijun_v2_200_1(df['high'], df['low'], df['close'])
    
    # Calculate Ehlers Stochastic RSI
    logger.info("Calculating Ehlers Stochastic RSI...")
    df['es_stoch_rsi_k'] = calculate_ehlers_stoch_rsi_k(df['close'])
    df['es_stoch_rsi_d'] = calculate_ehlers_stoch_rsi_d(df['close'])
    
    # Final row count validation
    final_count = len(df)
    if final_count != initial_count:
        logger.error(f"CRITICAL: Row count mismatch detected!")
        logger.error(f"Input records: {initial_count}")
        logger.error(f"Output records: {final_count}")
        logger.error(f"Difference: {final_count - initial_count}")
        raise ValueError(f"Row count validation failed: {initial_count} input rows -> {final_count} output rows")
    else:
        logger.info(f"✓ Row count validation passed: {final_count} records (same as input)")
    
    logger.info(f"Processing complete. Output will have {final_count} records")
    
    return df


def calculate_and_save_indicators(product_id: str, granularity: str, input_file: str, output_file: str) -> str:
    """
    Load CSV data, calculate technical indicators, and save to parquet.
    
    Args:
        product_id: Product identifier (e.g., BTC-USD)
        granularity: Time granularity (e.g., ONE_DAY)
        input_file: Path to input CSV file
        output_file: Path for output parquet file
        
    Returns:
        Path to output file
    """
    logger.info(f"Starting technical indicator calculation for {product_id} {granularity}")
    logger.info(f"Input file: {input_file}")
    logger.info(f"Output file: {output_file}")
    
    # Validate input file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Load data
    logger.info(f"Loading data from: {input_file}")
    df = pd.read_csv(input_file)
    logger.info(f"Loaded {len(df)} records")
    
    # Validate required columns
    required_columns = ['date', 'start', 'open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Process technical indicators
    df_processed = process_technical_indicators(df)
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Created output directory: {output_dir}")
    
    # Save to parquet
    logger.info(f"Saving processed data to: {output_file}")
    df_processed.to_parquet(output_file, compression='snappy', index=False)
    
    # Validate saved file
    if not os.path.exists(output_file):
        raise IOError(f"Failed to save output file: {output_file}")
    
    file_size = os.path.getsize(output_file) / (1024 * 1024)  # Size in MB
    logger.info(f"✓ Successfully saved {len(df_processed)} records to {output_file} ({file_size:.2f} MB)")
    
    return output_file

