import os
import pandas as pd
import numpy as np
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def _rolling_extrema(
		series: pd.Series,
		length: int,
		kind: str = "max",
) -> pd.Series:
		"""
		Helper that returns the rolling *maximum* (or *minimum*) of a series,
		centred on the current bar (i.e. `length` bars to the left **and**
		`length` bars to the right).

		Parameters
		----------
		series : pd.Series
				The series to compute the extrema for (high or low).
		length : int
				Number of bars on each side of the centre bar.
		kind : {"max", "min"}
				Whether to compute a rolling maximum or minimum.

		Returns
		-------
		pd.Series
				Series of the same length as ``series`` where each element contains
				the maximum/minimum of the window ``[i‑length, i+length]``.
				The first ``length`` and last ``length`` values are ``NaN`` because the
				window would be incomplete.
		"""
		if kind == "max":
				rolled = series.rolling(window=2 * length + 1, center=True).max()
		elif kind == "min":
				rolled = series.rolling(window=2 * length + 1, center=True).min()
		else:
				raise ValueError("kind must be 'max' or 'min'")
		return rolled


def _detect_pivots(
		high: pd.Series,
		low: pd.Series,
		length: int,
) -> tuple[pd.Series, pd.Series]:
		"""
		Detect pivot‑highs and pivot‑lows exactly like PineScript's
		``ta.pivothigh(length, length)`` and ``ta.pivotlow(length, length)``.

		Returns
		-------
		pivot_high : pd.Series (bool)
				True on bars that are a pivot‑high.
		pivot_low : pd.Series (bool)
				True on bars that are a pivot‑low.
		"""
		# A bar is a pivot‑high when its high equals the *maximum* of the
		# centred window and the maximum is unique (otherwise a flat top would
		# generate many true values).  The same idea applies to pivot‑low.
		max_center = _rolling_extrema(high, length, kind="max")
		min_center = _rolling_extrema(low, length, kind="min")

		# Equality test – pandas aligns indexes automatically.
		is_max = high == max_center
		is_min = low == min_center

		# Ensure uniqueness: count how many times the max/min appears inside the
		# window.  If it appears more than once we do **not** treat it as a pivot.
		# This mirrors PineScript's behaviour (it only returns the *first* bar of
		# a flat top/bottom as a pivot, the rest are false).
		# We use a rolling count of the equality mask.
		max_count = is_max.rolling(window=2 * length + 1, center=True).sum()
		min_count = is_min.rolling(window=2 * length + 1, center=True).sum()

		pivot_high = is_max & (max_count == 1)
		pivot_low = is_min & (min_count == 1)

		# The first/last `length` bars cannot be pivots because the window is
		# incomplete – set them to False explicitly.
		pivot_high.iloc[:length] = False
		pivot_high.iloc[-length:] = False
		pivot_low.iloc[:length] = False
		pivot_low.iloc[-length:] = False

		return pivot_high, pivot_low


def _classify_swings(
		pivot_high: pd.Series,
		pivot_low: pd.Series,
		high: pd.Series,
		low: pd.Series,
) -> pd.Series:
		"""
		Walk through the series and label each swing as HH/LH/HL/LL.
		The algorithm follows the PineScript logic:

		* When a new pivot‑high appears we compare it with the *previous* pivot‑high
			(stored in ``prev_ph``).  If the new high is higher → “HH”, otherwise “LH”.
		* When a new pivot‑low appears we compare it with the *previous* pivot‑low
			(stored in ``prev_pl``).  If the new low is lower → “LL”, otherwise “HL”.

		The result is a Series of the same length as the input where only the
		pivot bars contain a label, all other rows are ``np.nan``.
		"""
		swing_label = pd.Series(data=np.nan, index=high.index, dtype=object)

		# Keep track of the last swing‑high / swing‑low values.
		prev_ph_val = np.nan
		prev_pl_val = np.nan

		# Iterate **once** over the index – this is still O(N) but far cheaper
		# than a double‑nested loop.
		for idx in high.index:
				if pivot_high.loc[idx]:
						cur_val = high.loc[idx]
						if np.isnan(prev_ph_val):
								# First swing‑high – we cannot decide HH/LH yet, keep NaN.
								swing_label.at[idx] = np.nan
						else:
								swing_label.at[idx] = "HH" if cur_val > prev_ph_val else "LH"
						prev_ph_val = cur_val

				elif pivot_low.loc[idx]:
						cur_val = low.loc[idx]
						if np.isnan(prev_pl_val):
								swing_label.at[idx] = np.nan
						else:
								swing_label.at[idx] = "LL" if cur_val < prev_pl_val else "HL"
						prev_pl_val = cur_val

		return swing_label


def add_swing_columns(
		df: pd.DataFrame,
		length: int = 5,
		high_col: str = "high",
		low_col: str = "low",
		open_col: str = "open",
		close_col: str = "close",
) -> pd.DataFrame:
		"""
		Add three columns to ``df`` that contain swing‑high / swing‑low information.

		Parameters
		----------
		df : pd.DataFrame
				Must contain at least the columns given by ``high_col`` and ``low_col``.
		length : int, default 5
				Number of bars on each side of the centre bar that define a pivot.
				This is the same as the PineScript input ``length``.
		high_col, low_col, open_col, close_col : str, optional
				Column names in the DataFrame.  They default to the conventional
				lowercase names used by most CSV exports.

		Returns
		-------
		pd.DataFrame
				The original DataFrame with three extra columns:
				* ``swing_high`` – bool
				* ``swing_low``  – bool
				* ``swing_label`` – object (HH, LH, HL, LL or NaN)

		Notes
		-----
		The function **does not modify** the input DataFrame in‑place; a copy is
		returned.  If you want to work in‑place, assign the result back:
		``df = add_swing_columns(df, length=5)``.
		"""
		# ------------------------------------------------------------------ #
		# 1️⃣  Basic validation
		# ------------------------------------------------------------------ #
		required = {high_col, low_col, open_col, close_col}
		missing = required.difference(df.columns)
		if missing:
				raise ValueError(f"The DataFrame is missing required columns: {missing}")

		# Work on a copy to avoid side‑effects.
		out = df.copy()

		# ------------------------------------------------------------------ #
		# 2️⃣  Detect pivots
		# ------------------------------------------------------------------ #
		ph, pl = _detect_pivots(
				high=out[high_col],
				low=out[low_col],
				length=length,
		)
		out["swing_high"] = ph
		out["swing_low"] = pl

		# ------------------------------------------------------------------ #
		# 3️⃣  Classify swings (HH/LH/HL/LL)
		# ------------------------------------------------------------------ #
		out["swing_label"] = _classify_swings(
				pivot_high=ph,
				pivot_low=pl,
				high=out[high_col],
				low=out[low_col],
		)

		return out

# The divergence methods below assume you have added the swing columns using the add_swing_columns function above.

def regular_div(df, span, bullish=True, min_distance=10):
		"""
		Detect regular divergence (trend reversal signal).
		"""
		mask = pd.Series(False, index=df.index)
		# Filter swings by type: bullish looks for swing lows (-1), bearish looks for swing highs (1)
		swing_type = -1 if bullish else 1
		swings_ = df[df['swing'] == swing_type]
		
		# Early return if no swings or only one swing
		if len(swings_) <= 1:
				return mask
		
		# Pre-extract columns and indices for faster access
		swing_indices = swings_.index.values
		if bullish:
				swing_prices = swings_['low'].values
		else:
				swing_prices = swings_['high'].values
		swing_rsi = swings_['rsi'].values
		
		# Compare each swing with previous swings of the same type
		# Search backwards from most recent to optimize for early termination
		for i in range(1, len(swings_)):
				p2_idx = swing_indices[i]
				p2_price = swing_prices[i]
				r2 = swing_rsi[i]
				
				# Search backwards through previous swings
				# Stop when distance < min_distance (too close) or when valid divergence found
				for j in range(i - 1, -1, -1):
						p1_idx = swing_indices[j]
						swing_distance = p2_idx - p1_idx
						
						# Early termination: if too close, all earlier swings will be even closer
						if swing_distance < min_distance:
								break
						
						# Skip if too far (beyond span)
						if swing_distance >= span:
								continue
						
						# Both swings are within valid distance window, compare price and RSI
						p1_price = swing_prices[j]
						r1 = swing_rsi[j]
						
						if bullish:
								# Regular bullish divergence: lower low but higher RSI
								cond = (p2_price < p1_price) and (r2 > r1)
						else:
								# Regular bearish divergence: higher high but lower RSI
								cond = (p2_price > p1_price) and (r2 < r1)
						
						if cond:
								mask.loc[p2_idx] = True
								break  # Found a valid divergence, no need to check more previous swings
		
		return mask

def hidden_div(df, span, bullish=True, min_distance=150):
		"""
		Detect hidden divergence (trend continuation signal).
		"""
		mask = pd.Series(False, index=df.index)
		# Filter swings by type: bullish looks for swing lows (-1), bearish looks for swing highs (1)
		swing_type = -1 if bullish else 1
		swings_ = df[df['swing'] == swing_type]
		
		# Early return if no swings or only one swing
		if len(swings_) <= 1:
				return mask
		
		# Pre-extract columns and indices for faster access
		swing_indices = swings_.index.values
		if bullish:
				swing_prices = swings_['low'].values
		else:
				swing_prices = swings_['high'].values
		swing_rsi = swings_['rsi'].values
		
		# Compare each swing with previous swings of the same type
		# Search backwards from most recent to optimize for early termination
		for i in range(1, len(swings_)):
				p2_idx = swing_indices[i]
				p2_price = swing_prices[i]
				r2 = swing_rsi[i]
				
				# Search backwards through previous swings
				# Stop when distance < min_distance (too close) or when valid divergence found
				for j in range(i - 1, -1, -1):
						p1_idx = swing_indices[j]
						swing_distance = p2_idx - p1_idx
						
						# Early termination: if too close, all earlier swings will be even closer
						if swing_distance < min_distance:
								break
						
						# Skip if too far (beyond span)
						if swing_distance >= span:
								continue
						
						# Both swings are within valid distance window, compare price and RSI
						p1_price = swing_prices[j]
						r1 = swing_rsi[j]
						
						if bullish:
								# Hidden bullish divergence: higher low but lower RSI
								cond = (p2_price > p1_price) and (r2 < r1)
						else:
								# Hidden bearish divergence: lower high but higher RSI
								cond = (p2_price < p1_price) and (r2 > r1)
						
						if cond:
								mask.loc[p2_idx] = True
								break  # Found a valid divergence, no need to check more previous swings
		
		return mask

def add_risk_target(df: pd.DataFrame,
										lookback_div=500,
										lookback_rev=200,
										atr_q=0.8,
										rsi_ob=70,
										rsi_os=30,
										stoch_ob=80,
										stoch_os=20) -> pd.DataFrame:
		"""
		Adds column 'risk_score' to the dataframe.
		Expects columns:
				['close','high','low','rsi','rsi_lag1','rsi_lag2','rsi_lag3',
					'rsi_pct','macd','atr','sma_14','sma_50','sma_138','kijun_v2_200_1',
					'es_stoch_rsi_k','es_stoch_rsi_d']
		"""
		logger.info(f"Adding risk target to {len(df)} records")

		df = df.copy().sort_values('timestamp').reset_index(drop=True)

		# 1. volatility risk
		atr_high = df['atr'].quantile(atr_q)
		vol_risk = (df['atr'] >= atr_high).astype(int) * 2

		# 2. rsi_ob/os risk
		rsi_risk = pd.Series(0, index=df.index)
		rsi_risk = rsi_risk.where(~((df['rsi'] > rsi_ob) | (df['rsi'] < rsi_os)), 2)

		# 3. stochastic not in ob/os
		stoch_mid = (df['es_stoch_rsi_k'].between(stoch_os, stoch_ob)) & \
								(df['es_stoch_rsi_d'].between(stoch_os, stoch_ob))
		stoch_risk = stoch_mid.astype(int) * 1

		# 4. ma-divergence risk
		ma_span = df[['sma_14','sma_50','sma_138']]
		ma_diff = ma_span.max(axis=1) - ma_span.min(axis=1)
		ma_risk = (ma_diff > ma_diff.quantile(0.8)).astype(int) * 2

		# 5. swing detection helpers
		df_w_swings = add_swing_columns(df, length=21)
		swing = pd.Series(0, index=df_w_swings.index, dtype=int)
		swing.loc[df_w_swings['swing_high']] = 1
		swing.loc[df_w_swings['swing_low']] = -1
		df['swing'] = swing

		# 6. hidden divergence (trend continuation) – low risk
		logger.info(f"Adding hidden divergence to {len(df)} records")
		hidden_bull = hidden_div(df, span=lookback_div, bullish=True, min_distance=200)
		hidden_bear = hidden_div(df, span=lookback_div, bullish=False, min_distance=200)

		# 7. regular divergence (trend reversal) – medium risk
		logger.info(f"Adding regular divergence to {len(df)} records")
		regular_bull = regular_div(df, span=lookback_rev, bullish=True, min_distance=30)
		regular_bear = regular_div(df, span=lookback_rev, bullish=False, min_distance=30)

		# 8. kijun baseline pull-back – low risk
		above_kijun = df['close'] > df['kijun_v2_200_1']
		below_kijun = df['close'] < df['kijun_v2_200_1']
		sma14, sma50 = df['sma_14'], df['sma_50']
		# bearish cross followed by bullish cross while above kijun
		cross_dn = (sma14 < sma50) & (sma14.shift() >= sma50.shift())
		cross_up = (sma14 > sma50) & (sma14.shift() <= sma50.shift())
		kijun_bull = above_kijun & cross_up & cross_dn.rolling(20).max().astype(bool)
		kijun_bear = below_kijun & cross_dn & cross_up.rolling(20).min().astype(bool)

		# 9. assemble risk score
		risk = pd.Series(0, index=df.index)

		# very low risk
		risk = risk.mask(hidden_bull & ~vol_risk.astype(bool), -1)
		risk = risk.mask(hidden_bear & ~vol_risk.astype(bool), -1)
		risk = risk.mask(kijun_bull & ~vol_risk.astype(bool), -1)
		risk = risk.mask(kijun_bear & ~vol_risk.astype(bool), -1)
		
		# low risk
		risk = risk.mask(regular_bull, 1)
		risk = risk.mask(regular_bear, 1)

		# high risk
		risk = np.maximum(risk, vol_risk)
		risk = np.maximum(risk, rsi_risk)
		risk = np.maximum(risk, stoch_risk)
		risk = np.maximum(risk, ma_risk)

		# very high risk: overlap of multiple flags
		very_high = (vol_risk>=2) & (rsi_risk>=2) & (stoch_risk>=1) & (ma_risk>=2)
		risk = risk.mask(very_high, 4)

		df['risk_score'] = risk.clip(0, 4).astype(int)
		df['hidden_bull'] = hidden_bull
		df['hidden_bear'] = hidden_bear
		df['bull_div'] = regular_bull
		df['bear_div'] = regular_bear

		return df


def read_parquet_tail(filepath: str, n_rows: int) -> pd.DataFrame:
    """
    Read the last n_rows from a parquet file. For small files (<= n_rows) reads fully.
    For larger files uses pyarrow to read only the last row groups to bound memory.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Input file not found: {filepath}")
    try:
        import pyarrow.parquet as pq
    except ImportError:
        # Fallback: read full and tail (memory = full file)
        df = pd.read_parquet(filepath)
        return df.tail(n_rows) if len(df) > n_rows else df

    pf = pq.ParquetFile(filepath)
    total_rows = pf.metadata.num_rows
    if total_rows <= n_rows:
        return pf.read().to_pandas()
    # Read only the last row groups that contain at least n_rows
    rows_needed = n_rows
    row_groups_to_read = []
    for i in range(pf.metadata.num_row_groups - 1, -1, -1):
        row_groups_to_read.append(i)
        rows_in_groups = sum(pf.metadata.row_group(rg).num_rows for rg in row_groups_to_read)
        if rows_in_groups >= n_rows:
            break
    row_groups_to_read.reverse()
    table = pf.read_row_groups(row_groups_to_read)
    df = table.to_pandas()
    return df.tail(n_rows)


def calculate_risk_target_and_save_tail_only(
    product_id: str,
    granularity: str,
    input_file: str,
    output_file: str,
    tail_rows: int = 2500,
) -> str:
    """
    Read only the last tail_rows from the indicators parquet, add risk target, overwrite output.
    Memory-bounded when used with tail-only indicators output (small parquet).
    """
    logger.info(f"Starting tail-only risk target calculation for {product_id} {granularity}")
    df = read_parquet_tail(input_file, tail_rows)
    logger.info(f"Loaded last {len(df)} records from {input_file}")
    # Same validation as calculate_risk_target_and_save
    required_base = ["date", "start", "open", "high", "low", "close", "volume"]
    required_tech = [
        "rsi", "rsi_lag1", "rsi_lag2", "rsi_lag3", "rsi_pct",
        "macd", "atr", "sma_14", "sma_50", "sma_138", "kijun_v2_200_1",
        "es_stoch_rsi_k", "es_stoch_rsi_d",
    ]
    for col in required_base + required_tech:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
    df_processed = add_risk_target(df)
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    df_processed.to_parquet(output_file, compression="snappy", index=False)
    logger.info(f"✓ Saved {len(df_processed)} records to {output_file}")
    return output_file


def calculate_risk_target_and_save(product_id: str, granularity: str, input_file: str, output_file: str) -> str:
    """
    Load parquet data (with technical indicators), calculate risk target, and save to parquet.
    
    Args:
        product_id: Product identifier (e.g., BTC-USD)
        granularity: Time granularity (e.g., ONE_DAY)
        input_file: Path to input parquet file (from technical indicators task)
        output_file: Path for output parquet file
        
    Returns:
        Path to output file
    """
    logger.info(f"Starting risk target calculation for {product_id} {granularity}")
    logger.info(f"Input file: {input_file}")
    logger.info(f"Output file: {output_file}")
    
    # Validate input file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Load data from parquet (should already have technical indicators)
    logger.info(f"Loading data from: {input_file}")
    df = pd.read_parquet(input_file)
    logger.info(f"Loaded {len(df)} records")
    
    # Validate required base columns
    required_base_columns = ['date', 'start', 'open', 'high', 'low', 'close', 'volume']
    missing_base = [col for col in required_base_columns if col not in df.columns]
    if missing_base:
        raise ValueError(f"Missing required base columns: {missing_base}")
    
    # Validate required technical indicator columns (needed for risk target calculation)
    required_tech_columns = [
        'rsi', 'rsi_lag1', 'rsi_lag2', 'rsi_lag3', 'rsi_pct',
        'macd', 'atr', 'sma_14', 'sma_50', 'sma_138', 'kijun_v2_200_1',
        'es_stoch_rsi_k', 'es_stoch_rsi_d'
    ]
    missing_tech = [col for col in required_tech_columns if col not in df.columns]
    if missing_tech:
        raise ValueError(f"Missing required technical indicator columns: {missing_tech}. "
                        f"Please ensure technical indicators are calculated first.")
    
    # Process risk target
    df_processed = add_risk_target(df)
    
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

def main():
		calculate_risk_target_and_save(
				product_id="BTC-USD",
				granularity="FIFTEEN_MINUTE",
				input_file="/Users/marcus/ML_Projects/Algo_Trading/processed_data/BTC_USD_fifteen_minute.parquet",
				output_file="/Users/marcus/ML_Projects/Algo_Trading/processed_data/BTC_USD_fifteen_minute.parquet"
		)

if __name__ == "__main__":
  	main()