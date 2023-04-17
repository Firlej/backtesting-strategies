from datetime import datetime
import os
import json

from binance.client import Client

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

client = Client()

symbol = "BTCUSDT"
start_str = "2017-08-17 04:00:00"
end_str = "2023-04-17 00:00:00"
output_filename = f"data/{symbol}_{start_str}_{end_str}.parquet"

# calculate mins for tqdm progress bar
fmt = "%Y-%m-%d %H:%M:%S"
td = datetime.strptime(end_str, fmt) - datetime.strptime(start_str, fmt)
td_mins = int(round(td.total_seconds() / 60))

# Instantiate data generator
data_generator = client.get_historical_klines_generator(
    symbol=symbol,
    interval=Client.KLINE_INTERVAL_1MINUTE,
    start_str=start_str,
    end_str=end_str
)

def parse_raw_candle(c):
    return {
        "time": c[0],
        "open": float(c[1]),
        "high": float(c[2]),
        "low": float(c[3]),
        "close": float(c[4]),
        "volume": float(c[5]),
        "close_time": c[6],
        "quote_asset_volume": float(c[7]),
        "number_of_trades": int(c[8]),
        "taker_buy_base_asset_volume": float(c[9]),
        "taker_buy_quote_asset_volume": float(c[10]),
    }

success = False

# Try to parse data until it works
# TODO: This is a hacky way to do this, but it works for now
while not success:
    data = []
    try:
        for raw_candle in tqdm(data_generator, desc = "Downloading data from Binance API", total=td_mins): 
            data.append(parse_raw_candle(raw_candle))
        success = True
    except TimeoutError as e:
        print("TimeoutError")
        print(e)
        pass

df = pd.DataFrame(data)
df["time"] = pd.to_datetime(df["time"], unit="ms")
df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")


# Convert DataFrame to Apache Arrow Table
table = pa.Table.from_pandas(df)
# Write data as parquet with Brotli compression (most space efficient type of compression)
pq.write_table(table, output_filename)
