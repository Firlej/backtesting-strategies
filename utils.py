
symbols = [
    'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'XRP/USDT', 'ADA/USDT',
    'ETH/BTC', 'BNB/BTC', 'XRP/BTC', 'ADA/BTC'
]

def get_ohlcv_filename(symbol: str, timeframe: str):
    return f"data/ohlcv_{symbol.replace('/', '_')}_{timeframe}.pkl"

def get_SMA_filename(symbol, start, stop, step, sample_size):
    return f"data/SMA_{symbol.replace('/', '_')}_{start}_{stop}_{step}_{sample_size}.pkl"