import os
import time
import random
import itertools
import multiprocessing

from tqdm import tqdm
import ccxt
import pandas as pd

from ta.utils import dropna

from settings import DOWNLOAD_FOLDER, symbols, timeframes

class OHLCV:
    def __init__(self, exchange: ccxt.Exchange, symbol: str, timeframe: str, update: bool = False):
        """OHLCV data handler.
        
        Args:
            exchange (ccxt.Exchange): ccxt exchange object.
            symbol (str): symbol to fetch.
            timeframe (str): timeframe to fetch.
            update (bool, optional): whether to update the data. Defaults to False. 
        """
        
        self.exchange = exchange
        self.symbol = symbol
        self.timeframe = timeframe
        
        self.filename = f"ohlcv_{self.symbol.replace('/', '_')}_{self.timeframe}.pkl"
        self.filepath = os.path.join(DOWNLOAD_FOLDER, self.filename)

        if update:
            self.update()

    def __fetch_ohlcv(self, since: int = 0) -> list:
        
        sleep_timer = 10
        while True:
            try:
                return self.exchange.fetch_ohlcv(
                    symbol=self.symbol, 
                    timeframe=self.timeframe,
                    since=since,
                    limit=1000
                )
            except ccxt.NetworkError:
                time.sleep(random.randint(1, sleep_timer))
                sleep_timer += 10
                continue
    
    def __fetch_candles(self, since: int = 0) -> list:
        """Fetches candles from exchange.
        
        Args:
            since (int, optional): timestamp to start fetching from. Defaults to 0.
        """
    
        candles = []
        
        with tqdm(desc = f"Fetching {self.symbol:>9} {self.timeframe:>3} candles") as pbar:

            while True:

                candles_new = self.__fetch_ohlcv(since=since)
                
                if len(candles_new) == 0:
                    pbar.close()
                    # drop last candle since it is not complete
                    return candles[:-1]
                
                candles += candles_new

                since = candles[-1][0] + 1
                
                pbar.update(len(candles_new))
            
    def __parse_candles(self, candles: list) -> pd.DataFrame:
        """Parses candles into a pandas DataFrame.
        
        Args:
            candles (list): list of candles.
        """
        
        columns = ["timestamp", "open", "high", "low", "close", "volume"]
        df = pd.DataFrame(candles, columns = columns)
        
        return df
        
    def update(self):
        """Updates the raw data file."""
        
        try:
            
            df = pd.read_pickle(self.filepath)
            
            # print(f"File {filepath} found. Updating...")
            since = df["timestamp"].iloc[-1] + 1
            
            candles = self.__fetch_candles(since)
            df_new = self.__parse_candles(candles)
            if df_new.shape[0] > 0:
                df = pd.concat([df, df_new], ignore_index = True)
                df.to_pickle(self.filepath)
            
        except FileNotFoundError:
            
            # print(f"File {filepath} not found. Downloading...")
            candles = self.__fetch_candles()
            df = self.__parse_candles(candles)
            df.to_pickle(self.filepath)
    
    @staticmethod
    def __clean(df: pd.DataFrame) -> pd.DataFrame:
        
        df = dropna(df)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index('timestamp', inplace=True)
        
        return df
    
    def get_data(self, update: bool = False) -> pd.DataFrame:
        """Returns the cleaned data.
        
        Args:
            update (bool, optional): whether to update the data. Defaults to False.
            
        Returns:
            pd.DataFrame: cleaned data.
        """
        
        if update:
            self.update()
        elif not os.path.isfile(self.filepath):
            print(f"File {self.filepath} not found. Downloading...")
            self.update()
        
        df = pd.read_pickle(self.filepath)
        df = self.__clean(df)
        
        return df

if __name__ == "__main__":
    
    exchange = ccxt.binance()

    args = list(itertools.product([exchange], symbols, timeframes, [True]))
    
    with multiprocessing.Pool(len(args)) as pool:
        pool.starmap(OHLCV, args)
