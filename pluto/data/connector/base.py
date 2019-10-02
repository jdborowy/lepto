import abc

import pandas as pd
import json
import urllib.request as urlrequest


class Connector(abc.ABC):

    @abc.abstractmethod
    def fetch(self, ticker, freq, start):
        """fetch data"""


class KrakenOHLC(Connector):

    def __init__(self):
        self.columns = ["time", "open", "high", "low", "close", "vwap",
                        "volume", "count"]
        self.intervals = {"B": 1440, "D": 1440}

    def _fetch_raw(self, ticker: str, freq=None):
        url = f"https://api.kraken.com/0/public/OHLC?pair={ticker}"\
              f"&interval={self.intervals.get(freq, 1440)}"
        return urlrequest.urlopen(url).read()

    def fetch(self, ticker: str, freq=None, start=None) -> pd.DataFrame:
        data_dict = json.loads(self._fetch_raw(ticker, freq))
        df = pd.DataFrame(data_dict["result"][ticker], columns=self.columns)
        df.set_index("time", inplace=True)
        df = df.astype(float)
        df.index = pd.to_datetime(df.index, unit="s")
        return df
