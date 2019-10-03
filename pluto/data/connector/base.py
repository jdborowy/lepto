import abc

import pandas as pd
import json
import urllib.request as urlrequest


class Connector(abc.ABC):

    @classmethod
    def from_name(cls, name):
        for connector_class in cls.__subclasses__():
            if connector_class.name() == name:
                return connector_class()

    @abc.abstractmethod
    def fetch(self, ticker, freq, start):
        """fetch data"""


class KrakenConnector(Connector):

    def __init__(self):
        self.columns = ["time", "open", "high", "low", "close", "vwap",
                        "volume", "count"]
        self.intervals = {"B": 1440, "D": 1440}

    @classmethod
    def name(cls):
        return "kraken"

    def _build_url(self, ticker, freq=None):
        return f"https://api.kraken.com/0/public/OHLC?pair={ticker}"\
               f"&interval={self.intervals.get(freq, 1440)}"

    def _fetch_raw(self, ticker, freq=None):
        return urlrequest.urlopen(self._build_url(ticker, freq)).read()

    def fetch(self, ticker, freq=None, start=None) -> pd.DataFrame:
        data_dict = json.loads(self._fetch_raw(ticker, freq))
        df = pd.DataFrame(data_dict["result"][ticker], columns=self.columns)
        df.set_index("time", inplace=True)
        df.index = pd.to_datetime(df.index, unit="s")
        return df.astype(float)


class FredConnector(Connector):

    @classmethod
    def name(cls):
        return "fred"

    def _build_url(self, ticker):
        return f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={ticker}"

    def fetch(self, ticker, freq=None, start=None) -> pd.DataFrame:
        return pd.read_csv(self._build_url(ticker),
                           index_col=0, parse_dates=True)
