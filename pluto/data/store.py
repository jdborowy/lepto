import abc
from collections import namedtuple
from os import path
from pathlib import Path
import pandas as pd

StoreRequest = namedtuple("StoreRequest", ("source", "ticker", "freq"))


class DataStore(abc.ABC):

    @abc.abstractmethod
    def save(self, request: StoreRequest, df: pd.DataFrame):
        """save time series"""

    @abc.abstractmethod
    def read(self, request: StoreRequest):
        """read time series"""


class CSVDataStore(DataStore):

    def __init__(self, basedir: str):
        self.basedir = basedir

    def _build_path(self, request: StoreRequest, mkdir=False):
        filedir = path.join(self.basedir, request.source)
        if mkdir:
            Path(filedir).mkdir(parents=True, exist_ok=True)
        filename = f"{request.ticker}_{request.freq or str()}.csv"
        return path.join(filedir, filename)

    def save(self, request: StoreRequest, df: pd.DataFrame):
        df.to_csv(self._build_path(request, mkdir=True), sep=",")

    def read(self, request: StoreRequest):
        return pd.read_csv(self._build_path(request),
                           index_col=0, parse_dates=True)
