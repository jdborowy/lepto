from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor

from .store import DataStore, StoreRequest
from .connector import Connector

FetcherRequest = namedtuple("DataRequest", ("source", "ticker", "freq", "start"))


class DataFetcher:

    def __init__(self, store: DataStore, max_workers=16):
        self.store = store
        self.max_workers = max_workers

    def fetch_one(self, request: FetcherRequest):
        connector = Connector.from_name(request.source)
        df = connector.fetch(request.ticker, request.freq, request.start)
        self.store.save(request, df)

    def fetch(self, requests):
        def _fetch_one_noexcept(request):
            try:
                self.fetch_one(request)
            except Exception as e:
                return e

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = executor.map(_fetch_one_noexcept, requests)

        return {req: res for req, res in zip(requests, results)
                if res is not None}
