from unittest import TestCase, mock
from os import path

from .. import connector
from ..fetcher import DataFetcher, FetcherRequest

DIRNAME = path.dirname(path.abspath(__file__))


class DataFetcherTest(TestCase):

    def setUp(self):
        self.store_mock = mock.MagicMock()
        self.fetcher = DataFetcher(self.store_mock)

    @mock.patch(f"{connector.__name__}.FredConnector._build_url",
                return_value=path.join(DIRNAME, "fred", "ICSA_.csv"))
    def test_fetch_one(self, connector_mock):
        self.fetcher.fetch_one(FetcherRequest("fred", "ICSA", None, None))
        connector_mock.assert_called_once()
        self.store_mock.save.assert_called_once()

    @mock.patch(f"{connector.__name__}.FredConnector._build_url",
                return_value=path.join(DIRNAME, "fred", "ICSA_.csv"))
    def test_fetch(self, _):
        result = self.fetcher.fetch([
            FetcherRequest("fred", "ICSA", None, None),
            FetcherRequest("fred", "ICSA", None, None),
            FetcherRequest("AAA", "AAA", None, None)
        ])
        expected_errors = [
            FetcherRequest("AAA", "AAA", None, None)
        ]
        assert 2 == self.store_mock.save.call_count
        assert expected_errors == list(result.keys())
