from unittest import TestCase, mock
from os import path
import pandas as pd

from ..base import KrakenConnector, FredConnector

DIRNAME = path.dirname(path.abspath(__file__))


def _read_file(ticker, _):
    file_path = path.join(DIRNAME, f"{ticker}.json")
    with open(file_path, "r") as f:
        return f.read()


def _get_reference_df(ticker):
    return pd.read_csv(path.join(DIRNAME, f"{ticker}_ref.csv"), index_col=0,
                       parse_dates=True)


class KrakenConnectorTest(TestCase):

    def setUp(self):
        self.connector = KrakenConnector()

    def test_build_url(self):
        expected = "https://api.kraken.com/0/public/OHLC?pair=XXBTZUSD" \
                   "&interval=1440"
        assert expected == self.connector._build_url("XXBTZUSD", "B")

    def test_fetch(self):
        self.connector._fetch_raw = mock.MagicMock(side_effect=_read_file)
        ref = _get_reference_df("XXBTZUSD")
        result = self.connector.fetch("XXBTZUSD")
        pd.testing.assert_frame_equal(ref.astype(float), result)


class FredConnectorTest(TestCase):

    def setUp(self):
        self.connector = FredConnector()

    def test_build_url(self):
        expected = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=ICSA"
        assert expected == self.connector._build_url("ICSA")

    def test_fetch(self):
        url = path.join(DIRNAME, "ICSA.csv")
        self.connector._build_url = mock.MagicMock(return_value=url)
        ref = _get_reference_df("ICSA")
        result = self.connector.fetch("ICSA")
        pd.testing.assert_frame_equal(ref, result)
