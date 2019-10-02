from unittest import TestCase, mock
from os import path
import pandas as pd

from ..base import KrakenOHLC

DIRNAME = path.dirname(path.abspath(__file__))


def _read_file(ticker):
    file_path = path.join(DIRNAME, f"{ticker}.json")
    with open(file_path, "r") as f:
        return f.read()


class KrakenOHLCTest(TestCase):

    def test_fetch(self):
        kraken_connector = KrakenOHLC()
        kraken_connector._fetch_raw = mock.MagicMock(side_effect=_read_file)
        ref = pd.read_csv(path.join(DIRNAME, "XXBTZUSD_ref.csv"), index_col=0,
                          parse_dates=True)
        result = kraken_connector.fetch("XXBTZUSD")
        pd.testing.assert_frame_equal(ref.astype(float), result)
