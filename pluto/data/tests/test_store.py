from unittest import TestCase
from datetime import datetime
from os import path
import shutil

import pandas as pd

from ..store import CSVDataStore, StoreRequest

DIRNAME = path.dirname(path.abspath(__file__))


class CSVDataStoreTest(TestCase):

    def setUp(self):
        self.dirname = f"/tmp/{datetime.now().timestamp()}"
        self.store = CSVDataStore(self.dirname)
        self.ref = pd.read_csv(path.join(DIRNAME, "fred", "ICSA_.csv"),
                               index_col=0, parse_dates=True)

    def test_build_path(self):
        request = StoreRequest("kraken", "XXBTZUSD", None)
        expected = path.join(self.dirname, "kraken/XXBTZUSD_.csv")
        assert expected == self.store._build_path(request, mkdir=True)
        assert path.exists(path.join(self.dirname, "kraken"))

    def test_save(self):
        request = StoreRequest("fred", "ICSA", None)
        self.store.save(request, self.ref)
        file_path = path.join(self.dirname, "fred/ICSA_.csv")
        saved_file = pd.read_csv(file_path, index_col=0, parse_dates=True)
        pd.testing.assert_frame_equal(saved_file, self.ref)

    def test_read(self):
        store = CSVDataStore(DIRNAME)
        result = store.read(StoreRequest("fred", "ICSA", None))
        pd.testing.assert_frame_equal(result, self.ref)

    def tearDown(self):
        shutil.rmtree(self.dirname, ignore_errors=True)
