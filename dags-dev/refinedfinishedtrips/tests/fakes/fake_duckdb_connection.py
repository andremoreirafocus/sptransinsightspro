import pandas as pd


class FakeDuckDBConnection:
    def __init__(self, df=None, raises=None):
        self._df = df if df is not None else pd.DataFrame()
        self._raises = raises
        self.closed = False

    def execute(self, sql):
        if self._raises:
            raise self._raises
        return self

    def df(self):
        return self._df

    def close(self):
        self.closed = True
