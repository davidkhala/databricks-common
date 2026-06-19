import unittest


class TextbookTest(unittest.TestCase):
    def load_data(self):
        from pandas import DataFrame
        from sklearn.datasets import fetch_california_housing
        basedata = fetch_california_housing()

        df = DataFrame(basedata.data, columns=basedata.feature_names)

        percent20 = int(df.shape[0] * 0.2)
        return {
            'testdata': df.iloc[:percent20],
            'traindata': df.iloc[percent20:],
        }

    def test_load_boston(self):
        self.load_data()
