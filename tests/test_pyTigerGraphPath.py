import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphPath(pyTigerGraphUnitTest):
    conn = None

    def test_01_preparePathParams(self):
        ret = self.conn._preparePathParams([("srctype1", 1), ("srctype2", 2), ("srctype3", 3)],
            [("trgtype1", 1), ("trgtype2", 2), ("trgtype3", 3)], 5,
            [("srctype1", "a01>10")])
        print(ret)


if __name__ == '__main__':
    unittest.main()
