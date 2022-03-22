import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphGSQL(pyTigerGraphUnitTest):
    conn = None

    def test_01_gsql(self):
        ret = self.conn.gsql("help")
        self.assertIsInstance(ret, str)
        ret = ret.split("\n")
        self.assertEqual("GSQL Help: Summary of TigerGraph GSQL Shell commands.", ret[0])

    def test_02_gsql(self):
        ret = self.conn.gsql("ls")
        self.assertIsInstance(ret, str)
        ret = ret.split("\n")
        self.assertEqual("---- Graph " + self.conn.graphname, ret[0])


if __name__ == '__main__':
    unittest.main()
