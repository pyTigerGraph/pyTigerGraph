import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphGSQL(pyTigerGraphUnitTest):
    conn = None

    def test_01_gsql(self):
        res = self.conn.gsql("help")
        self.assertIsInstance(res, str)
        res = res.split("\n")
        self.assertEqual("GSQL Help: Summary of TigerGraph GSQL Shell commands.", res[0])

    def test_02_gsql(self):
        res = self.conn.gsql("ls")
        self.assertIsInstance(res, str)
        res = res.split("\n")
        self.assertEqual("---- Graph " + self.conn.graphname, res[0])


if __name__ == '__main__':
    unittest.main()
