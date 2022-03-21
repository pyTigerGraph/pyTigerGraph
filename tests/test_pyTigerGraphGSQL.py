import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphGSQL(pyTigerGraphUnitTest):
    conn = None

    def test_01_gsql(self):
        ret = self.conn.gsql("help")
        self.assertIsInstance(ret, str)
        ret = ret.split("\n")
        self.assertEqual(ret[0], "GSQL Help: Summary of TigerGraph GSQL Shell commands.")

    def test_02_gsql(self):
        ret = self.conn.gsql("ls")
        self.assertIsInstance(ret, str)
        ret = ret.split("\n")
        self.assertEqual(ret[0], "---- Graph " + self.conn.graphname)


if __name__ == '__main__':
    unittest.main()
