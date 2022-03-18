import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphGSQL(pyTigerGraphUnitTest):
    conn = None

    def test_01_gsql(self):
        ret = self.conn.gsql("help")
        self.assertIsInstance(ret, str, "Invalid response type: " + str(type(ret)))
        ret = ret.split("\n")
        self.assertEqual(ret[0], "GSQL Help: Summary of TigerGraph GSQL Shell commands.",
            "Invalid output: " + ret[0])

    def test_02_gsql(self):
        ret = self.conn.gsql("ls")
        self.assertIsInstance(ret, str, "Invalid response type: " + str(type(ret)))
        ret = ret.split("\n")
        self.assertEqual(ret[0], "---- Graph " + self.conn.graphname,
            "Invalid output: " + ret[0])


if __name__ == '__main__':
    unittest.main()
