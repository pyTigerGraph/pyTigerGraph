import re
import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphUtils(pyTigerGraphUnitTest):
    conn = None

    def test_01_echo(self):
        ret = self.conn.echo()
        self.assertIsInstance(ret, str)
        self.assertEqual("Hello GSQL", ret)
        ret = self.conn.echo(True)
        self.assertIsInstance(ret, str)
        self.assertEqual("Hello GSQL", ret)

    def test_02_getVersion(self):
        ret = self.conn.getVersion()
        self.assertIsInstance(ret, list)
        self.assertGreater(len(ret), 0)

    def test_03_getVer(self):
        ret = self.conn.getVer()
        self.assertIsInstance(ret, str)
        m = re.match("[0-9]+\.[0-9]+\.[0-9]", ret)
        self.assertIsNotNone(m)


if __name__ == '__main__':
    unittest.main()
