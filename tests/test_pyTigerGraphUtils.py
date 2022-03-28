import re
import unittest
from datetime import datetime

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphUtils(pyTigerGraphUnitTest):
    conn = None

    def test_01_safeChar(self):
        res = self.conn._safeChar(" _space")
        self.assertEqual("%20_space", res)
        res = self.conn._safeChar("/_slash")
        self.assertEqual("%2F_slash", res)
        res = self.conn._safeChar("ñ_LATIN_SMALL_LETTER_N_WITH_TILDE")
        self.assertEqual(res, '%C3%B1_LATIN_SMALL_LETTER_N_WITH_TILDE')
        res = self.conn._safeChar(12345)
        self.assertEqual("12345", res)
        res = self.conn._safeChar(12.345)
        self.assertEqual("12.345", res)
        now = datetime.now()
        res = self.conn._safeChar(now)
        exp = str(now).replace(" ", "%20").replace(":", "%3A")
        self.assertEqual(exp, res)
        res = self.conn._safeChar(True)
        self.assertEqual("True", res)

    def test_02_echo(self):
        res = self.conn.echo()
        self.assertIsInstance(res, str)
        self.assertEqual("Hello GSQL", res)
        res = self.conn.echo(True)
        self.assertIsInstance(res, str)
        self.assertEqual("Hello GSQL", res)

    def test_03_getVersion(self):
        res = self.conn.getVersion()
        self.assertIsInstance(res, list)
        self.assertGreater(len(res), 0)

    def test_04_getVer(self):
        res = self.conn.getVer()
        self.assertIsInstance(res, str)
        m = re.match(r"[0-9]+\.[0-9]+\.[0-9]", res)
        self.assertIsNotNone(m)


if __name__ == '__main__':
    unittest.main()
