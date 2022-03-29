import unittest

from pyTigerGraph.pyTigerGraphException import TigerGraphException
from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphPath(pyTigerGraphUnitTest):
    conn = None

    def test_01_getSecrets(self):
        res = self.conn.showSecrets()
        self.assertIsInstance(dict)
        self.assertEqual(3, len(res))
        self.assertIn("secret1", res)
        self.assertIn("secret2", res)
        self.assertIn("secret2", res)

    def test_02_getSecret(self):
        pass
        # TODO Implement

    def test_03_createSecret(self):
        res = self.conn.createSecret("secret4")
        self.assertIsInstance(res, str)

        res = self.conn.createSecret("secret5", True)
        self.assertIsInstance(res, dict)
        self.assertEqual(1, len(res))
        alias = list(res.keys())[0]
        self.assertEqual("secret5", alias)

        res = self.conn.createSecret(withAlias=True)
        self.assertIsInstance(res, dict)
        self.assertEqual(1, len(res))
        alias = list(res.keys())[0]
        self.assertTrue(alias.startswith("AUTO_GENERATED_ALIAS_"))

        with self.assertRaises(TigerGraphException) as tge:
            self.conn.createSecret("secret1")
        self.assertEqual("The secret with alias secret1 already exists.", tge.exception.message)

    def test_04_dropSecret(self):
        res = self.conn.showSecrets()
        for a in list(res.keys()):
            if a.startswith("AUTO_GENERATED_ALIAS"):
                res = self.conn.dropSecret(a)
                self.assertTrue("Successfully dropped secrets" in res)

        res = self.conn.dropSecret(["secret4", "secret5"])
        self.assertTrue("Failed to drop secrets" not in res)

        res = self.conn.dropSecret("non_existent_secret")
        self.assertTrue("Failed to drop secrets" in res)

        with self.assertRaises(TigerGraphException) as tge:
            res = self.conn.dropSecret("non_existent_secret", False)

    def test_05_getToken(self):
        pass

    def test_06_refreshToken(self):
        pass

    def test_07_deleteToken(self):
        pass


if __name__ == '__main__':
    unittest.main()
