import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphQuery(pyTigerGraphUnitTest):
    conn = None

    def test_01_getQueries(self):
        # TODO Once pyTigerGraphQuery.getQueries() is available
        None

    def test_02_getInstalledQueries(self):
        ret = self.conn.getInstalledQueries()
        self.assertIn("GET /query/tests/query1", ret)
        # self.assertNotIn("GET /query/tests/query2_not_installed", ret)
        self.assertIn("GET /query/tests/query3_installed", ret)

    def test_03_runInstalledQuery(self):
        ret = self.conn.runInstalledQuery("query1")
        self.assertIn("ret", ret[0])
        self.assertEqual(15, ret[0]["ret"])

    def test_04_runInterpretedQuery(self):
        queryText = \
"""INTERPRET QUERY () FOR GRAPH $graphname {
  SumAccum<INT> @@summa;
  start = {vertex4.*};
  res =
    SELECT src
    FROM   start:src
    ACCUM  @@summa += src.a01;
  PRINT @@summa AS ret;
}"""
        ret = self.conn.runInterpretedQuery(queryText)
        self.assertIn("ret", ret[0])
        self.assertEqual(15, ret[0]["ret"])

        queryText = \
"""INTERPRET QUERY () FOR GRAPH @graphname@ {
  SumAccum<INT> @@summa;
  start = {vertex4.*};
  res =
    SELECT src
    FROM   start:src
    ACCUM  @@summa += src.a01;
  PRINT @@summa AS ret;
}"""
        ret = self.conn.runInterpretedQuery(queryText)
        self.assertIn("ret", ret[0])
        self.assertEqual(15, ret[0]["ret"])


if __name__ == '__main__':
    unittest.main()
