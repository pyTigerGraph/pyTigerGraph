import unittest
from datetime import datetime

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphQuery(pyTigerGraphUnitTest):
    conn = None

    def test_01_getQueries(self):
        # TODO Once pyTigerGraphQuery.getQueries() is available
        pass

    def test_02_getInstalledQueries(self):
        res = self.conn.getInstalledQueries()
        self.assertIn("GET /query/tests/query1", res)
        # self.assertNotIn("GET /query/tests/query2_not_installed", res)
        self.assertIn("GET /query/tests/query3_installed", res)

    def test_03_runInstalledQuery(self):
        res = self.conn.runInstalledQuery("query1")
        self.assertIn("ret", res[0])
        self.assertEqual(15, res[0]["ret"])

        params = {
            "p01_int": 1,
            "p02_uint": 1,
            "p03_float": 1.1,
            "p04_double": 1.1,
            "p05_string": "test <>\"'`\\/{}[]()<>!@£$%^&*-_=+;:|,.§±~` árvíztűrő tükörfúrógép 👍",
            "p06_bool": True,
            "p07_vertex": (1, "vertex4"),
            "p08_vertex_vertex4": 1,
            "p09_datetime": datetime.now(),
            "p10_set_int": [1, 2, 3, 2, 3, 3],  # Intentionally bag-like, to see it behaving as set
            "p11_bag_int": [1, 2, 3, 2, 3, 3],
            "p13_set_vertex": [(1, "vertex4"), (2, "vertex4"), (3, "vertex4")],
            "p14_set_vertex_vertex4": [1, 2, 3]
        }

        res = self.conn.runInstalledQuery("query4_all_param_types", params)
        self.assertIsInstance(res, list)
        self.assertIsInstance(res[4], dict)
        self.assertIn("p05_string", res[4])
        self.assertEqual(params["p05_string"], res[4]["p05_string"])
        self.assertIsInstance(res[11], dict)
        vs = res[11]
        self.assertIn("p13_set_vertex", vs)
        vs = sorted(vs["p13_set_vertex"])
        self.assertIsInstance(vs, list)
        self.assertEqual(["1", "2", "3"], vs)

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
        res = self.conn.runInterpretedQuery(queryText)
        self.assertIn("ret", res[0])
        self.assertEqual(15, res[0]["ret"])

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
        res = self.conn.runInterpretedQuery(queryText)
        self.assertIn("ret", res[0])
        self.assertEqual(15, res[0]["ret"])


if __name__ == '__main__':
    unittest.main()
