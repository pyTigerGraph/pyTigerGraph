import json
import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphPath(pyTigerGraphUnitTest):
    conn = None

    vs = [
        (10, {"a01": 999}),
        (20, {"a01": 999}),
        (30, {"a01": 999}),
        (40, {"a01": 999}),
        (50, {"a01": 999}),
        (60, {"a01": 900}),
        (70, {"a01": 900}),
        (80, {"a01": 900})
    ]
    es = [
        (10, 20, {"a01": 999}),
        (20, 30, {"a01": 999}),
        (30, 40, {"a01": 999}),
        (40, 50, {"a01": 900}),
        (10, 60, {"a01": 900}),
        (60, 70, {"a01": 900}),
        (70, 80, {"a01": 999}),
        (80, 40, {"a01": 999}),
        (80, 20, {"a01": 999}),
        (70, 40, {"a01": 900}),
        (30, 60, {"a01": 999})
    ]

    def setUp(self):
        super().setUp()
        self.conn.upsertVertices("vertex4", self.vs)
        self.conn.upsertEdges("vertex4", "edge6_loop", "vertex4", self.es)

    def tearDown(self):
        for i in self.es:
            self.conn.delEdges("vertex4", i[0], "edge6_loop", "vertex4", i[1])
        for i in self.vs:
            self.conn.delVerticesById("vertex4", i[0])

    def _check_vertices(self, res_vs: list, exp_vs: list) -> bool:
        self.assertEqual(len(exp_vs), len(res_vs))
        vs = []
        for v in res_vs:
            vs.append(int(v["v_id"]))
        return sorted(vs) == sorted(exp_vs)

    def _check_edges(self, res_es: list, exp_es: list) -> bool:
        self.assertEqual(len(exp_es), len(res_es))
        es = []
        for e in res_es:
            es.append((int(e["from_id"]), int(e["to_id"])))
        return sorted(es) == sorted(exp_es)

    def test_01_preparePathParams(self):
        res = self.conn._preparePathParams([("srctype1", 1), ("srctype2", 2), ("srctype3", 3)],
            [("trgtype1", 1), ("trgtype2", 2), ("trgtype3", 3)], 5,
            [("srctype1", "a01>10")], [("trgtype1", "a10<20")], True)
        self.assertIsInstance(res, str)
        res = json.loads(res)
        self.assertEqual(6, len(res))
        self.assertIn("sources", res)
        srcs = res["sources"]
        self.assertIsInstance(srcs, list)
        self.assertEqual(3, len(srcs))
        self.assertEqual('{"type": "srctype1", "id": 1}', json.dumps(srcs[0]))
        self.assertIn("targets", res)
        self.assertIn("vertexFilters", res)
        self.assertIn("edgeFilters", res)
        self.assertIn("maxLength", res)
        self.assertIn("allShortestPaths", res)
        self.assertTrue(res["allShortestPaths"])

        res = self.conn._preparePathParams([("srct", 1)], [("trgt", 1)])
        self.assertEqual(
            '{"sources": [{"type": "srct", "id": 1}], "targets": [{"type": "trgt", "id": 1}]}',
            res
        )

    def test_02_shortestPath(self):

        self.assertEqual(8, self.conn.getVertexCount("vertex4", where="a01>=900"))
        self.assertEqual(11, self.conn.getEdgeCount("edge6_loop", "vertex4", "vertex4"))

        res = self.conn.shortestPath(("vertex4", 10), ("vertex4", 50))
        vs1 = [10, 20, 30, 40, 50]
        es1 = [(10, 20), (20, 30), (30, 40), (40, 50)]
        vs2 = [10, 60, 70, 40, 50]
        es2 = [(10, 60), (60, 70), (70, 40), (40, 50)]
        self.assertTrue(
            (self._check_vertices(res[0]["vertices"], vs1) and
             self._check_edges(res[0]["edges"], es1)) or
            (self._check_vertices(res[0]["vertices"], vs2) and
             self._check_edges(res[0]["edges"], es2))
        )

        res = self.conn.shortestPath(("vertex4", 10), ("vertex4", 50), allShortestPaths=True)
        vs3 = [10, 20, 30, 40, 50, 60, 70]
        es3 = [(10, 20), (20, 30), (30, 40), (40, 50), (10, 60), (60, 70), (70, 40)]
        self.assertTrue(
            (self._check_vertices(res[0]["vertices"], vs3) and
             self._check_edges(res[0]["edges"], es3))
        )

        res = self.conn.shortestPath(("vertex4", 10), ("vertex4", 50), maxLength=3)
        self.assertEqual([], res[0]["vertices"])
        self.assertEqual([], res[0]["edges"])

        res = self.conn.shortestPath(("vertex4", 10), ("vertex4", 50), allShortestPaths=True,
            vertexFilters=("vertex4", "a01>950"))
        self.assertTrue(
            (self._check_vertices(res[0]["vertices"], vs1) and
             self._check_edges(res[0]["edges"], es1))
        )

        res = self.conn.shortestPath(("vertex4", 10), ("vertex4", 50), allShortestPaths=True,
            edgeFilters=("edge6_loop", "a01<950"))
        self.assertTrue(
            (self._check_vertices(res[0]["vertices"], vs2) and
             self._check_edges(res[0]["edges"], es2))
        )

    def test_03_allPaths(self):
        res = self.conn.allPaths(("vertex4", 10), ("vertex4", 50), maxLength=4)
        vs = [10, 20, 30, 40, 50, 60, 70]
        es = [(10, 20), (20, 30), (30, 40), (40, 50), (10, 60), (60, 70), (70, 40)]
        self.assertTrue(
            (self._check_vertices(res[0]["vertices"], vs) and
             self._check_edges(res[0]["edges"], es))
        )

        res = self.conn.allPaths(("vertex4", 10), ("vertex4", 50), maxLength=5)
        vs = [10, 20, 30, 40, 50, 60, 70, 80]
        es = [(10, 20), (20, 30), (30, 40), (40, 50), (10, 60), (60, 70), (70, 40), (70, 80),
            (80, 40)]
        self.assertTrue(
            (self._check_vertices(res[0]["vertices"], vs) and
             self._check_edges(res[0]["edges"], es))
        )

        res = self.conn.allPaths(("vertex4", 10), ("vertex4", 50), maxLength=6)
        vs = [10, 20, 30, 40, 50, 60, 70, 80]
        es = [(10, 20), (20, 30), (30, 40), (40, 50), (10, 60), (60, 70), (70, 40), (70, 80),
            (80, 40), (30, 60)]
        self.assertTrue(
            (self._check_vertices(res[0]["vertices"], vs) and
             self._check_edges(res[0]["edges"], es))
        )

        res = self.conn.allPaths(("vertex4", 10), ("vertex4", 50), maxLength=5,
            vertexFilters=("vertex4", "a01>950"))
        vs = [10, 20, 30, 40, 50]
        es = [(10, 20), (20, 30), (30, 40), (40, 50)]
        self.assertTrue(
            (self._check_vertices(res[0]["vertices"], vs) and
             self._check_edges(res[0]["edges"], es))
        )

        res = self.conn.allPaths(("vertex4", 10), ("vertex4", 50), maxLength=5,
            edgeFilters=("edge6_loop", "a01<950"))
        vs = [10, 60, 70, 40, 50]
        es = [(10, 60), (60, 70), (70, 40), (40, 50)]
        self.assertTrue(
            (self._check_vertices(res[0]["vertices"], vs) and
             self._check_edges(res[0]["edges"], es))
        )


if __name__ == '__main__':
    unittest.main()
