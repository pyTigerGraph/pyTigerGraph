import json
import unittest

import pandas

from pyTigerGraph.pyTigerGraphException import TigerGraphException
from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphVertex(pyTigerGraphUnitTest):
    conn = None

    def test_01_getVertexTypes(self):
        res = sorted(self.conn.getVertexTypes())
        self.assertIsInstance(res, list)
        self.assertEqual(7, len(res))
        exp = ["vertex1_all_types", "vertex2_primary_key", "vertex3_primary_key_composite",
            "vertex4", "vertex5", "vertex6", "vertex7"]
        self.assertEqual(exp, res)

    def test_02_getVertexType(self):
        res = self.conn.getVertexType("vertex1_all_types")
        self.assertIsInstance(res, dict)
        self.assertIn("PrimaryId", res)
        self.assertIn("AttributeName", res["PrimaryId"])
        self.assertEqual("id", res["PrimaryId"]["AttributeName"])
        self.assertIn("AttributeType", res["PrimaryId"])
        self.assertIn("Name", res["PrimaryId"]["AttributeType"])
        self.assertEqual("STRING", res["PrimaryId"]["AttributeType"]["Name"])
        self.assertIn("IsLocal", res)
        self.assertTrue(res["IsLocal"])

        res = self.conn.getVertexType("non_existing_vertex_type")
        self.assertEqual({}, res)
        # TODO This will need to be reviewed if/when getVertexType() return value changes from {} in
        #      case of invalid/non-existing edge type name is specified (e.g. an exception will be
        #      raised instead of returning {}

    def test_03_getVertexCount(self):
        res = self.conn.getVertexCount("*")
        self.assertIsInstance(res, dict)
        self.assertEqual(7, len(res))
        self.assertIn("vertex4", res)
        self.assertEqual(5, res["vertex4"])
        self.assertIn("vertex1_all_types", res)
        self.assertEqual(0, res["vertex1_all_types"])

        res = self.conn.getVertexCount("vertex4")
        self.assertIsInstance(res, int)
        self.assertEqual(5, res)

        res = self.conn.getVertexCount(["vertex4", "vertex5", "vertex6"])
        self.assertIsInstance(res, dict)
        self.assertEqual(3, len(res))
        self.assertIn("vertex4", res)
        self.assertEqual(5, res["vertex4"])

        res = self.conn.getVertexCount("vertex4", "a01>=3")
        self.assertIsInstance(res, int)
        self.assertEqual(3, res)

        with self.assertRaises(TigerGraphException) as tge:
            self.conn.getVertexCount("*", "a01>=3")
        self.assertEqual("VertexType cannot be \"*\" if where condition is specified.",
            tge.exception.message)

        with self.assertRaises(TigerGraphException) as tge:
            self.conn.getVertexCount(["vertex4", "vertex5", "vertex6"], "a01>=3")
        self.assertEqual("VertexType cannot be a list if where condition is specified.",
            tge.exception.message)

        with self.assertRaises(TigerGraphException) as tge:
            self.conn.getVertexCount("non_existing_vertex_type")
        self.assertEqual("REST-30000", tge.exception.code)
        # self.assertEqual("GSQL-7004", tge.exception.code)  # TODO use with /builtins/

    def test_04_upsertVertex(self):
        res = self.conn.upsertVertex("vertex4", 100, {"a01": 100})
        self.assertIsInstance(res, int)
        self.assertEqual(1, res)

        with self.assertRaises(TigerGraphException) as tge:
            self.conn.upsertVertex("non_existing_vertex_type", 100, {"a01": 100})
        self.assertEqual("REST-30200", tge.exception.code)

        with self.assertRaises(TigerGraphException) as tge:
            self.conn.upsertVertex("vertex4", 100, {"non_existing_vertex_attribute": 100})
        self.assertEqual("REST-30200", tge.exception.code)

    def test_05_upsertVertices(self):
        vs = [
            (100, {"a01": (11, "+")}),
            (200, {"a01": 200}),
            (201, {"a01": 201}),
            (202, {"a01": 202})
        ]
        res = self.conn.upsertVertices("vertex4", vs)
        self.assertIsInstance(res, int)
        self.assertEqual(4, res)

        res = self.conn.getVertices("vertex4", where="a01>100")
        self.assertIsInstance(res, list)
        v = {}
        for r in res:
            if "v_id" in r and r["v_id"] == '100':  # v_id value is returned as str, not int
                v = r
        self.assertNotEqual({}, v)
        self.assertIn("attributes", v)
        self.assertIn("a01", v["attributes"])
        self.assertEqual(111, v["attributes"]["a01"])

        res = self.conn.delVertices("vertex4", "a01>100")
        self.assertIsInstance(res, int)
        self.assertEqual(4, res)

        res = self.conn.getVertices("vertex4", where="a01>100")
        self.assertIsInstance(res, list)
        self.assertEqual(res, [])

    def test_06_upsertVertexDataFrame(self):
        # TODO Implement
        pass

    def test_07_getVertices(self):
        res = self.conn.getVertices("vertex4", select="a01", where="a01>1,a01<5", sort="-a01",
            limit=2)
        self.assertIsInstance(res, list)
        self.assertEqual(2, len(res))
        self.assertEqual(4, res[0]["attributes"]["a01"])
        self.assertEqual(3, res[1]["attributes"]["a01"])

        res = self.conn.getVertices("vertex4", select="a01", where="a01>1,a01<5", sort="-a01",
            limit=2, fmt="json")
        self.assertIsInstance(res, str)
        res = json.loads(res)
        self.assertIsInstance(res, list)
        self.assertEqual(2, len(res))
        self.assertEqual(4, res[0]["attributes"]["a01"])
        self.assertEqual(3, res[1]["attributes"]["a01"])

        res = self.conn.getVertices("vertex4", select="a01", where="a01>1,a01<5", sort="-a01",
            limit=2, fmt="df")
        self.assertIsInstance(res, pandas.DataFrame)
        self.assertEqual(2, len(res.index))

    def test_08_getVertexDataFrame(self):
        res = self.conn.getVertexDataFrame("vertex4", select="a01", where="a01>1,a01<5",
            sort="-a01",
            limit=2)
        self.assertIsInstance(res, pandas.DataFrame)
        self.assertEqual(2, len(res.index))

    def test_09_getVerticesById(self):
        res = self.conn.getVerticesById("vertex4", [1, 3, 5], select="a01")  # select is ignored
        self.assertIsInstance(res, list)
        self.assertEqual(3, len(res))

        res = self.conn.getVerticesById("vertex4", [1, 3, 5], fmt="json")
        self.assertIsInstance(res, str)
        res = json.loads(res)
        self.assertIsInstance(res, list)
        self.assertEqual(3, len(res))

        res = self.conn.getVerticesById("vertex4", [1, 3, 5], fmt="df")
        self.assertIsInstance(res, pandas.DataFrame)

    def test_10_getVertexDataFrameById(self):
        res = self.conn.getVertexDataFrameById("vertex4", [1, 3, 5])
        self.assertIsInstance(res, pandas.DataFrame)
        self.assertEqual(3, len(res.index))

    def test_11_getVertexStats(self):
        res = self.conn.getVertexStats("*", skipNA=True)
        self.assertIsInstance(res, dict)
        self.assertIn("vertex4", res)
        self.assertEqual(1, res["vertex4"]["a01"]["MIN"])
        self.assertEqual(3, res["vertex4"]["a01"]["AVG"])
        self.assertEqual(5, res["vertex4"]["a01"]["MAX"])
        self.assertNotIn("vertex5", res)

        res = self.conn.getVertexStats("vertex4")
        self.assertEqual(1, res["vertex4"]["a01"]["MIN"])
        self.assertEqual(3, res["vertex4"]["a01"]["AVG"])
        self.assertEqual(5, res["vertex4"]["a01"]["MAX"])

        res = self.conn.getVertexStats("vertex5", skipNA=True)
        self.assertEqual({}, res)

    def test_12_delVertices(self):
        vs = [
            (300, {"a01": 300}),
            (301, {"a01": 301}),
            (302, {"a01": 302}),
            (303, {"a01": 303}),
            (304, {"a01": 304})
        ]
        res = self.conn.upsertVertices("vertex4", vs)
        self.assertIsInstance(res, int)
        self.assertEqual(5, res)

        res = self.conn.getVertices("vertex4", where="a01>=300")
        self.assertIsInstance(res, list)
        self.assertEqual(5, len(res))

        res = self.conn.delVertices("vertex4", where="a01>=303")
        self.assertIsInstance(res, int)
        self.assertEqual(2, res)

    def test_13_delVerticesById(self):
        res = self.conn.delVerticesById("vertex4", 300)
        self.assertIsInstance(res, int)
        self.assertEqual(1, res)

        res = self.conn.delVerticesById("vertex4", [301, 302])
        self.assertIsInstance(res, int)
        self.assertEqual(2, res)

    def test_14_delVerticesByType(self):
        pass
        # TODO Implement pyTigergraphVertices.delVerticesByType() first

    def test_15_vertexSetToDataFrame(self):
        res = self.conn.getVertices("vertex4")
        self.assertIsInstance(res, list)
        self.assertEqual(5, len(res))

        res = self.conn.vertexSetToDataFrame(res)
        self.assertIsInstance(res, pandas.DataFrame)
        self.assertEqual(5, len(res.index))
        self.assertEqual(["v_id","a01"], list(res.columns))


if __name__ == '__main__':
    unittest.main()
