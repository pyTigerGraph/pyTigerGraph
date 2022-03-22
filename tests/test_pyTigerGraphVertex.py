import json
import unittest

import pandas

from pyTigerGraph.pyTigerGraphException import TigerGraphException
from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphVertex(pyTigerGraphUnitTest):
    conn = None

    def test_01_getVertexTypes(self):
        ret = sorted(self.conn.getVertexTypes())
        self.assertIsInstance(ret, list)
        self.assertEqual(7, len(ret))
        exp = ["vertex1_all_types", "vertex2_primary_key", "vertex3_primary_key_composite",
            "vertex4", "vertex5", "vertex6", "vertex7"]
        self.assertEqual(exp, ret)

    def test_02_getVertexType(self):
        ret = self.conn.getVertexType("vertex1_all_types")
        self.assertIsInstance(ret, dict)
        self.assertIn("PrimaryId", ret)
        self.assertIn("AttributeName", ret["PrimaryId"])
        self.assertEqual("id", ret["PrimaryId"]["AttributeName"])
        self.assertIn("AttributeType", ret["PrimaryId"])
        self.assertIn("Name", ret["PrimaryId"]["AttributeType"])
        self.assertEqual("STRING", ret["PrimaryId"]["AttributeType"]["Name"])
        self.assertIn("IsLocal", ret)
        self.assertTrue(ret["IsLocal"])

        ret = self.conn.getVertexType("non_existing_vertex_type")
        self.assertEqual({}, ret)
        # TODO This will need to be reviewed if/when getVertexType() return value changes from {} in
        #      case of invalid/non-existing edge type name is specified (e.g. an exception will be
        #      raised instead of returning {}

    def test_03_getVertexCount(self):
        ret = self.conn.getVertexCount("*")
        self.assertIsInstance(ret, dict)
        self.assertIn("vertex4", ret)
        self.assertEqual(5, ret["vertex4"])
        self.assertIn("vertex1_all_types", ret)
        self.assertEqual(0, ret["vertex1_all_types"])

        ret = self.conn.getVertexCount("vertex4")
        self.assertIsInstance(ret, int)
        self.assertEqual(5, ret)

        ret = self.conn.getVertexCount("vertex4", "a01>=3")
        self.assertIsInstance(ret, int)
        self.assertEqual(3, ret)

        with self.assertRaises(TigerGraphException) as tge:
            ret = self.conn.getVertexCount("*", "a01>=3")
        self.assertEqual("VertexType cannot be \"*\" if where condition is specified.",
            tge.exception.message)

        with self.assertRaises(TigerGraphException) as tge:
            ret = self.conn.getVertexCount("non_existing_vertex_type")
        self.assertEqual("GSQL-7004", tge.exception.code)

    def test_04_upsertVertex(self):
        ret = self.conn.upsertVertex("vertex4", 100, {"a01": 100})
        self.assertIsInstance(ret, int)
        self.assertEqual(1, ret)

        with self.assertRaises(TigerGraphException) as tge:
            ret = self.conn.upsertVertex("non_existing_vertex_type", 100, {"a01": 100})
        self.assertEqual("REST-30200", tge.exception.code)

        with self.assertRaises(TigerGraphException) as tge:
            ret = self.conn.upsertVertex("vertex4", 100, {"non_existing_vertex_attribute": 100})
        self.assertEqual("REST-30200", tge.exception.code)

    def test_05_upsertVertices(self):
        vs = [
            (100, {"a01": (11, "+")}),
            (200, {"a01": 200}),
            (201, {"a01": 201}),
            (202, {"a01": 202})
        ]
        ret = self.conn.upsertVertices("vertex4", vs)
        self.assertIsInstance(ret, int)
        self.assertEqual(4, ret)

        ret = self.conn.getVertices("vertex4", where="a01>100")
        self.assertIsInstance(ret, list)
        v = {}
        for r in ret:
            if "v_id" in r and r["v_id"] == '100':  # v_id value is returned as str, not int
                v = r
        self.assertNotEqual({}, v)
        self.assertIn("attributes", v)
        self.assertIn("a01", v["attributes"])
        self.assertEqual(111, v["attributes"]["a01"])

        ret = self.conn.delVertices("vertex4", "a01>100")
        self.assertIsInstance(ret, int)
        self.assertEqual(4, ret)

        ret = self.conn.getVertices("vertex4", where="a01>100")
        self.assertIsInstance(ret, list)
        self.assertEqual(ret, [])

    def test_06_upsertVertexDataFrame(self):
        # TODO Implement
        None

    def test_07_getVertices(self):
        ret = self.conn.getVertices("vertex4", select="a01", where="a01>1,a01<5", sort="-a01",
            limit=2)
        self.assertIsInstance(ret, list)
        self.assertEqual(2, len(ret))
        self.assertEqual(4, ret[0]["attributes"]["a01"])
        self.assertEqual(3, ret[1]["attributes"]["a01"])

        ret = self.conn.getVertices("vertex4", select="a01", where="a01>1,a01<5", sort="-a01",
            limit=2, fmt="json")
        self.assertIsInstance(ret, str)
        ret = json.loads(ret)
        self.assertIsInstance(ret, list)
        self.assertEqual(2, len(ret))
        self.assertEqual(4, ret[0]["attributes"]["a01"])
        self.assertEqual(3, ret[1]["attributes"]["a01"])

        ret = self.conn.getVertices("vertex4", select="a01", where="a01>1,a01<5", sort="-a01",
            limit=2, fmt="df")
        self.assertIsInstance(ret, pandas.DataFrame)
        self.assertEqual(2, len(ret.index))

    def test_08_getVertexDataFrame(self):
        ret = self.conn.getVertexDataFrame("vertex4", select="a01", where="a01>1,a01<5",
            sort="-a01",
            limit=2)
        self.assertIsInstance(ret, pandas.DataFrame)
        self.assertEqual(2, len(ret.index))

    def test_09_getVerticesById(self):
        ret = self.conn.getVerticesById("vertex4", [1, 3, 5], select="a01")  # select is ignored
        self.assertIsInstance(ret, list)
        self.assertEqual(3, len(ret))

        ret = self.conn.getVerticesById("vertex4", [1, 3, 5], fmt="json")
        self.assertIsInstance(ret, str)
        ret = json.loads(ret)
        self.assertIsInstance(ret, list)
        self.assertEqual(3, len(ret))

        ret = self.conn.getVerticesById("vertex4", [1, 3, 5], fmt="df")
        self.assertIsInstance(ret, pandas.DataFrame)

    def test_10_getVertexDataFrameById(self):
        ret = self.conn.getVertexDataFrameById("vertex4", [1, 3, 5])
        self.assertIsInstance(ret, pandas.DataFrame)
        self.assertEqual(3, len(ret.index))

    def test_11_getVertexStats(self):
        ret = self.conn.getVertexStats("*", skipNA=True)
        self.assertIsInstance(ret, dict)
        self.assertIn("vertex4", ret)
        self.assertEqual(1, ret["vertex4"]["a01"]["MIN"])
        self.assertEqual(3, ret["vertex4"]["a01"]["AVG"])
        self.assertEqual(5, ret["vertex4"]["a01"]["MAX"])
        self.assertNotIn("vertex5", ret)

        ret = self.conn.getVertexStats("vertex4")
        self.assertEqual(1, ret["vertex4"]["a01"]["MIN"])
        self.assertEqual(3, ret["vertex4"]["a01"]["AVG"])
        self.assertEqual(5, ret["vertex4"]["a01"]["MAX"])

        ret = self.conn.getVertexStats("vertex5", skipNA=True)
        self.assertEqual({}, ret)


if __name__ == '__main__':
    unittest.main()
