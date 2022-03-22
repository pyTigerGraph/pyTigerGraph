import unittest

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
        self.assertEqual(ret["PrimaryId"]["AttributeType"]["Name"], "STRING")
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
        self.assertEqual(ret["vertex4"], 5)
        self.assertIn("vertex1_all_types", ret)
        self.assertEqual(ret["vertex1_all_types"], 0)

        ret = self.conn.getVertexCount("vertex4")
        self.assertIsInstance(ret, int)
        self.assertEqual(ret, 5)

        ret = self.conn.getVertexCount("vertex4", "a01>=3")
        self.assertIsInstance(ret, int)
        self.assertEqual(ret, 3)

        with self.assertRaises(TigerGraphException):
            self.conn.getVertexCount("non_existing_vertex_type")


if __name__ == '__main__':
    unittest.main()
