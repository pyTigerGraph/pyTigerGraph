import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphVertex(pyTigerGraphUnitTest):
    conn = None

    def test_01_getVertexTypes(self):
        ret = sorted(self.conn.getVertexTypes())
        self.assertEqual(5, len(ret))
        exp = ["vertex1_all_types", "vertex2_primary_key", "vertex3_primary_key_composite",
            "vertex4", "vertex5"]
        self.assertEqual(exp, ret)

    def test_02_getVertexType(self):
        ret = self.conn.getVertexType("vertex1_all_types")
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


if __name__ == '__main__':
    unittest.main()
