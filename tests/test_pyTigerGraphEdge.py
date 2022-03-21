import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphEdge(pyTigerGraphUnitTest):
    conn = None

    def test_01_getEdgeTypes(self):
        ret = sorted(self.conn.getEdgeTypes())
        self.assertEqual(3, len(ret))
        exp = ["edge1_undirected", "edge2_directed", "edge3_directed_with_reverse"]
        self.assertEqual(exp, ret)

    def test_02_getEdgeType(self):
        ret = self.conn.getEdgeType("edge1_undirected")
        self.assertIsNotNone(ret)
        self.assertIn("FromVertexTypeName", ret)
        self.assertEqual("vertex4", ret["FromVertexTypeName"])
        self.assertIn("ToVertexTypeName", ret)
        self.assertEqual("vertex5", ret["ToVertexTypeName"])
        self.assertIn("IsDirected", ret)
        self.assertFalse(ret["IsDirected"])

        ret = self.conn.getEdgeType("edge2_directed")
        self.assertIn("IsDirected", ret)
        self.assertTrue(ret["IsDirected"])
        self.assertIn("Config", ret)
        self.assertNotIn("REVERSE_EDGE", ret["Config"])

        ret = self.conn.getEdgeType("edge3_directed_with_reverse")
        self.assertIn("IsDirected", ret)
        self.assertTrue(ret["IsDirected"])
        self.assertIn("Config", ret)
        self.assertIn("REVERSE_EDGE", ret["Config"])
        self.assertEqual("edge3_directed_with_reverse_reverse_edge", ret["Config"]["REVERSE_EDGE"])

        ret = self.conn.getEdgeType("non_existing_edge_type")
        self.assertEqual({}, ret)
        # TODO This will need to be reviewed if/when getEdgeType() return value changes from {} in
        #      case of invalid/non-existing edge type name is specified (e.g. an exception will be
        #      raised instead of returning {}


if __name__ == '__main__':
    unittest.main()
