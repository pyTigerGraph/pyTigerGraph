import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphEdge(pyTigerGraphUnitTest):
    conn = None

    def test_01_getEdgeTypes(self):
        ret = sorted(self.conn.getEdgeTypes())
        self.assertEqual(6, len(ret))
        exp = ["edge1_undirected", "edge2_directed", "edge3_directed_with_reverse",
            "edge4_many_to_many", "edge5_all_to_all", "edge6_loop"]
        self.assertEqual(exp, ret)

    def test_02_getEdgeType(self):
        ret = self.conn.getEdgeType("edge1_undirected")
        self.assertIsNotNone(ret)
        self.assertIsInstance(ret, dict)
        self.assertIn("FromVertexTypeName", ret)
        self.assertEqual(ret["FromVertexTypeName"], "vertex4")
        self.assertIn("ToVertexTypeName", ret)
        self.assertEqual(ret["ToVertexTypeName"], "vertex5")
        self.assertIn("IsDirected", ret)
        self.assertFalse(ret["IsDirected"])
        self.assertNotIn("EdgePairs", ret)

        ret = self.conn.getEdgeType("edge2_directed")
        self.assertIsNotNone(ret)
        self.assertIsInstance(ret, dict)
        self.assertIn("IsDirected", ret)
        self.assertTrue(ret["IsDirected"])
        self.assertIn("Config", ret)
        self.assertNotIn("REVERSE_EDGE", ret["Config"])

        ret = self.conn.getEdgeType("edge3_directed_with_reverse")
        self.assertIsNotNone(ret)
        self.assertIsInstance(ret, dict)
        self.assertIn("IsDirected", ret)
        self.assertTrue(ret["IsDirected"])
        self.assertIn("Config", ret)
        self.assertIn("REVERSE_EDGE", ret["Config"])
        self.assertEqual(ret["Config"]["REVERSE_EDGE"], "edge3_directed_with_reverse_reverse_edge")

        ret = self.conn.getEdgeType("edge4_many_to_many")
        self.assertIsNotNone(ret)
        self.assertIsInstance(ret, dict)
        self.assertIn("ToVertexTypeName", ret)
        self.assertEqual(ret["ToVertexTypeName"], "*")
        self.assertIn("FromVertexTypeName", ret)
        self.assertEqual(ret["FromVertexTypeName"], "*")
        self.assertIn("EdgePairs", ret)
        self.assertEqual(len(ret["EdgePairs"]), 5)

        ret = self.conn.getEdgeType("edge5_all_to_all")
        self.assertIsNotNone(ret)
        self.assertIsInstance(ret, dict)
        self.assertIn("ToVertexTypeName", ret)
        self.assertEqual(ret["ToVertexTypeName"], "*")
        self.assertIn("FromVertexTypeName", ret)
        self.assertEqual(ret["FromVertexTypeName"], "*")
        self.assertIn("EdgePairs", ret)
        self.assertEqual(len(ret["EdgePairs"]), 49)

        ret = self.conn.getEdgeType("non_existing_edge_type")
        self.assertEqual({}, ret)
        # TODO This will need to be reviewed if/when getEdgeType() return value changes from {} in
        #      case of invalid/non-existing edge type name is specified (e.g. an exception will be
        #      raised instead of returning {}

    def test_03_getEdgeSourceVertexType(self):
        ret = self.conn.getEdgeSourceVertexType("edge1_undirected")
        self.assertIsInstance(ret, str)
        self.assertEqual(ret, "vertex4")

    def test_04_getEdgeTargetVertexType(self):
        ret = self.conn.getEdgeTargetVertexType("edge2_directed")
        self.assertIsInstance(ret, str)
        self.assertEqual(ret, "vertex5")

    def test_05_isDirected(self):
        ret = self.conn.isDirected("edge1_undirected")
        self.assertIsInstance(ret, bool)
        self.assertFalse(ret)
        ret = self.conn.isDirected("edge2_directed")
        self.assertIsInstance(ret, bool)
        self.assertTrue(ret)

    def test_06_getReverseEdge(self):
        ret = self.conn.getReverseEdge("edge1_undirected")
        self.assertIsInstance(ret, str)
        self.assertEqual(ret, "")  # TODO Change this to None or something in getReverseEdge()?
        ret = self.conn.getReverseEdge("edge2_directed")
        self.assertIsInstance(ret, str)
        self.assertEqual(ret, "")  # TODO Change this to None or something in getReverseEdge()?
        ret = self.conn.getReverseEdge("edge3_directed_with_reverse")
        self.assertIsInstance(ret, str)
        self.assertEqual(ret, "edge3_directed_with_reverse_reverse_edge")

    def test_07_getEdgeCountFrom(self):
        ret = self.conn.getEdgeCountFrom(edgeType="*")
        self.assertIsInstance(ret, dict)
        self.assertIn("edge1_undirected", ret)
        self.assertEqual(ret["edge1_undirected"], 8)
        self.assertIn("edge6_loop", ret)
        self.assertEqual(ret["edge6_loop"], 0)

        ret = self.conn.getEdgeCountFrom(edgeType="edge4_many_to_many")
        self.assertIsInstance(ret, int)
        self.assertEqual(ret, 8)

        ret = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", edgeType="edge4_many_to_many",
            targetVertexType="vertex5")
        self.assertIsInstance(ret, int)
        self.assertEqual(ret, 3)

        ret = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", sourceVertexId=1)
        self.assertIsInstance(ret, dict)
        self.assertIn("edge1_undirected", ret)
        self.assertEqual(ret["edge1_undirected"], 3)
        self.assertIn("edge2_directed", ret)
        self.assertEqual(ret["edge2_directed"], 0)
        self.assertIn("edge4_many_to_many", ret)
        self.assertEqual(ret["edge1_undirected"], 3)

        ret = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", sourceVertexId=1,
            edgeType="edge1_undirected")
        self.assertIsInstance(ret, int)
        self.assertEqual(ret, 3)


if __name__ == '__main__':
    unittest.main()
