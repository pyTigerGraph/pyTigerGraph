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
        self.assertEqual("vertex4", ret["FromVertexTypeName"])
        self.assertIn("ToVertexTypeName", ret)
        self.assertEqual("vertex5", ret["ToVertexTypeName"])
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
        self.assertEqual("edge3_directed_with_reverse_reverse_edge", ret["Config"]["REVERSE_EDGE"])

        ret = self.conn.getEdgeType("edge4_many_to_many")
        self.assertIsNotNone(ret)
        self.assertIsInstance(ret, dict)
        self.assertIn("ToVertexTypeName", ret)
        self.assertEqual("*", ret["ToVertexTypeName"])
        self.assertIn("FromVertexTypeName", ret)
        self.assertEqual("*", ret["FromVertexTypeName"])
        self.assertIn("EdgePairs", ret)
        self.assertEqual(5, len(ret["EdgePairs"]))

        ret = self.conn.getEdgeType("edge5_all_to_all")
        self.assertIsNotNone(ret)
        self.assertIsInstance(ret, dict)
        self.assertIn("ToVertexTypeName", ret)
        self.assertEqual("*", ret["ToVertexTypeName"])
        self.assertIn("FromVertexTypeName", ret)
        self.assertEqual("*", ret["FromVertexTypeName"])
        self.assertIn("EdgePairs", ret)
        self.assertEqual(49, len(ret["EdgePairs"]))

        ret = self.conn.getEdgeType("non_existing_edge_type")
        self.assertEqual({}, ret)
        # TODO This will need to be reviewed if/when getEdgeType() return value changes from {} in
        #      case of invalid/non-existing edge type name is specified (e.g. an exception will be
        #      raised instead of returning {}

    def test_03_getEdgeSourceVertexType(self):
        ret = self.conn.getEdgeSourceVertexType("edge1_undirected")
        self.assertIsInstance(ret, str)
        self.assertEqual("vertex4", ret)

    def test_04_getEdgeTargetVertexType(self):
        ret = self.conn.getEdgeTargetVertexType("edge2_directed")
        self.assertIsInstance(ret, str)
        self.assertEqual("vertex5", ret)

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
        self.assertEqual("", ret)  # TODO Change this to None or something in getReverseEdge()?
        ret = self.conn.getReverseEdge("edge2_directed")
        self.assertIsInstance(ret, str)
        self.assertEqual("", ret)  # TODO Change this to None or something in getReverseEdge()?
        ret = self.conn.getReverseEdge("edge3_directed_with_reverse")
        self.assertIsInstance(ret, str)
        self.assertEqual("edge3_directed_with_reverse_reverse_edge", ret)

    def test_07_getEdgeCountFrom(self):
        ret = self.conn.getEdgeCountFrom(edgeType="*")
        self.assertIsInstance(ret, dict)
        self.assertIn("edge1_undirected", ret)
        self.assertEqual(8, ret["edge1_undirected"])
        self.assertIn("edge6_loop", ret)
        self.assertEqual(0, ret["edge6_loop"])

        ret = self.conn.getEdgeCountFrom(edgeType="edge4_many_to_many")
        self.assertIsInstance(ret, int)
        self.assertEqual(8, ret)

        ret = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", edgeType="edge4_many_to_many",
            targetVertexType="vertex5")
        self.assertIsInstance(ret, int)
        self.assertEqual(3, ret)

        ret = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", sourceVertexId=1)
        self.assertIsInstance(ret, dict)
        self.assertIn("edge1_undirected", ret)
        self.assertEqual(3, ret["edge1_undirected"])
        self.assertIn("edge2_directed", ret)
        self.assertEqual(0, ret["edge2_directed"])
        self.assertIn("edge4_many_to_many", ret)
        self.assertEqual(3, ret["edge1_undirected"])

        ret = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", sourceVertexId=1,
            edgeType="edge1_undirected")
        self.assertIsInstance(ret, int)
        self.assertEqual(3, ret)

        ret = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", sourceVertexId=1,
            edgeType="edge1_undirected", where="a01=2")
        self.assertIsInstance(ret, int)
        self.assertEqual(2, ret)

        ret = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", sourceVertexId=1,
            edgeType="edge1_undirected", targetVertexType="vertex5")
        self.assertIsInstance(ret, int)
        self.assertEqual(3, ret)

        ret = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", sourceVertexId=1,
            edgeType="edge1_undirected", targetVertexType="vertex5", targetVertexId=3)
        self.assertIsInstance(ret, int)
        self.assertEqual(1, ret)

    def test_08_getEdgeCount(self):
        ret = self.conn.getEdgeCount("*")
        self.assertIsInstance(ret, dict)
        self.assertIn("edge1_undirected", ret)
        self.assertEqual(8, ret["edge1_undirected"])
        self.assertIn("edge6_loop", ret)
        self.assertEqual(0, ret["edge6_loop"])

        ret = self.conn.getEdgeCount("edge4_many_to_many")
        self.assertIsInstance(ret, int)
        self.assertEqual(8, ret)

        ret = self.conn.getEdgeCount("edge4_many_to_many", "vertex4")
        self.assertIsInstance(ret, int)
        self.assertEqual(8, ret)

        ret = self.conn.getEdgeCount("edge4_many_to_many", "vertex4", "vertex5")
        self.assertIsInstance(ret, int)
        self.assertEqual(3, ret)

    def test_09_upsertEdge(self):
        ret = self.conn.upsertEdge("vertex6", 1, "edge4_many_to_many", "vertex7", 1)
        self.assertIsInstance(ret, int)
        self.assertEqual(1, ret)

        ret = self.conn.upsertEdge("vertex6", 6, "edge4_many_to_many", "vertex7", 6)
        self.assertIsInstance(ret, int)
        self.assertEqual(1, ret)

        # TODO Tests with ack, new_vertex_only, vertex_must_exist, update_vertex_only and
        #   atomic_level parameters; when they will be added to pyTigergrapEdge.upsertEdge()

    def test_10_upsertEdges(self):
        pass

    def test_11_upsertEdgeDataFrame(self):
        pass

    def test_12_getEdges(self):
        pass

    def test_13_getEdgesDataFrame(self):
        pass

    def test_14_getEdgesByType(self):
        pass

    def test_15_getEdgesDataFrameByType(self):
        pass

    def test_16_getEdgeStats(self):
        pass

    def test_17_delEdges(self):
        pass

    def test_18_edgeSetToDataFrame(self):
        pass


if __name__ == '__main__':
    unittest.main()
