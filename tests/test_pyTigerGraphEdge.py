import json
import unittest

import pandas

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphEdge(pyTigerGraphUnitTest):
    conn = None

    def test_01_getEdgeTypes(self):
        res = sorted(self.conn.getEdgeTypes())
        self.assertEqual(6, len(res))
        exp = ["edge1_undirected", "edge2_directed", "edge3_directed_with_reverse",
            "edge4_many_to_many", "edge5_all_to_all", "edge6_loop"]
        self.assertEqual(exp, res)

    def test_02_getEdgeType(self):
        res = self.conn.getEdgeType("edge1_undirected")
        self.assertIsNotNone(res)
        self.assertIsInstance(res, dict)
        self.assertIn("FromVertexTypeName", res)
        self.assertEqual("vertex4", res["FromVertexTypeName"])
        self.assertIn("ToVertexTypeName", res)
        self.assertEqual("vertex5", res["ToVertexTypeName"])
        self.assertIn("IsDirected", res)
        self.assertFalse(res["IsDirected"])
        self.assertNotIn("EdgePairs", res)

        res = self.conn.getEdgeType("edge2_directed")
        self.assertIsNotNone(res)
        self.assertIsInstance(res, dict)
        self.assertIn("IsDirected", res)
        self.assertTrue(res["IsDirected"])
        self.assertIn("Config", res)
        self.assertNotIn("REVERSE_EDGE", res["Config"])

        res = self.conn.getEdgeType("edge3_directed_with_reverse")
        self.assertIsNotNone(res)
        self.assertIsInstance(res, dict)
        self.assertIn("IsDirected", res)
        self.assertTrue(res["IsDirected"])
        self.assertIn("Config", res)
        self.assertIn("REVERSE_EDGE", res["Config"])
        self.assertEqual("edge3_directed_with_reverse_reverse_edge", res["Config"]["REVERSE_EDGE"])

        res = self.conn.getEdgeType("edge4_many_to_many")
        self.assertIsNotNone(res)
        self.assertIsInstance(res, dict)
        self.assertIn("ToVertexTypeName", res)
        self.assertEqual("*", res["ToVertexTypeName"])
        self.assertIn("FromVertexTypeName", res)
        self.assertEqual("*", res["FromVertexTypeName"])
        self.assertIn("EdgePairs", res)
        self.assertEqual(5, len(res["EdgePairs"]))

        res = self.conn.getEdgeType("edge5_all_to_all")
        self.assertIsNotNone(res)
        self.assertIsInstance(res, dict)
        self.assertIn("ToVertexTypeName", res)
        self.assertEqual("*", res["ToVertexTypeName"])
        self.assertIn("FromVertexTypeName", res)
        self.assertEqual("*", res["FromVertexTypeName"])
        self.assertIn("EdgePairs", res)
        self.assertEqual(49, len(res["EdgePairs"]))

        res = self.conn.getEdgeType("non_existing_edge_type")
        self.assertEqual({}, res)
        # TODO This will need to be reviewed if/when getEdgeType() return value changes from {} in
        #      case of invalid/non-existing edge type name is specified (e.g. an exception will be
        #      raised instead of returning {}

    def test_03_getEdgeSourceVertexType(self):
        res = self.conn.getEdgeSourceVertexType("edge1_undirected")
        self.assertIsInstance(res, str)
        self.assertEqual("vertex4", res)

    def test_04_getEdgeTargetVertexType(self):
        res = self.conn.getEdgeTargetVertexType("edge2_directed")
        self.assertIsInstance(res, str)
        self.assertEqual("vertex5", res)

    def test_05_isDirected(self):
        res = self.conn.isDirected("edge1_undirected")
        self.assertIsInstance(res, bool)
        self.assertFalse(res)
        res = self.conn.isDirected("edge2_directed")
        self.assertIsInstance(res, bool)
        self.assertTrue(res)

    def test_06_getReverseEdge(self):
        res = self.conn.getReverseEdge("edge1_undirected")
        self.assertIsInstance(res, str)
        self.assertEqual("", res)  # TODO Change this to None or something in getReverseEdge()?
        res = self.conn.getReverseEdge("edge2_directed")
        self.assertIsInstance(res, str)
        self.assertEqual("", res)  # TODO Change this to None or something in getReverseEdge()?
        res = self.conn.getReverseEdge("edge3_directed_with_reverse")
        self.assertIsInstance(res, str)
        self.assertEqual("edge3_directed_with_reverse_reverse_edge", res)

    def test_07_getEdgeCountFrom(self):
        res = self.conn.getEdgeCountFrom(edgeType="*")
        self.assertIsInstance(res, dict)
        self.assertIn("edge1_undirected", res)
        self.assertEqual(8, res["edge1_undirected"])
        self.assertIn("edge6_loop", res)
        self.assertEqual(0, res["edge6_loop"])

        res = self.conn.getEdgeCountFrom(edgeType="edge4_many_to_many")
        self.assertIsInstance(res, int)
        self.assertEqual(8, res)

        res = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", edgeType="edge4_many_to_many",
            targetVertexType="vertex5")
        self.assertIsInstance(res, int)
        self.assertEqual(3, res)

        res = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", sourceVertexId=1)
        self.assertIsInstance(res, dict)
        self.assertIn("edge1_undirected", res)
        self.assertEqual(3, res["edge1_undirected"])
        self.assertIn("edge2_directed", res)
        self.assertEqual(0, res["edge2_directed"])
        self.assertIn("edge4_many_to_many", res)
        self.assertEqual(3, res["edge1_undirected"])

        res = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", sourceVertexId=1,
            edgeType="edge1_undirected")
        self.assertIsInstance(res, int)
        self.assertEqual(3, res)

        res = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", sourceVertexId=1,
            edgeType="edge1_undirected", where="a01=2")
        self.assertIsInstance(res, int)
        self.assertEqual(2, res)

        res = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", sourceVertexId=1,
            edgeType="edge1_undirected", targetVertexType="vertex5")
        self.assertIsInstance(res, int)
        self.assertEqual(3, res)

        res = self.conn.getEdgeCountFrom(sourceVertexType="vertex4", sourceVertexId=1,
            edgeType="edge1_undirected", targetVertexType="vertex5", targetVertexId=3)
        self.assertIsInstance(res, int)
        self.assertEqual(1, res)

    def test_08_getEdgeCount(self):
        res = self.conn.getEdgeCount("*")
        self.assertIsInstance(res, dict)
        self.assertIn("edge1_undirected", res)
        self.assertEqual(8, res["edge1_undirected"])
        self.assertIn("edge6_loop", res)
        self.assertEqual(0, res["edge6_loop"])

        res = self.conn.getEdgeCount("edge4_many_to_many")
        self.assertIsInstance(res, int)
        self.assertEqual(8, res)

        res = self.conn.getEdgeCount("edge4_many_to_many", "vertex4")
        self.assertIsInstance(res, int)
        self.assertEqual(8, res)

        res = self.conn.getEdgeCount("edge4_many_to_many", "vertex4", "vertex5")
        self.assertIsInstance(res, int)
        self.assertEqual(3, res)

    """
    Apparently, the following tests are not structured properly.
    The code below first inserts edges in two steps, then retrieves them, and finally, deletes them.
    It seems that the order of execution is not guaranteed, so the serialised nature of steps might
    not work in some environments/setups.
    Also, unittest runs separate tests with fresh instances of the TestCase, so setUp and tearDown
    are executed before/after each tests and – importantly – it is not "possible" to persist
    information between test cases (i.e. save a piece of information in e.g. a variable of the class
    instance in one test and use it in another test) (it is technically possible, but not
    recommended due to the aforementioned reasons).
    
    Luckily, it seems that tests are executed in alphabetical order, so there is a good chance that
    in basic testing setups, they will be executed in the desired order.
    
    TODO How to structure tests so that every step can be executed independently?
    E.g. how to test insertion and deletion of edge?
     • Should the insertion test have a clean-up stage deleting the newly inserted vertices?
       And similarly, should the deletion test have a setup stage, when vertices to be deleted are
       inserted?
     • Or should these two actions tested together? But that would defeat the idea of unittests.
    """

    def test_09_upsertEdge(self):
        res = self.conn.upsertEdge("vertex6", 1, "edge4_many_to_many", "vertex7", 1)
        self.assertIsInstance(res, int)
        self.assertEqual(1, res)

        res = self.conn.upsertEdge("vertex6", 6, "edge4_many_to_many", "vertex7", 6)
        self.assertIsInstance(res, int)
        self.assertEqual(1, res)

        # TODO Tests with ack, new_vertex_only, vertex_must_exist, update_vertex_only and
        #   atomic_level parameters; when they will be added to pyTigerGraphEdge.upsertEdge()

    def test_10_upsertEdges(self):
        es = [
            (2, 1),
            (2, 2),
            (2, 3),
            (2, 4)
        ]
        res = self.conn.upsertEdges("vertex6", "edge4_many_to_many", "vertex7", es)
        self.assertIsInstance(res, int)
        self.assertEqual(4, res)

        res = self.conn.getEdgeCount("edge4_many_to_many")
        self.assertIsInstance(res, int)
        self.assertEqual(14, res)

    def test_11_upsertEdgeDataFrame(self):
        # TODO Implement
        pass

    def test_12_getEdges(self):
        res = self.conn.getEdges("vertex4", 1)
        self.assertIsInstance(res, list)
        self.assertEqual(6, len(res))

        res = self.conn.getEdges("vertex4", 1, "edge1_undirected")
        self.assertIsInstance(res, list)
        self.assertEqual(3, len(res))

        res = self.conn.getEdges("vertex4", 1, "edge1_undirected", "vertex5")
        self.assertIsInstance(res, list)
        self.assertEqual(3, len(res))

        res = self.conn.getEdges("vertex4", 1, "edge1_undirected", "vertex5", 2)
        self.assertIsInstance(res, list)
        self.assertEqual(1, len(res))

        res = self.conn.getEdges("vertex4", 1, "edge1_undirected", select="a01", where="a01>1")
        self.assertIsInstance(res, list)
        self.assertEqual(2, len(res))

        res = self.conn.getEdges("vertex4", 1, "edge1_undirected", sort="-a01", limit=2)
        self.assertIsInstance(res, list)
        self.assertEqual(2, len(res))

        res = self.conn.getEdges("vertex4", 1, "edge1_undirected", "vertex5", fmt="json")
        self.assertIsInstance(res, str)
        res = json.loads(res)
        self.assertIsInstance(res, list)
        self.assertEqual(3, len(res))

        res = self.conn.getEdges("vertex4", 1, "edge1_undirected", "vertex5", fmt="df")
        self.assertIsInstance(res, pandas.DataFrame)
        self.assertEqual(3, len(res.index))

    def test_13_getEdgesDataFrame(self):
        res = self.conn.getEdgesDataFrame("vertex4", 1, "edge1_undirected", "vertex5")
        self.assertIsInstance(res, pandas.DataFrame)
        self.assertEqual(3, len(res.index))

    def test_14_getEdgesByType(self):
        res = self.conn.getEdgesByType("edge1_undirected")
        self.assertIsInstance(res, list)
        self.assertEqual(8, len(res))

    def test_15_getEdgesDataFrameByType(self):
        pass

    def test_16_getEdgeStats(self):
        res = self.conn.getEdgeStats("edge1_undirected")
        self.assertIsInstance(res, dict)
        self.assertEqual(1, len(res))
        self.assertIn("edge1_undirected", res)
        self.assertEqual(2, res["edge1_undirected"]["a01"]["MAX"])
        self.assertEqual(1.875, res["edge1_undirected"]["a01"]["AVG"])

        res = self.conn.getEdgeStats(["edge1_undirected", "edge2_directed", "edge6_loop"])
        self.assertIsInstance(res, dict)
        self.assertEqual(3, len(res))
        self.assertIn("edge1_undirected", res)
        self.assertEqual(2, res["edge1_undirected"]["a01"]["MAX"])
        self.assertIn("edge2_directed", res)
        self.assertEqual(2, res["edge2_directed"]["a01"]["AVG"])
        self.assertIn("edge6_loop", res)
        self.assertEqual({}, res["edge6_loop"])

        res = self.conn.getEdgeStats(["edge1_undirected", "edge2_directed", "edge6_loop"],
            skipNA=True)
        self.assertIsInstance(res, dict)
        self.assertEqual(2, len(res))
        self.assertIn("edge1_undirected", res)
        self.assertEqual(2, res["edge1_undirected"]["a01"]["MAX"])
        self.assertIn("edge2_directed", res)
        self.assertNotIn("edge6_loop", res)

        res = self.conn.getEdgeStats("*", skipNA=True)
        self.assertIsInstance(res, dict)
        self.assertIn("edge3_directed_with_reverse", res)
        self.assertNotIn("edge4_many_to_many", res)

    def test_17_delEdges(self):
        res = self.conn.delEdges("vertex6", 1)
        self.assertIsInstance(res, dict)
        self.assertEqual(7, len(res))
        self.assertIn("edge4_many_to_many", res)
        self.assertEqual(1, res["edge4_many_to_many"])

        res = self.conn.delEdges("vertex6", 6, "edge4_many_to_many")
        self.assertIsInstance(res, dict)
        self.assertEqual(1, len(res))
        self.assertIn("edge4_many_to_many", res)
        self.assertEqual(1, res["edge4_many_to_many"])

        res = self.conn.delEdges("vertex6", 6, "edge4_many_to_many")
        self.assertIsInstance(res, dict)
        self.assertEqual(1, len(res))
        self.assertIn("edge4_many_to_many", res)
        self.assertEqual(0, res["edge4_many_to_many"])

        res = self.conn.delEdges("vertex6", 2, "edge4_many_to_many", "vertex7", 1)
        self.assertIsInstance(res, dict)
        self.assertEqual(1, len(res))
        self.assertIn("edge4_many_to_many", res)
        self.assertEqual(1, res["edge4_many_to_many"])

        res = self.conn.delEdges("vertex6", 2, "edge4_many_to_many", "vertex7")
        self.assertIsInstance(res, dict)
        self.assertEqual(1, len(res))
        self.assertIn("edge4_many_to_many", res)
        self.assertEqual(3, res["edge4_many_to_many"])

    def test_18_edgeSetToDataFrame(self):
        pass


if __name__ == '__main__':
    unittest.main()
