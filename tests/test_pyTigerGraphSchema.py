import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphSchema(pyTigerGraphUnitTest):
    conn = None

    def test_01_getUDTs(self):
        res = self.conn._getUDTs()
        self.assertIsInstance(res, list)
        self.assertEqual(2, len(res))
        self.assertTrue(res[0]["name"] == "tuple1_all_types" or res[0]["name"] == "tuple2_simple")
        tuple2_simple = {'name': 'tuple2_simple',
            'fields': [{'fieldName': 'field1', 'fieldType': 'INT'},
                {'fieldName': 'field2', 'length': 10, 'fieldType': 'STRING'},
                {'fieldName': 'field3', 'fieldType': 'DATETIME'}]}
        self.assertTrue(res[0] == tuple2_simple or res[1] == tuple2_simple)

    def test_02_upsertAttrs(self):
        tests = [
            ({"attr_name": "attr_value"}, {"attr_name": {"value": "attr_value"}}),
            ({"attr_name1": "attr_value1", "attr_name2": "attr_value2"},
                {"attr_name1": {"value": "attr_value1"}, "attr_name2": {"value": "attr_value2"}}),
            ({"attr_name": ("attr_value", "operator")},
                {"attr_name": {"value": "attr_value", "op": "operator"}}),
            ({"attr_name1": ("attr_value1", "+"), "attr_name2": ("attr_value2", "-")},
                {"attr_name1": {"value": "attr_value1", "op": "+"},
                 "attr_name2": {"value": "attr_value2", "op": "-"}}),
            ("a string", {}),
            ({"attr_name"}, {}),
            (1, {}),
            ({}, {})
        ]

        for t in tests:
            res = self.conn._upsertAttrs(t[0])
            self.assertEqual(t[1], res)

    def test_03_getSchema(self):
        res = self.conn.getSchema()
        items = [
            ("GraphName"),
            ("VertexTypes", "Name", [
                "vertex1_all_types",
                "vertex2_primary_key",
                "vertex3_primary_key_composite",
                "vertex4",
                "vertex5",
                "vertex6",
                "vertex7"
            ]),
            ("EdgeTypes", "Name", [
                "edge1_undirected",
                "edge2_directed",
                "edge3_directed_with_reverse",
                "edge4_many_to_many",
                "edge5_all_to_all",
                "edge6_loop"
            ]),
            ("UDTs", "name", [
                "tuple1_all_types",
                "tuple2_simple"
            ])
        ]
        self.assertEqual(len(items), len(res))
        for i in items:
            if i == "GraphName":
                self.assertEqual(self.conn.graphname, res[i])
            else:
                self.assertIn(i[0], res)
                t = res[i[0]]
                self.assertIsInstance(t, list)
                self.assertEqual(len(i[2]), len(t))
                for tt in t:
                    self.assertIn(i[1], tt)
                    self.assertIn(tt[i[1]], i[2])

    def test_04_upsertData(self):
        data = {
            "vertices": {
                "vertex4": {
                    "4000": {
                        "a01": {
                            "value": 4000
                        }
                    },
                    "4001": {
                        "a01": {
                            "value": 4001
                        }
                    }
                },
                "vertex5": {
                    "5000": {},
                    "5001": {}
                }
            },
            "edges": {
                "vertex4": {
                    "4000": {
                        "edge2_directed": {
                            "vertex5": {
                                "5000": {
                                    "a01": {
                                        "value": 40005000
                                    }
                                },
                                "5001": {
                                    "a01": {
                                        "value": 40005001
                                    }
                                }
                            }
                        }
                    },
                    "4001": {
                        "edge3_directed_with_reverse": {
                            "vertex5": {
                                "5000": {
                                    "a01": {
                                        "value": 40005000
                                    }
                                },
                            }
                        }
                    }
                }
            }
        }
        res = self.conn.upsertData(data)
        self.assertEqual({"accepted_vertices": 4, "accepted_edges": 3}, res)

        res = self.conn.delVertices("vertex4", where="a01>1000")
        self.assertEqual(2, res)

        res = self.conn.delVerticesById("vertex5", [5000, 5001])
        self.assertEqual(2, res)

    def test_05_getEndpoints(self):
        res = self.conn.getEndpoints()
        self.assertIsInstance(res, dict)
        self.assertIn("GET /endpoints/{graph_name}", res)

        res = self.conn.getEndpoints(dynamic=True)
        self.assertEqual(4, len(res))


if __name__ == '__main__':
    unittest.main()
