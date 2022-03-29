import json
import unittest

from pyTigerGraph.pyTigerGraphException import TigerGraphException
from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphBase(pyTigerGraphUnitTest):
    conn = None

    def test_00_errorCheck(self):
        json_ok1 = {
            "error": False,
            "message": "",
            "results": {
                "GraphName": "tests"
            }
        }

        json_ok2 = {
            "error": "false",
            "message": "",
            "results": {
                "GraphName": "tests"
            }
        }

        json_ok3 = {
            "error": "",
            "message": "",
            "results": {
                "GraphName": "tests"
            }
        }

        json_ok4 = {
            "message": "",
            "results": {
                "GraphName": "tests"
            }
        }

        json_not_ok1 = {
            "error": True,
            "message": "error message",
            "results": {}
        }

        json_not_ok2 = {
            "error": "true",
            "message": "error message",
            "code": "JB-007",
            "results": {}
        }

        self.conn._errorCheck(json_ok1)
        self.conn._errorCheck(json_ok2)
        self.conn._errorCheck(json_ok3)
        self.conn._errorCheck(json_ok4)

        with self.assertRaises(TigerGraphException) as tge:
            res = self.conn._errorCheck(json_not_ok1)
        self.assertEqual("error message", tge.exception.message)

        with self.assertRaises(TigerGraphException) as tge:
            res = self.conn._errorCheck(json_not_ok2)
        self.assertEqual("JB-007", tge.exception.code)

    def test_01_req(self):
        pass

    def test_02_get(self):
        exp = {'error': False, 'message': 'Hello GSQL'}
        res = self.conn._get(self.conn.restppUrl + "/echo/" + self.conn.graphname, resKey=None)
        self.assertEqual(exp, res)

    def test_03_post(self):
        exp = {'error': False, 'message': 'Hello GSQL'}
        res = self.conn._post(self.conn.restppUrl + "/echo/" + self.conn.graphname, resKey=None)
        self.assertEqual(exp, res)

        data = json.dumps({"function": "stat_vertex_attr", "type": "vertex4"})
        exp = [{'attributes': {'a01': {'AVG': 3, 'MAX': 5, 'MIN': 1}}, 'v_type': 'vertex4'}]
        res = self.conn._post(self.conn.restppUrl + "/builtins/" + self.conn.graphname, data=data)
        self.assertEqual(exp, res)

    def test_04_delete(self):
        with self.assertRaises(TigerGraphException) as tge:
            res = self.conn._delete(self.conn.restppUrl + "/graph/" + self.conn.graphname +
                "/vertices/non_existent_vertex_type/1")
        self.assertEqual("REST-30000", tge.exception.code)


if __name__ == '__main__':
    unittest.main()
