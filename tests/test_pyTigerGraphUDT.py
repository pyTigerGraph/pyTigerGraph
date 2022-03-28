import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphUDT(pyTigerGraphUnitTest):
    conn = None

    def test_01_getUDTs(self):
        res = self.conn.getUDTs()
        exp = ["tuple1_all_types", "tuple2_simple"]
        self.assertEqual(exp, res)

    def test_02_getUDT(self):
        res = self.conn.getUDT("tuple2_simple")
        exp = [{'fieldName': 'field1', 'fieldType': 'INT'},
            {'fieldName': 'field2', 'length': 10, 'fieldType': 'STRING'},
            {'fieldName': 'field3', 'fieldType': 'DATETIME'}]
        self.assertEqual(exp, res)


if __name__ == '__main__':
    unittest.main()
