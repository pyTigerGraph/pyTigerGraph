import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphUDT(pyTigerGraphUnitTest):
    conn = None

    def test_01_getUDTs(self):
        ret = self.conn.getUDTs()
        self.assertEqual(["tuple1_all_types", "tuple2_simple"], ret)

    def test_02_getUDT(self):
        ret = str(self.conn.getUDT("tuple2_simple"))
        exp = "[{'fieldName': 'field1', 'fieldType': 'INT'}, {'fieldName': 'field2', 'length': 10, 'fieldType': 'STRING'}, {'fieldName': 'field3', 'fieldType': 'DATETIME'}]"
        self.assertEqual(exp, ret)


if __name__ == '__main__':
    unittest.main()
