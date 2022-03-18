import unittest

from pyTigerGraphUnitTest import pyTigerGraphUnitTest


class test_pyTigerGraphUDT(pyTigerGraphUnitTest):
    conn = None

    def test_01_getUDTs(self):
        ret = str(sorted(self.conn.getUDTs()))
        self.assertEqual(ret, "['tuple1_all_types', 'tuple2_simple']", "Invalid response")

    def test_02_getUDT(self):
        ret = str(self.conn.getUDT("tuple2_simple"))
        exp = "[{'fieldName': 'field1', 'fieldType': 'INT'}, {'fieldName': 'field2', 'length': 10, 'fieldType': 'STRING'}, {'fieldName': 'field3', 'fieldType': 'DATETIME'}]"
        self.assertEqual(ret, exp, "Invalid response")


if __name__ == '__main__':
    unittest.main()
