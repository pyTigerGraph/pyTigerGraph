import unittest
import os
from os.path import join as pjoin
from unittest import runner
#from parso import split_lines
from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds.featurizer import Featurizer

class test_Featurizer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conn = TigerGraphConnection(host="http://34.232.63.121", graphname="BankSim2")
        cls.featurizer = Featurizer(conn)

    def test_is_query_installed(self):
        self.assertFalse(self.featurizer._is_query_installed("not_listed"))

    def test_install_query_file(self):
        query_path = pjoin(os.path.dirname(
            os.path.abspath(__file__)), "gsql", "degrees" + ".gsql")
        resp = self.featurizer._install_query_file(query_path) 
        self.assertEqual(resp,"degrees")
        self.assertTrue(self.featurizer._is_query_installed("degrees"))

    def test01_get_Params(self):
        algo_path = pjoin(os.path.dirname(
            os.path.abspath(__file__)), "gsql", "degrees" + ".gsql")
        self.local_gsql_path = algo_path
        self.assertEqual(self.featurizer._get_Params("degrees"),{})
    
    def test02_get_Params(self):
        query_path = pjoin(os.path.dirname(
            os.path.abspath(__file__)), "gsql", "tg_pagerank" + ".gsql")
        self.local_gsql_path = query_path
        _dict = {'v_type': None,
            'e_type': None,
            'max_change': 0.001,
            'max_iter': 25,
            'damping': 0.85,
            'top_k': 100,
            'print_accum': True,
            'display_edges': True}
        self.assertEqual(self.featurizer._get_Params("tg_pagerank"),_dict)

    def test01_add_attribute(self):
        self.assertEqual( self.featurizer._add_attribute("VERTEX","FLOAT","attr1"),'Global schema change succeeded.')

    def test02_add_attribute(self):
        self.assertEqual(self.featurizer._add_attribute("Edge","BOOL","attr2"),'Global schema change succeeded.')
    
    def test03_add_attribute(self):
        self.assertEqual(self.featurizer._add_attribute("Edge","BOOL","attr2"),'Attribute already exists')

    def test04_add_attribute(self):
        with self.assertRaises(Exception) as context:
            self.featurizer._add_attribute("Something","BOOL","attr3")
        self.assertTrue('schema_type has to be VERTEX or EDGE' in str(context.exception))
    
    def test05_add_attribute(self):
        self.assertEqual(self.featurizer._add_attribute("VERTEX","BOOL","attr4",'Customer'),'Global schema change succeeded.')

    def test_installAlgorithm(self):
       self.assertEqual(self.featurizer.installAlgorithm("tg_pagerank","VERTEX","pagerank").strip(),"tg_pagerank")
    
    def test01_runAlgorithm(self):
        algo_path = pjoin(os.path.dirname(
            os.path.abspath(__file__)), "gsql", "degrees" + ".gsql")
        self.local_gsql_path = algo_path
        params = self.featurizer._get_Params("degrees")
        self.assertEqual(self.featurizer.runAlgorithm("degrees",params),"Success!")

    def test02_runAlgorithm(self):
        params = {'v_type': 'Payer', 'e_type': 'Trans', 'max_change': 0.001, 'max_iter': 25, 'damping': 0.85, 'top_k': 100, 'print_accum': True, 'display_edges': True}
        self.assertEqual(self.featurizer.runAlgorithm("tg_pagerank",params),"Success!")

    
    def test03_runAlgorithm(self):
        algo_path = pjoin(os.path.dirname(
            os.path.abspath(__file__)), "gsql", "tg_pagerank" + ".gsql")
        self.local_gsql_path = algo_path
        params = self.featurizer._get_Params("tg_pagerank")
        with self.assertRaises(ValueError) as exception_context:
            self.featurizer.runAlgorithm("tg_pagerank",params)
        self.assertEqual(str(exception_context.exception), "Query parameters which are None need to be initialized.")

    

if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(test_Featurizer("test_is_query_installed"))
    suite.addTest(test_Featurizer("test_install_query_file"))
    suite.addTest(test_Featurizer("test01_get_Params"))
    suite.addTest(test_Featurizer("test02_get_Params"))
    suite.addTest(test_Featurizer("test01_add_attribute"))
    suite.addTest(test_Featurizer("test02_add_attribute"))
    suite.addTest(test_Featurizer("test03_add_attribute"))
    suite.addTest(test_Featurizer("test04_add_attribute"))
    suite.addTest(test_Featurizer("test05_add_attribute"))
    suite.addTest(test_Featurizer("test_installAlgorithm"))
    suite.addTest(test_Featurizer("test01_runAlgorithm"))
    suite.addTest(test_Featurizer("test02_runAlgorithm"))
    suite.addTest(test_Featurizer("test03_runAlgorithm")) 

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

    
    