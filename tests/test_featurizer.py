import unittest
import os
from os.path import join as pjoin
from unittest import runner
#from parso import split_lines
from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds import featurizer
from pyTigerGraph.gds.featurizer import Featurizer
from pyTigerGraph.gds.utilities import random_string


class test_Featurizer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        conn = TigerGraphConnection(host="http://34.232.63.121", graphname="BankSim")
        cls.featurizer = Featurizer(conn)

    def test_is_query_installed(self):
        self.assertFalse(self.featurizer._is_query_installed("not_listed"))

    def test_install_query_file(self):
        query_name = "tg_pagerank"
        resp = self.featurizer._install_query_file(query_name) 
        self.assertEqual(resp,"tg_pagerank")
        self.assertTrue(self.featurizer._is_query_installed("tg_pagerank"))

    def test_get_Params(self):
        _dict = {'v_type': None,
            'e_type': None,
            'max_change': 0.001,
            'max_iter': 25,
            'damping': 0.85,
            'top_k': 100,
            'print_accum': True,
            'result_attr': '', 
            'file_path': '',
            'display_edges': True}
        self.assertEqual(self.featurizer._get_Params("tg_pagerank"),_dict)

    def test01_add_attribute(self):
        try:
            tasks = "ALTER VERTEX Payer DROP ATTRIBUTE (attr1);"
            job_name = "drop_{}_attr_{}".format("VERTEX",random_string(6)) 
            job = "USE GRAPH {}\n".format(self.featurizer.conn.graphname) + "CREATE GLOBAL SCHEMA_CHANGE JOB {} {{\n".format(
                job_name) + ''.join(tasks) + "}}\nRUN GLOBAL SCHEMA_CHANGE JOB {}".format(job_name)
            # Submit the job
            resp = self.featurizer.conn.gsql(job)
            status = resp.splitlines()[-1]
            if "Failed" in status:
                raise ConnectionError(status)
            else:
                pass
        except:
            pass
        self.assertEqual( self.featurizer._add_attribute("VERTEX","FLOAT","attr1"),'Global schema change succeeded.')

    def test02_add_attribute(self):
        try:
            tasks = "ALTER Edge Trans DROP ATTRIBUTE (attr2);"
            job_name = "drop_{}_attr_{}".format("EDGE",random_string(6)) 
            job = "USE GRAPH {}\n".format(self.featurizer.conn.graphname) + "CREATE GLOBAL SCHEMA_CHANGE JOB {} {{\n".format(
                job_name) + ''.join(tasks) + "}}\nRUN GLOBAL SCHEMA_CHANGE JOB {}".format(job_name)
            # Submit the job
            resp = self.featurizer.conn.gsql(job)
            status = resp.splitlines()[-1]
            if "Failed" in status:
                raise ConnectionError(status)
            else:
                pass
        except:
            pass
        self.assertEqual(self.featurizer._add_attribute("Edge","BOOL","attr2"),'Global schema change succeeded.')
    
    def test03_add_attribute(self):
        self.assertEqual(self.featurizer._add_attribute("Vertex","BOOL","attr1"),'Attribute already exists')

    def test04_add_attribute(self):
        with self.assertRaises(Exception) as context:
            self.featurizer._add_attribute("Something","BOOL","attr3")
        self.assertTrue('schema_type has to be VERTEX or EDGE' in str(context.exception))
    
    def test05_add_attribute(self):
        try:
            tasks = "ALTER VERTEX Payer DROP ATTRIBUTE (attr4);"
            job_name = "drop_{}_attr_{}".format("VERTEX",random_string(6)) 
            job = "USE GRAPH {}\n".format(self.featurizer.conn.graphname) + "CREATE GLOBAL SCHEMA_CHANGE JOB {} {{\n".format(
                job_name) + ''.join(tasks) + "}}\nRUN GLOBAL SCHEMA_CHANGE JOB {}".format(job_name)
            # Submit the job
            resp = self.featurizer.conn.gsql(job)
            status = resp.splitlines()[-1]
            if "Failed" in status:
                raise ConnectionError(status)
            else:
                pass
        except:
            pass
        self.assertEqual(self.featurizer._add_attribute("VERTEX","BOOL","attr4",['Customer']),'Global schema change succeeded.')

    def test01_installAlgorithm(self):
       self.assertEqual(self.featurizer.installAlgorithm("tg_pagerank").strip(),"tg_pagerank")

    def test02_installAlgorithm(self):
        with self.assertRaises(Exception):
            self.featurizer.installAlgorithm("someQuery")
 
    def test01_runAlgorithm(self):
        try:
            tasks = "ALTER VERTEX Payer DROP ATTRIBUTE (pagerank);"
            job_name = "drop_{}_attr_{}".format("VERTEX",random_string(6)) 
            job = "USE GRAPH {}\n".format(self.featurizer.conn.graphname) + "CREATE GLOBAL SCHEMA_CHANGE JOB {} {{\n".format(
                job_name) + ''.join(tasks) + "}}\nRUN GLOBAL SCHEMA_CHANGE JOB {}".format(job_name)
            # Submit the job
            resp = self.featurizer.conn.gsql(job)
            status = resp.splitlines()[-1]
            if "Failed" in status:
                raise ConnectionError(status)
            else:
                pass
        except:
            pass
        params = {'v_type': 'Payer',
            'e_type': 'Trans',
            'max_change': 0.001,
            'max_iter': 25,
            'damping': 0.85,
            'top_k': 100,
            'print_accum': True,
            'result_attr': '', 
            'file_path': '',
            'display_edges': True}
        message = "Test value is not none."
        self.assertIsNotNone(self.featurizer.runAlgorithm("tg_pagerank",params=params,feat_name="pagerank",timeout=2147480),message)

    
    def test02_runAlgorithm(self):
        with self.assertRaises(ValueError):
            self.featurizer.runAlgorithm("tg_pagerank",timeout=2147480)

    def test03_runAlgorithm(self):
        params = {'v_type': 'Payer', 'e_type': ['Trans','reverse_Trans'], 'weights': '1,1,2', 'beta': -0.85, 'k': 3, 'reduced_dim': 128, 
          'sampling_constant': 1, 'random_seed': 42, 'print_accum': False,'result_attr':"",'file_path' :""}
        with self.assertRaises(Exception):
            self.featurizer.runAlgorithm("tg_fastRP",params=params,feat_name="fastrp_embedding",timeout=1)

    def test04_runAlgorithm(self):
        params = {'v_type': 'Payer', 'e_type': ['Trans','reverse_Trans'], 'weights': '1,1,2', 'beta': -0.85, 'k': 3, 'reduced_dim': 128, 
          'sampling_constant': 1, 'random_seed': 42, 'print_accum': False,'result_attr':"",'file_path' :""}
        with self.assertRaises(Exception):
            self.featurizer.runAlgorithm("tg_fastRP",params=params,feat_name="fastrp_embedding",sizeLimit=1)
    
    def test05_runAlgorithm(self):
        params = {'v_type': 'Payer',
            'e_type': 'Trans',
            'max_change': 0.001,
            'max_iter': 25,
            'damping': 0.85,
            'top_k': 100,
            'print_accum': True,
            'file_path': '',
            'display_edges': True}
        message = "Test value is not none."
        self.assertIsNotNone(self.featurizer.runAlgorithm("tg_pagerank",params=params,timeout=2147480),message)

    

if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(test_Featurizer("test_is_query_installed"))
    suite.addTest(test_Featurizer("test_install_query_file"))
    suite.addTest(test_Featurizer("test_get_Params"))
    suite.addTest(test_Featurizer("test01_add_attribute"))
    suite.addTest(test_Featurizer("test02_add_attribute"))
    suite.addTest(test_Featurizer("test03_add_attribute"))
    suite.addTest(test_Featurizer("test04_add_attribute"))
    suite.addTest(test_Featurizer("test01_installAlgorithm"))
    suite.addTest(test_Featurizer("test02_installAlgorithm"))
    suite.addTest(test_Featurizer("test01_runAlgorithm"))
    suite.addTest(test_Featurizer("test02_runAlgorithm"))
    suite.addTest(test_Featurizer("test03_runAlgorithm")) 
    suite.addTest(test_Featurizer("test04_runAlgorithm"))
    suite.addTest(test_Featurizer("test05_runAlgorithm"))
    


    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

    
    