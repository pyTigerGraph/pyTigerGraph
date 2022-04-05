import unittest

from pandas import DataFrame
from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds.dataloaders import EdgeLoader


class TestGDSEdgeLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=False,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
            kafka_address="18.117.192.44:9092",
        )
        self.assertTrue(loader._is_query_installed(loader.query_name))
        self.assertEqual(loader.num_batches, 11)

    def test_iterate(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
            kafka_address="18.117.192.44:9092",
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    def test_whole_edgelist(self):
        loader = EdgeLoader(
            graph=self.conn,
            num_batches=1,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
            kafka_address="18.117.192.44:9092",
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)

    # TODO: test filter_by


class TestGDSEdgeLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=False,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(loader._is_query_installed(loader.query_name))
        self.assertEqual(loader.num_batches, 11)

    def test_iterate(self):
        loader = EdgeLoader(
            graph=self.conn,
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    def test_whole_edgelist(self):
        loader = EdgeLoader(
            graph=self.conn,
            num_batches=1,
            shuffle=True,
            filter_by=None,
            loader_id=None,
            buffer_size=4,
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)

    # TODO: test filter_by


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSEdgeLoader("test_init"))
    suite.addTest(TestGDSEdgeLoader("test_iterate"))
    suite.addTest(TestGDSEdgeLoader("test_whole_edgelist"))
    suite.addTest(TestGDSEdgeLoaderREST("test_init"))
    suite.addTest(TestGDSEdgeLoaderREST("test_iterate"))
    suite.addTest(TestGDSEdgeLoaderREST("test_whole_edgelist"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
