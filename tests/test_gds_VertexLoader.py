import unittest

from pandas import DataFrame
from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds.dataloaders import VertexLoader


class TestGDSVertexLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
            kafka_address="18.117.192.44:9092",
        )
        self.assertTrue(loader._is_query_installed(loader.query_name))
        self.assertEqual(loader.num_batches, 9)

    def test_iterate(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
            kafka_address="18.117.192.44:9092",
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("x", data.columns)
            self.assertIn("y", data.columns)
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            num_batches += 1
        self.assertEqual(num_batches, 9)

    def test_all_vertices(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            num_batches=1,
            shuffle=False,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
            kafka_address="18.117.192.44:9092",
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)
        self.assertIn("x", data.columns)
        self.assertIn("y", data.columns)
        self.assertIn("train_mask", data.columns)
        self.assertIn("val_mask", data.columns)
        self.assertIn("test_mask", data.columns)


class TestGDSVertexLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(loader._is_query_installed(loader.query_name))
        self.assertEqual(loader.num_batches, 9)

    def test_iterate(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            batch_size=16,
            shuffle=True,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data.head())
            self.assertIsInstance(data, DataFrame)
            self.assertIn("x", data.columns)
            self.assertIn("y", data.columns)
            self.assertIn("train_mask", data.columns)
            self.assertIn("val_mask", data.columns)
            self.assertIn("test_mask", data.columns)
            num_batches += 1
        self.assertEqual(num_batches, 9)

    def test_all_vertices(self):
        loader = VertexLoader(
            graph=self.conn,
            attributes=["x", "y", "train_mask", "val_mask", "test_mask"],
            num_batches=1,
            shuffle=False,
            filter_by="train_mask",
            loader_id=None,
            buffer_size=4,
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, DataFrame)
        self.assertIn("x", data.columns)
        self.assertIn("y", data.columns)
        self.assertIn("train_mask", data.columns)
        self.assertIn("val_mask", data.columns)
        self.assertIn("test_mask", data.columns)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSVertexLoader("test_init"))
    suite.addTest(TestGDSVertexLoader("test_iterate"))
    suite.addTest(TestGDSVertexLoader("test_all_vertices"))
    suite.addTest(TestGDSVertexLoaderREST("test_init"))
    suite.addTest(TestGDSVertexLoaderREST("test_iterate"))
    suite.addTest(TestGDSVertexLoaderREST("test_all_vertices"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
