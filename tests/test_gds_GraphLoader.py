import unittest

from pandas import DataFrame
from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds.dataloaders import GraphLoader
from torch_geometric.data import Data as pygData


class TestGDSGraphLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="dataframe",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
            kafka_address="18.117.192.44:9092",
        )
        self.assertTrue(loader._is_query_installed(loader.query_name))
        self.assertEqual(loader.num_batches, 11)

    def test_iterate_pyg(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
            kafka_address="18.117.192.44:9092",
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygData)
            self.assertIn("x", data)
            self.assertIn("y", data)
            self.assertIn("train_mask", data)
            self.assertIn("val_mask", data)
            self.assertIn("test_mask", data)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    def test_iterate_df(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="dataframe",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
            kafka_address="18.117.192.44:9092",
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data[0], DataFrame)
            self.assertIsInstance(data[1], DataFrame)
            self.assertIn("x", data[0].columns)
            self.assertIn("y", data[0].columns)
            self.assertIn("train_mask", data[0].columns)
            self.assertIn("val_mask", data[0].columns)
            self.assertIn("test_mask", data[0].columns)
            num_batches += 1
        self.assertEqual(num_batches, 11)


class TestGDSGraphLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="dataframe",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(loader._is_query_installed(loader.query_name))
        self.assertEqual(loader.num_batches, 11)

    def test_iterate_pyg(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data, pygData)
            self.assertIn("x", data)
            self.assertIn("y", data)
            self.assertIn("train_mask", data)
            self.assertIn("val_mask", data)
            self.assertIn("test_mask", data)
            num_batches += 1
        self.assertEqual(num_batches, 11)

    def test_iterate_df(self):
        loader = GraphLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=1024,
            shuffle=True,
            filter_by=None,
            output_format="dataframe",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        num_batches = 0
        for data in loader:
            # print(num_batches, data)
            self.assertIsInstance(data[0], DataFrame)
            self.assertIsInstance(data[1], DataFrame)
            self.assertIn("x", data[0].columns)
            self.assertIn("y", data[0].columns)
            self.assertIn("train_mask", data[0].columns)
            self.assertIn("val_mask", data[0].columns)
            self.assertIn("test_mask", data[0].columns)
            num_batches += 1
        self.assertEqual(num_batches, 11)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSGraphLoader("test_init"))
    suite.addTest(TestGDSGraphLoader("test_iterate_pyg"))
    suite.addTest(TestGDSGraphLoader("test_iterate_df"))
    suite.addTest(TestGDSGraphLoaderREST("test_init"))
    suite.addTest(TestGDSGraphLoaderREST("test_iterate_pyg"))
    suite.addTest(TestGDSGraphLoaderREST("test_iterate_df"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
