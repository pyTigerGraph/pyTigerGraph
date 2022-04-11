import unittest

from pyTigerGraph import TigerGraphConnection
from pyTigerGraph.gds.dataloaders import NeighborLoader
from torch_geometric.data import Data as pygData


class TestGDSNeighborLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
            kafka_address="18.117.192.44:9092",
        )
        self.assertTrue(loader._is_query_installed(loader.query_name))
        self.assertEqual(loader.num_batches, 9)

    def test_iterate_pyg(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
            kafka_address="18.117.192.44:9092",
        )
        for epoch in range(2):
            with self.subTest(i=epoch):
                num_batches = 0
                for data in loader:
                    # print(num_batches, data)
                    self.assertIsInstance(data, pygData)
                    self.assertIn("x", data)
                    self.assertIn("y", data)
                    self.assertIn("train_mask", data)
                    self.assertIn("val_mask", data)
                    self.assertIn("test_mask", data)
                    self.assertIn("is_seed", data)
                    num_batches += 1
                self.assertEqual(num_batches, 9)

    def test_whole_graph_pyg(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            num_batches=1,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
            kafka_address="18.117.192.44:9092",
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, pygData)
        self.assertIn("x", data)
        self.assertIn("y", data)
        self.assertIn("train_mask", data)
        self.assertIn("val_mask", data)
        self.assertIn("test_mask", data)
        self.assertIn("is_seed", data)


class TestGDSNeighborLoaderREST(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = TigerGraphConnection(host="http://35.230.92.92", graphname="Cora")
        # cls.conn.gsql("drop query all")

    def test_init(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        self.assertTrue(loader._is_query_installed(loader.query_name))
        self.assertEqual(loader.num_batches, 9)

    def test_iterate_pyg(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            batch_size=16,
            num_neighbors=10,
            num_hops=2,
            shuffle=True,
            filter_by="train_mask",
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
            self.assertIn("is_seed", data)
            num_batches += 1
        self.assertEqual(num_batches, 9)

    def test_whole_graph_pyg(self):
        loader = NeighborLoader(
            graph=self.conn,
            v_in_feats=["x"],
            v_out_labels=["y"],
            v_extra_feats=["train_mask", "val_mask", "test_mask"],
            num_batches=1,
            num_neighbors=10,
            num_hops=2,
            shuffle=False,
            filter_by="train_mask",
            output_format="PyG",
            add_self_loop=False,
            loader_id=None,
            buffer_size=4,
        )
        data = loader.data
        # print(data)
        self.assertIsInstance(data, pygData)
        self.assertIn("x", data)
        self.assertIn("y", data)
        self.assertIn("train_mask", data)
        self.assertIn("val_mask", data)
        self.assertIn("test_mask", data)
        self.assertIn("is_seed", data)


if __name__ == "__main__":
    suite = unittest.TestSuite()
    suite.addTest(TestGDSNeighborLoader("test_init"))
    suite.addTest(TestGDSNeighborLoader("test_iterate_pyg"))
    suite.addTest(TestGDSNeighborLoader("test_whole_graph_pyg"))
    suite.addTest(TestGDSNeighborLoaderREST("test_init"))
    suite.addTest(TestGDSNeighborLoaderREST("test_iterate_pyg"))
    suite.addTest(TestGDSNeighborLoaderREST("test_whole_graph_pyg"))

    runner = unittest.TextTestRunner(verbosity=2, failfast=True)
    runner.run(suite)
