"""Graph Data Science Functions"""
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection

from .dataloaders import EdgeLoader, GraphLoader, NeighborLoader, VertexLoader


class GDS:
    """Graph Data Science Functions"""

    def __init__(self, conn: TigerGraphConnection) -> None:
        """Initiate a GDS object.
            Args:
                conn (TigerGraphConnection):
                    Accept a TigerGraphConnection to run queries with
            Returns:
                None
        """
        self.conn = conn

    def neighborLoader(
        self,
        v_in_feats: Union[list, dict] = None,
        v_out_labels: Union[list, dict] = None,
        v_extra_feats: Union[list, dict] = None,
        batch_size: int = None,
        num_batches: int = 1,
        num_neighbors: int = 10,
        num_hops: int = 2,
        shuffle: bool = False,
        filter_by: str = None,
        output_format: str = "PyG",
        add_self_loop: bool = False,
        loader_id: str = None,
        buffer_size: int = 4,
        kafka_address: str = None,
        kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = True,
        kafka_address_consumer: str = None,
        kafka_address_producer: str = None,
        timeout: int = 300000,
    ) -> NeighborLoader:
        """Get a graph loader that performs neighbor sampling as introduced in the
        [Inductive Representation Learning on Large Graphs](https://arxiv.org/abs/1706.02216)
        paper.

        Specifically, the loader first chooses `batch_size` number of vertices as seeds,
        then picks `num_neighbors` number of neighbors of each seed at random,
        then `num_neighbors` neighbors of each neighbor, and repeat for `num_hops`.
        This generates one subgraph. As you loop through this data loader, every
        vertex will at some point be chosen as a seed and you will get the subgraph
        expanded from the seed. If you want to limit seeds to certain vertices, the boolean
        attribute provided to `filter_by` will be used to indicate which vertices can be
        included as seeds.

        **Note**: For the first time you initialize the loader on a graph in TigerGraph,
        the initialization might take a minute as it installs the corresponding
        query to the database and optimizes it. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same TG graph again.

        There are two ways to use the data loader. See
        [here](https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/main/tutorials/basics/2_dataloaders.ipynb)
        for examples.

        * First, it can be used as an iterable, which means you can loop through
          it to get every batch of data. If you load all data at once (`num_batches=1`),
          there will be only one batch (of all the data) in the iterator.
        * Second, you can access the `data` property of the class directly. If there is
          only one batch of data to load, it will give you the batch directly instead
          of an iterator, which might make more sense in that case. If there are
          multiple batches of data to load, it will return the loader itself.

        Args:
            v_in_feats (list, optional):
                Vertex attributes to be used as input features.
                Only numeric and boolean attributes are allowed. The type of an attrbiute
                is automatically determined from the database schema. Defaults to None.
            v_out_labels (list, optional):
                Vertex attributes to be used as labels for
                prediction. Only numeric and boolean attributes are allowed. Defaults to None.
            v_extra_feats (list, optional): 
                Other attributes to get such as indicators of
                train/test data. All types of attributes are allowed. Defaults to None.
            batch_size (int, optional):
                Number of vertices as seeds in each batch.
                Defaults to None.
            num_batches (int, optional):
                Number of batches to split the vertices into as seeds.
                If both `batch_size` and `num_batches` are provided, `batch_size` takes higher
                priority. Defaults to 1.
            num_neighbors (int, optional): 
                Number of neighbors to sample for each vertex.
                Defaults to 10.
            num_hops (int, optional): 
                Number of hops to traverse when sampling neighbors.
                Defaults to 2.
            shuffle (bool, optional): 
                Whether to shuffle the vertices before loading data.
                Defaults to False.
            filter_by (str, optional): 
                A boolean attribute used to indicate which vertices
                can be included as seeds. Defaults to None.
            output_format (str, optional): 
                Format of the output data of the loader. Only
                "PyG", "DGL" and "dataframe" are supported. Defaults to "PyG".
            add_self_loop (bool, optional): 
                Whether to add self-loops to the graph. Defaults to False.
            loader_id (str, optional): 
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional): 
                Number of data batches to prefetch and store in memory. Defaults to 4.
            kafka_address (str, optional): 
                Address of the kafka broker. Defaults to None.
            kafka_max_msg_size (int, optional):
                Maximum size of a Kafka message in bytes.
                Defaults to 104857600.
            kafka_num_partitions (int, optional):
                Number of partitions for the topic created by this loader.
                Defaults to 1.
            kafka_replica_factor (int, optional):
                Number of replications for the topic created by this
                loader. Defaults to 1.
            kafka_retention_ms (int, optional): 
                Retention time for messages in the topic created by this
                loader in milliseconds. Defaults to 60000.
            kafka_auto_del_topic (bool, optional): 
                Whether to delete the Kafka topic once the
                loader finishes pulling data. Defaults to True.
            kafka_address_consumer (str, optional):
                Address of the kafka broker that a consumer
                should use. Defaults to be the same as `kafkaAddress`.
            kafka_address_producer (str, optional):
                Address of the kafka broker that a producer
                should use. Defaults to be the same as `kafkaAddress`.
            timeout (int, optional): 
                Timeout value for GSQL queries, in ms. Defaults to 300000.
          
        Returns:
            NeighborLoader
        """
        return NeighborLoader(
            self.conn,
            v_in_feats,
            v_out_labels,
            v_extra_feats,
            batch_size,
            num_batches,
            num_neighbors,
            num_hops,
            shuffle,
            filter_by,
            output_format,
            add_self_loop,
            loader_id,
            buffer_size,
            kafka_address,
            kafka_max_msg_size,
            kafka_num_partitions,
            kafka_replica_factor,
            kafka_retention_ms,
            kafka_auto_del_topic,
            kafka_address_consumer,
            kafka_address_producer,
            timeout,
        )

    def edgeLoader(
        self,
        batch_size: int = None,
        num_batches: int = 1,
        shuffle: bool = False,
        filter_by: str = None,
        output_format: str = "dataframe",
        loader_id: str = None,
        buffer_size: int = 4,
        kafka_address: str = None,
        kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = True,
        kafka_address_consumer: str = None,
        kafka_address_producer: str = None,
        timeout: int = 300000,
    ) -> EdgeLoader:
        """Get a graph loader that pulls batches of edges from database.
        Edge attributes are not supported.

        Specifically, it divides edges into `num_batches` and returns each batch separately.
        The boolean attribute provided to `filter_by` indicates which edges are included.
        If you need random batches, set `shuffle` to True.

        **Note**: For the first time you initialize the loader on a graph in TigerGraph,
        the initialization might take a minute as it installs the corresponding
        query to the database and optimizes it. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same TG graph again.

        There are two ways to use the data loader. See
        [here](https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/main/tutorials/basics/2_dataloaders.ipynb)
        for examples.

        * First, it can be used as an iterable, which means you can loop through
          it to get every batch of data. If you load all edges at once (`num_batches=1`),
          there will be only one batch (of all the edges) in the iterator.
        * Second, you can access the `data` property of the class directly. If there is
          only one batch of data to load, it will give you the batch directly instead
          of an iterator, which might make more sense in that case. If there are
          multiple batches of data to load, it will return the loader again.

        Args:
            batch_size (int, optional):
                Number of edges in each batch.
                Defaults to None.
            num_batches (int, optional):
                Number of batches to split the edges.
                Defaults to 1.
            shuffle (bool, optional):
                Whether to shuffle the edges before loading data.
                Defaults to False.
            filter_by (str, optional):
                A boolean attribute used to indicate which edges
                are included. Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader. Only
                "dataframe" is supported. Defaults to "dataframe".
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            kafka_address (str, optional):
                Address of the kafka broker. Defaults to None.
            kafka_max_msg_size (int, optional):
                Maximum size of a Kafka message in bytes.
                Defaults to 104857600.
            kafka_num_partitions (int, optional):
                Number of partitions for the topic created by this loader.
                Defaults to 1.
            kafka_replica_factor (int, optional):
                Number of replications for the topic created by this
                loader. Defaults to 1.
            kafka_retention_ms (int, optional):
                Retention time for messages in the topic created by this
                loader in milliseconds. Defaults to 60000.
            kafka_auto_del_topic (bool, optional):
                Whether to delete the Kafka topic once the
                loader finishes pulling data. Defaults to True.
            kafka_address_consumer (str, optional):
                Address of the kafka broker that a consumer
                should use. Defaults to be the same as `kafkaAddress`.
            kafka_address_producer (str, optional):
                Address of the kafka broker that a producer
                should use. Defaults to be the same as `kafkaAddress`.
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
        
        Returns:
            EdgeLoader
        """
        return EdgeLoader(
            self.conn,
            batch_size,
            num_batches,
            shuffle,
            filter_by,
            output_format,
            loader_id,
            buffer_size,
            kafka_address,
            kafka_max_msg_size,
            kafka_num_partitions,
            kafka_replica_factor,
            kafka_retention_ms,
            kafka_auto_del_topic,
            kafka_address_consumer,
            kafka_address_producer,
            timeout,
        )

    def vertexLoader(
        self,
        attributes: Union[list, dict] = None,
        batch_size: int = None,
        num_batches: int = 1,
        shuffle: bool = False,
        filter_by: str = None,
        output_format: str = "dataframe",
        loader_id: str = None,
        buffer_size: int = 4,
        kafka_address: str = None,
        kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = True,
        kafka_address_consumer: str = None,
        kafka_address_producer: str = None,
        timeout: int = 300000,
    ) -> VertexLoader:
        """Get a data loader that pulls batches of vertices from database.

        Specifically, it divides vertices into `num_batches` and returns each batch separately.
        The boolean attribute provided to `filter_by` indicates which vertices are included.
        If you need random batches, set `shuffle` to True.

        **Note**: For the first time you initialize the loader on a graph in TigerGraph,
        the initialization might take a minute as it installs the corresponding
        query to the database and optimizes it. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same TG graph again.

        There are two ways to use the data loader.
        See [here](https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/main/tutorials/basics/2_dataloaders.ipynb)
        for examples.

        * First, it can be used as an iterable, which means you can loop through
          it to get every batch of data. If you load all vertices at once (`num_batches=1`),
          there will be only one batch (of all the vertices) in the iterator.
        * Second, you can access the `data` property of the class directly. If there is
          only one batch of data to load, it will give you the batch directly instead
          of an iterator, which might make more sense in that case. If there are
          multiple batches of data to load, it will return the loader again.

        Args:
            attributes (list, optional):
                Vertex attributes to be included. Defaults to None.
            batch_size (int, optional): 
                Number of vertices in each batch.
                Defaults to None.
            num_batches (int, optional):
                Number of batches to split the vertices.
                Defaults to 1.
            shuffle (bool, optional):
                Whether to shuffle the vertices before loading data.
                Defaults to False.
            filter_by (str, optional):
                A boolean attribute used to indicate which vertices
                can be included. Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader. Only
                "dataframe" is supported. Defaults to "dataframe".
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            kafka_address (str, optional):
                Address of the kafka broker. Defaults to None.
            kafka_max_msg_size (int, optional):
                Maximum size of a Kafka message in bytes.
                Defaults to 104857600.
            kafka_num_partitions (int, optional):
                Number of partitions for the topic created by this loader.
                Defaults to 1.
            kafka_replica_factor (int, optional):
                Number of replications for the topic created by this
                loader. Defaults to 1.
            kafka_retention_ms (int, optional):
                Retention time for messages in the topic created by this
                loader in milliseconds. Defaults to 60000.
            kafka_auto_del_topic (bool, optional):
                Whether to delete the Kafka topic once the
                loader finishes pulling data. Defaults to True.
            kafka_address_consumer (str, optional):
                Address of the kafka broker that a consumer
                should use. Defaults to be the same as `kafkaAddress`.
            kafka_address_producer (str, optional):
                Address of the kafka broker that a producer
                should use. Defaults to be the same as `kafkaAddress`.
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
          
        Returns:
            VertexLoader
        """
        return VertexLoader(
            self.conn,
            attributes,
            batch_size,
            num_batches,
            shuffle,
            filter_by,
            output_format,
            loader_id,
            buffer_size,
            kafka_address,
            kafka_max_msg_size,
            kafka_num_partitions,
            kafka_replica_factor,
            kafka_retention_ms,
            kafka_auto_del_topic,
            kafka_address_consumer,
            kafka_address_producer,
            timeout,
        )

    def graphLoader(
        self,
        v_in_feats: Union[list, dict] = None,
        v_out_labels: Union[list, dict] = None,
        v_extra_feats: Union[list, dict] = None,
        batch_size: int = None,
        num_batches: int = 1,
        shuffle: bool = False,
        filter_by: str = None,
        output_format: str = "PyG",
        add_self_loop: bool = False,
        loader_id: str = None,
        buffer_size: int = 4,
        kafka_address: str = None,
        kafka_max_msg_size: int = 104857600,
        kafka_num_partitions: int = 1,
        kafka_replica_factor: int = 1,
        kafka_retention_ms: int = 60000,
        kafka_auto_del_topic: bool = True,
        kafka_address_consumer: str = None,
        kafka_address_producer: str = None,
        timeout: int = 300000,
    ) -> GraphLoader:
        """Get a data loader that pulls batches of vertices and edges from database.

        Different from NeighborLoader which produces connected subgraphs, this loader
        generates (random) batches of edges and vertices attached to those edges.

        **Note**: For the first time you initialize the loader on a graph in TigerGraph,
        the initialization might take a minute as it installs the corresponding
        query to the database and optimizes it. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same TG graph again.

        There are two ways to use the data loader. See [here](https://github.com/TigerGraph-DevLabs/mlworkbench-docs/blob/main/tutorials/basics/2_dataloaders.ipynb)
        for examples.

        * First, it can be used as an iterable, which means you can loop through
          it to get every batch of data. If you load all data at once (`num_batches=1`),
          there will be only one batch (of all the data) in the iterator.
        * Second, you can access the `data` property of the class directly. If there is
          only one batch of data to load, it will give you the batch directly instead
          of an iterator, which might make more sense in that case. If there are
          multiple batches of data to load, it will return the loader itself.

        Args:
            v_in_feats (list, optional):
                Vertex attributes to be used as input features.
                Only numeric and boolean attributes are allowed. The type of an attrbiute
                is automatically determined from the database schema. Defaults to None.
            v_out_labels (list, optional):
                Vertex attributes to be used as labels for
                prediction. Only numeric and boolean attributes are allowed. Defaults to None.
            v_extra_feats (list, optional):
                Other attributes to get such as indicators of
                train/test data. All types of attributes are allowed. Defaults to None.
            batch_size (int, optional):
                Number of edges in each batch.
                Defaults to None.
            num_batches (int, optional):
                Number of batches to split the edges.
                Defaults to 1.
            shuffle (bool, optional):
                Whether to shuffle the data before loading.
                Defaults to False.
            filter_by (str, optional):
                A boolean attribute used to indicate which edges
                can be included. Defaults to None.
            output_format (str, optional):
                Format of the output data of the loader. Only
                "PyG", "DGL" and "dataframe" are supported. Defaults to "dataframe".
            add_self_loop (bool, optional):
                Whether to add self-loops to the graph. Defaults to False.
            loader_id (str, optional):
                An identifier of the loader which can be any string. It is
                also used as the Kafka topic name. If `None`, a random string will be generated
                for it. Defaults to None.
            buffer_size (int, optional):
                Number of data batches to prefetch and store in memory. Defaults to 4.
            kafka_address (str, optional):
                Address of the kafka broker. Defaults to None.
            kafka_max_msg_size (int, optional):
                Maximum size of a Kafka message in bytes.
                Defaults to 104857600.
            kafka_num_partitions (int, optional):
                Number of partitions for the topic created by this loader.
                Defaults to 1.
            kafka_replica_factor (int, optional):
                Number of replications for the topic created by this
                loader. Defaults to 1.
            kafka_retention_ms (int, optional):
                Retention time for messages in the topic created by this
                loader in milliseconds. Defaults to 60000.
            kafka_auto_del_topic (bool, optional):
                Whether to delete the Kafka topic once the
                loader finishes pulling data. Defaults to True.
            kafka_address_consumer (str, optional):
                Address of the kafka broker that a consumer
                should use. Defaults to be the same as `kafkaAddress`.
            kafka_address_producer (str, optional):
                Address of the kafka broker that a producer
                should use. Defaults to be the same as `kafkaAddress`.
            timeout (int, optional):
                Timeout value for GSQL queries, in ms. Defaults to 300000.
        
        Returns:
          GraphLoader
        """
        return GraphLoader(
            self.conn,
            v_in_feats,
            v_out_labels,
            v_extra_feats,
            batch_size,
            num_batches,
            shuffle,
            filter_by,
            output_format,
            add_self_loop,
            loader_id,
            buffer_size,
            kafka_address,
            kafka_max_msg_size,
            kafka_num_partitions,
            kafka_replica_factor,
            kafka_retention_ms,
            kafka_auto_del_topic,
            kafka_address_consumer,
            kafka_address_producer,
            timeout,
        )
