import io
import logging
import math
import os
import re
from argparse import ArgumentError
from queue import Empty, Queue
from threading import Event, Thread
from time import sleep
from typing import NoReturn, Union

import numpy as np
import pandas as pd
import torch
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic

from ..pyTigerGraph import TigerGraphConnection
from ..pyTigerGraphException import TigerGraphException
from .utilities import random_string

__all__ = ["VertexLoader", "EdgeLoader", "NeighborLoader"]
__pdoc__ = {}

_udf_funcs = {
    "INT": "int_to_string",
    "BOOL": "bool_to_string",
    "FLOAT": "float_to_string",
    "DOUBLE": "float_to_string",
}


class BaseLoader:
    def __init__(
        self,
        graph: TigerGraphConnection,
        loaderID: str = None,
        numBatches: int = 1,
        bufferSize: int = 4,
        outputFormat: str = "dataframe",
        kafkaAddress: str = "",
        KafkaMaxMsgSize: int = 104857600,
        kafkaNumPartitions: int = 1,
        kafkaReplicaFactor: int = 1,
        kafkaRetentionMS: int = 60000,
        kafkaAutoDelTopic: bool = True,
        kafkaAddressForConsumer: str = None,
        kafkaAddressForProducer: str = None,
        timeout: int = 300000,
    ):
        """Base Class for data loaders.

        The job of a data loader is to stream data from the TigerGraph database to the client.
        Kafka is used as the data streaming pipeline. Hence, for the data loader to work,
        a running Kafka cluster is required.

        **Note**: For the first time you initialize the loader on a graph in TigerGraph,
        the initialization might take a minute as it installs the corresponding
        query to the database and optimizes it. However, the query installation only
        needs to be done once, so it will take no time when you initialize the loader
        on the same TG graph again.

        Args:
            graph (TigerGraphConnection): Connection to the TigerGraph database.
            loaderID (str): An identifier of the loader which can be any string. It is
                also used as the Kafka topic name. If `None`, a random string
                will be generated for it. Defaults to None.
            numBatches (int): Number of batches to divide the desired data into. Defaults to 1.
            bufferSize (int): Number of data batches to prefetch and store in memory. Defaults to 4.
            outputFormat (str): Format of the output data of the loader. Defaults to dataframe.
            kafkaAddress (str): Address of the kafka broker. Defaults to localhost:9092.
            maxKafkaMsgSize (int, optional): Maximum size of a Kafka message in bytes.
                Defaults to 104857600.
            kafkaNumPartitions (int, optional): Number of partitions for the topic created by this loader.
                Defaults to 1.
            kafkaReplicaFactor (int, optional): Number of replications for the topic created by this
                loader. Defaults to 1.
            kafkaRetentionMS (int, optional): Retention time for messages in the topic created by this
                loader in milliseconds. Defaults to 60000.
            kafkaAutoDelTopic (bool, optional): Whether to delete the kafka topics created for this loader.
                Defaults to True.
            kafkaAddressForConsumer (str, optional): Address of the kafka broker that a consumer
                should use. Defaults to be the same as `kafkaAddress`.
            kafkaAddressForProducer (str, optional): Address of the kafka broker that a producer
                should use. Defaults to be the same as `kafkaAddress`.
            timeout (int, optional): Timeout value for GSQL queries, in ms. Defaults to 300000.
        """
        # Get graph info
        self._graph = graph
        self._v_schema, _ = self._get_schema()
        # Initialize basic params
        if not loaderID:
            self.loader_id = random_string(6)
        else:
            self.loader_id = loaderID
        self.num_batches = numBatches
        self.output_format = outputFormat
        self.buffer_size = bufferSize
        self.timeout = timeout
        self._iterations = 0
        self._iterator = False
        # Kafka consumer and admin
        self.max_kafka_msg_size = KafkaMaxMsgSize
        self.kafka_address_consumer = (
            kafkaAddressForConsumer if kafkaAddressForConsumer else kafkaAddress
        )
        self.kafka_address_producer = (
            kafkaAddressForProducer if kafkaAddressForProducer else kafkaAddress
        )
        if self.kafka_address_consumer:
            try:
                self._kafka_consumer = KafkaConsumer(
                    bootstrap_servers=self.kafka_address_consumer,
                    client_id=self.loader_id,
                    max_partition_fetch_bytes=KafkaMaxMsgSize,
                    fetch_max_bytes=KafkaMaxMsgSize,
                    auto_offset_reset="earliest",
                )
                self._kafka_admin = KafkaAdminClient(
                    bootstrap_servers=self.kafka_address_consumer,
                    client_id=self.loader_id,
                )
            except:
                raise ConnectionError(
                    "Cannot reach Kafka broker. Please check Kafka settings."
                )
        self.kafka_partitions = kafkaNumPartitions
        self.kafka_replica = kafkaReplicaFactor
        self.kafka_retention_ms = kafkaRetentionMS
        self.delete_kafka_topic = kafkaAutoDelTopic
        # Thread to send requests, download and load data
        self._requester = None
        self._downloader = None
        self._reader = None
        # Queues to store tasks and data
        self._request_task_q = None
        self._download_task_q = None
        self._read_task_q = None
        self._data_q = None
        self._kafka_topic = None
        # Exit signal to terminate threads
        self._exit_event = None
        # In-memory data cache. Only used if num_batches=1
        self._data = None
        # Default mode of the loader is for training
        self._mode = "training"
        # Implement `_install_query()` that installs your query
        # self._install_query()

    def __del__(self):
        self._reset()

    def _get_schema(self):
        v_schema = {}
        e_schema = {}
        schema = self._graph.getSchema()
        for vtype in schema["VertexTypes"]:
            v = vtype["Name"]
            v_schema[v] = {}
            for attr in vtype["Attributes"]:
                if "ValueTypeName" in attr["AttributeType"]:
                    v_schema[v][attr["AttributeName"]] = attr["AttributeType"][
                        "ValueTypeName"
                    ]
                else:
                    v_schema[v][attr["AttributeName"]] = attr["AttributeType"]["Name"]
            if vtype["PrimaryId"]["PrimaryIdAsAttribute"]:
                v_schema[v][vtype["PrimaryId"]["AttributeName"]] = vtype["PrimaryId"][
                    "AttributeType"
                ]["Name"]
        return v_schema, e_schema

    def _validate_vertex_attributes(
        self, attributes: Union[list, dict]
    ) -> Union[list, dict]:
        if not attributes:
            return []
        if isinstance(attributes, str):
            raise ArgumentError(
                "The old string way of specifying attributes is deprecated to better support heterogeneous graphs. Please use the new format."
            )
        if isinstance(attributes, list):
            for i in range(len(attributes)):
                attributes[i] = attributes[i].strip()
            attr_set = set(attributes)
            for vtype in self._v_schema:
                allowlist = set(self._v_schema[vtype].keys())
                if attr_set - allowlist:
                    raise ArgumentError(
                        "Not all attributes are available for vertex type {}.".format(
                            vtype
                        )
                    )
        elif isinstance(attributes, dict):
            # Wait for the heterogeneous graph support
            for vtype in attributes:
                if vtype not in self._v_schema:
                    raise ArgumentError(
                        "Vertex type {} is not available in the database.".format(vtype)
                    )
                for i in range(len(attributes[vtype])):
                    attributes[vtype][i] = attributes[vtype][i].strip()
                attr_set = set(attributes[vtype])
                allowlist = set(self._v_schema[vtype].keys())
                if attr_set - allowlist:
                    raise ArgumentError(
                        "Not all attributes are available for vertex type {}.".format(
                            vtype
                        )
                    )
            raise NotImplementedError
        return attributes

    def _install_query(self):
        # Install the right GSQL query for the loader.
        self.query_name = ""
        raise NotImplementedError

    def _is_query_installed(self, query_name: str) -> bool:
        target = "GET /query/{}/{}".format(self._graph.graphname, query_name)
        queries = self._graph.getInstalledQueries()
        return target in queries

    def _install_query_file(self, file_path: str, replace: dict = None):
        # Read the first line of the file to get query name. The first line should be
        # something like CREATE QUERY query_name (...
        with open(file_path) as infile:
            firstline = infile.readline()
        try:
            query_name = re.search("QUERY (.+?)\(", firstline).group(1).strip()
        except:
            raise ValueError(
                "Cannot parse the query file. It should start with CREATE QUERY ... "
            )
        # If a suffix is to be added to query name
        if replace and ("{QUERYSUFFIX}" in replace):
            query_name = query_name.replace("{QUERYSUFFIX}", replace["{QUERYSUFFIX}"])
        # If query is already installed, skip.
        if self._is_query_installed(query_name):
            return query_name
        # Otherwise, install the query from file
        with open(file_path) as infile:
            query = infile.read()
        # Replace placeholders with actual content if given
        if replace:
            for placeholder in replace:
                query = query.replace(placeholder, replace[placeholder])
        # TODO: Check if Distributed query is needed.
        query = (
            "USE GRAPH {}\n".format(self._graph.graphname)
            + query
            + "\nInstall Query {}\n".format(query_name)
        )
        print(
            "Installing and optimizing queries. It might take a minute if this is the first time you use this loader."
        )
        resp = self._graph.gsql(query)
        status = resp.splitlines()[-1]
        if "Failed" in status:
            raise ConnectionError(status)
        else:
            print(status)
        return query_name

    @staticmethod
    def _request_kafka(
        exit_event: Event,
        tgraph: TigerGraphConnection,
        query_name: str,
        kafka_consumer: KafkaConsumer,
        kafka_admin: KafkaAdminClient,
        kafka_topic: str,
        kafka_partitions: int = 1,
        kafka_replica: int = 1,
        kafka_topic_size: int = 100000000,
        kafka_retention_ms: int = 60000,
        timeout: int = 600000,
        payload: dict = {},
        headers: dict = {},
    ) -> None:
        # Create topic if not exist
        if kafka_topic not in kafka_consumer.topics():
            new_topic = NewTopic(
                kafka_topic,
                kafka_partitions,
                kafka_replica,
                topic_configs={
                    "retention.ms": str(kafka_retention_ms),
                    "max.message.bytes": str(kafka_topic_size),
                },
            )
            resp = kafka_admin.create_topics([new_topic])
            if resp.to_object()["topic_errors"][0]["error_code"] != 0:
                raise ConnectionError(
                    "Failed to create Kafka topic {} at {}.".format(
                        kafka_topic, kafka_consumer.config["bootstrap_servers"]
                    )
                )
        # Subscribe to the topic
        kafka_consumer.subscribe([kafka_topic])

        # Run query async
        # TODO: change to runInstalledQuery when it supports async mode
        _headers = {"GSQL-ASYNC": "true", "GSQL-TIMEOUT": str(timeout)}
        _headers.update(headers)
        _payload = {}
        _payload.update(payload)
        resp = tgraph._post(
            tgraph.restppUrl + "/query/" + tgraph.graphname + "/" + query_name,
            data=_payload,
            headers=_headers,
            resKey=None,
        )
        # Check status
        _stat_payload = {
            "graph_name": tgraph.graphname,
            "requestid": resp["request_id"],
        }
        while not exit_event.is_set():
            status = tgraph._get(
                tgraph.restppUrl + "/query_status", params=_stat_payload
            )
            if status[0]["status"] == "running":
                sleep(1)
                continue
            elif status[0]["status"] == "success":
                res = tgraph._get(
                    tgraph.restppUrl + "/query_result", params=_stat_payload
                )
                if res[0]["kafkaError"]:
                    raise TigerGraphException(
                        "Error writing to Kafka: {}".format(res[0]["kafkaError"])
                    )
                else:
                    break
            else:
                raise TigerGraphException(
                    "Error generating data. Query {}.".format(
                        status["results"][0]["status"]
                    )
                )

    @staticmethod
    def _request_rest(
        tgraph: TigerGraphConnection,
        query_name: str,
        read_task_q: Queue,
        timeout: int = 600000,
        payload: dict = {},
        resp_type: str = "both",
    ) -> None:
        # Run query
        resp = tgraph.runInstalledQuery(
            query_name, params=payload, timeout=timeout, usePost=True
        )
        # Put raw data into reading queue
        for i in resp:
            if resp_type == "both":
                data = ("".join(i["vertex_batch"].values()), i["edge_batch"])
            elif resp_type == "vertex":
                data = "".join(i["vertex_batch"].values())
            elif resp_type == "edge":
                data = i["edge_batch"]
            read_task_q.put(data)
        read_task_q.put(None)

    @staticmethod
    def _download_from_kafka(
        exit_event: Event,
        read_task_q: Queue,
        num_batches: int,
        out_tuple: bool,
        kafka_consumer: KafkaConsumer,
    ) -> None:
        delivered_batch = 0
        buffer = {}
        while not exit_event.is_set():
            if delivered_batch == num_batches:
                break
            resp = kafka_consumer.poll(1000)
            if not resp:
                continue
            for msgs in resp.values():
                for message in msgs:
                    key = message.key.decode("utf-8")
                    if out_tuple:
                        if key.startswith("vertex"):
                            companion_key = key.replace("vertex", "edge")
                            if companion_key in buffer:
                                read_task_q.put((message.value, buffer[companion_key]))
                                del buffer[companion_key]
                                delivered_batch += 1
                            else:
                                buffer[key] = message.value
                        elif key.startswith("edge"):
                            companion_key = key.replace("edge", "vertex")
                            if companion_key in buffer:
                                read_task_q.put((buffer[companion_key], message.value))
                                del buffer[companion_key]
                                delivered_batch += 1
                            else:
                                buffer[key] = message.value
                        else:
                            raise ValueError(
                                "Unrecognized key {} for messages in kafka".format(key)
                            )
                    else:
                        read_task_q.put(message.value)
                        delivered_batch += 1
        read_task_q.put(None)

    @staticmethod
    def _read_data(
        exit_event: Event,
        in_q: Queue,
        out_q: Queue,
        in_format: str = "vertex_bytes",
        out_format: str = "dataframe",
        v_in_feats: Union[list, dict] = [],
        v_out_labels: Union[list, dict] = [],
        v_extra_feats: Union[list, dict] = [],
        v_attr_types: dict = {},
        e_in_feats: Union[list, dict] = [],
        e_out_labels: Union[list, dict] = [],
        e_extra_feats: Union[list, dict] = [],
        e_attr_types: dict = {},
        add_self_loop: bool = False,
        reindex: bool = True,
    ) -> NoReturn:
        def attr_to_tensor(
            attributes: list, attr_types: dict, df: pd.DataFrame
        ) -> torch.Tensor:
            x = []
            for col in attributes:
                dtype = attr_types[col].lower()
                if dtype.startswith("str"):
                    raise TypeError(
                        "String type not allowed for input and output features."
                    )
                if df[col].dtype == "object":
                    x.append(df[col].str.split(expand=True).to_numpy().astype(dtype))
                else:
                    x.append(df[[col]].to_numpy().astype(dtype))
            return torch.tensor(np.hstack(x).squeeze())

        v_attributes = ["vid"] + v_in_feats + v_out_labels + v_extra_feats
        e_attributes = ["source", "target"]

        while not exit_event.is_set():
            raw = in_q.get()
            if raw is None:
                in_q.task_done()
                out_q.put(None)
                break
            vertices, edges = None, None
            if in_format == "vertex_bytes":
                # Bytes of vertices in format vid,v_in_feats,v_out_labels,v_extra_feats
                data = pd.read_csv(io.BytesIO(raw), header=None, names=v_attributes)
            elif in_format == "edge_bytes":
                # Bytes of edges in format source_vid,target_vid
                data = pd.read_csv(io.BytesIO(raw), header=None, names=e_attributes)
            elif in_format == "graph_bytes":
                # A pair of in-memory CSVs (vertex, edge)
                v_file, e_file = raw
                vertices = pd.read_csv(
                    io.BytesIO(v_file), header=None, names=v_attributes
                )
                edges = pd.read_csv(io.BytesIO(e_file), header=None, names=e_attributes)
                data = (vertices, edges)
            elif in_format == "vertex_str":
                # String of vertices in format vid,v_in_feats,v_out_labels,v_extra_feats
                data = pd.read_csv(io.StringIO(raw), header=None, names=v_attributes)
            elif in_format == "edge_str":
                # String of edges in format source_vid,target_vid
                data = pd.read_csv(io.StringIO(raw), header=None, names=e_attributes)
            elif in_format == "graph_str":
                # A pair of in-memory CSVs (vertex, edge)
                v_file, e_file = raw
                vertices = pd.read_csv(
                    io.StringIO(v_file), header=None, names=v_attributes
                )
                edges = pd.read_csv(
                    io.StringIO(e_file), header=None, names=e_attributes
                )
                data = (vertices, edges)
            else:
                raise NotImplementedError

            if out_format.lower() == "pyg" or out_format.lower() == "dgl":
                if vertices is None or edges is None:
                    raise ArgumentError(
                        "PyG or DGL format can only be used with graph output."
                    )
                if out_format.lower() == "dgl":
                    try:
                        import dgl

                        mode = "dgl"
                    except ImportError:
                        raise ImportError(
                            "DGL is not installed. Please install DGL to use DGL format."
                        )
                elif out_format.lower() == "pyg":
                    try:
                        from torch_geometric.data import Data as pygData
                        from torch_geometric.utils import add_self_loops

                        mode = "pyg"
                    except ImportError:
                        raise ImportError(
                            "PyG is not installed. Please install PyG to use PyG format."
                        )
                else:
                    raise NotImplementedError
                # Reformat as a graph.
                # Need to have a pair of tables for edges and vertices.
                # Deal with edgelist first
                if reindex:
                    vertices["tmp_id"] = range(len(vertices))
                    id_map = vertices[["vid", "tmp_id"]]
                    edges = edges.merge(id_map, left_on="source", right_on="vid")
                    edges.drop(columns=["source", "vid"], inplace=True)
                    edges = edges.merge(id_map, left_on="target", right_on="vid")
                    edges.drop(columns=["target", "vid"], inplace=True)
                    edges = edges[["tmp_id_x", "tmp_id_y"]]
                if mode == "dgl":
                    edges = torch.tensor(edges.to_numpy().T, dtype=torch.long)
                    data = dgl.graph(data=(edges[0], edges[1]))
                    if add_self_loop:
                        data = dgl.add_self_loop(data)
                elif mode == "pyg":
                    data = pygData()
                    edges = torch.tensor(edges.to_numpy().T, dtype=torch.long)
                    if add_self_loop:
                        edges = add_self_loops(edges)[0]
                    data["edge_index"] = edges
                del edges
                # Deal with vertex attributes next
                if v_in_feats:
                    if mode == "dgl":
                        data.ndata["feat"] = attr_to_tensor(
                            v_in_feats, v_attr_types, vertices
                        )
                    elif mode == "pyg":
                        data["x"] = attr_to_tensor(v_in_feats, v_attr_types, vertices)
                if v_out_labels:
                    if mode == "dgl":
                        data.ndata["label"] = attr_to_tensor(
                            v_out_labels, v_attr_types, vertices
                        )
                    elif mode == "pyg":
                        data["y"] = attr_to_tensor(v_out_labels, v_attr_types, vertices)
                if v_extra_feats:
                    if mode == "dgl":
                        data.extra_data = {}
                    for col in v_extra_feats:
                        dtype = v_attr_types[col].lower()
                        if dtype.startswith("str"):
                            if mode == "dgl":
                                data.extra_data[col] = vertices[col].to_list()
                            elif mode == "pyg":
                                data[col] = vertices[col].to_list()
                        elif vertices[col].dtype == "object":
                            if mode == "dgl":
                                data.ndata[col] = torch.tensor(
                                    vertices[col]
                                    .str.split(expand=True)
                                    .to_numpy()
                                    .astype(dtype)
                                )
                            elif mode == "pyg":
                                data[col] = torch.tensor(
                                    vertices[col]
                                    .str.split(expand=True)
                                    .to_numpy()
                                    .astype(dtype)
                                )
                        else:
                            if mode == "dgl":
                                data.ndata[col] = torch.tensor(
                                    vertices[col].to_numpy().astype(dtype)
                                )
                            elif mode == "pyg":
                                data[col] = torch.tensor(
                                    vertices[col].to_numpy().astype(dtype)
                                )
                del vertices
            elif out_format.lower() == "dataframe":
                pass
            else:
                raise NotImplementedError
            out_q.put(data)
            in_q.task_done()

    def _start(self) -> None:
        # This is a template. Implement your own logics here.
        # Create task and result queues
        self._request_task_q = Queue()
        self._read_task_q = Queue()
        self._data_q = Queue(self._buffer_size)
        self._exit_event = Event()

        # Start requesting thread. Finish with your logic.
        self._requester = Thread(target=self._request_kafka, args=())
        self._requester.start()

        # Start downloading thread. Finish with your logic.
        self._downloader = Thread(target=self._download_from_kafka, args=())
        self._downloader.start()

        # Start reading thread. Finish with your logic.
        self._reader = Thread(target=self._read_data, args=())
        self._reader.start()

        raise NotImplementedError

    def __iter__(self):
        if self.num_batches == 1:
            return iter([self.data])
        self._reset()
        self._start()
        self._iterations += 1
        self._iterator = True
        return self

    def __next__(self):
        if not self._iterator:
            raise TypeError(
                "Not an iterator. Call `iter` on it first or use it in a for loop."
            )
        if not self._data_q:
            self._iterator = False
            raise StopIteration
        data = self._data_q.get()
        if data is None:
            self._iterator = False
            raise StopIteration
        return data

    @property
    def data(self):
        if self.num_batches == 1:
            if self._data is None:
                self._reset()
                self._start()
                self._data = self._data_q.get()
            return self._data
        else:
            return self

    def _reset(self) -> None:
        logging.debug("Resetting the loader")
        if self._exit_event:
            self._exit_event.set()
        if self._request_task_q:
            self._request_task_q.put(None)
        if self._download_task_q:
            self._download_task_q.put(None)
        if self._read_task_q:
            while True:
                try:
                    self._read_task_q.get(block=False)
                except Empty:
                    break
            self._read_task_q.put(None)
        if self._data_q:
            while True:
                try:
                    self._data_q.get(block=False)
                except Empty:
                    break
        if self._requester:
            self._requester.join()
        if self._downloader:
            self._downloader.join()
        if self._reader:
            self._reader.join()
        del self._request_task_q, self._download_task_q, self._read_task_q, self._data_q
        self._exit_event = None
        self._requester, self._downloader, self._reader = None, None, None
        self._request_task_q, self._download_task_q, self._read_task_q, self._data_q = (
            None,
            None,
            None,
            None,
        )
        if self.delete_kafka_topic:
            if self._kafka_topic:
                resp = self._kafka_admin.delete_topics([self._kafka_topic])
                del_res = resp.to_object()["topic_error_codes"][0]
                if del_res["error_code"] != 0:
                    raise TigerGraphException(
                        "Failed to delete topic {}".format(del_res["topic"])
                    )
                self._kafka_topic = None
        logging.debug("Successfully reset the loader")

    def fetch(self, payload: dict):
        """Fetch the specific data instances for inference/prediction.

        Args:
            payload (dict): The JSON payload to send to the API.
        """
        if self._mode == "training":
            print(
                "Loader is in training mode. Please call `inference()` function to switch to inference mode."
            )

        # Send request
        # Parse data
        # Return data
        raise NotImplementedError
