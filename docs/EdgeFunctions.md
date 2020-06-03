## getEdgeTypes
`getEdgeTypes()`

Returns the list of edge type names of the graph.

## getEdgeType
`getEdgeType(typeName)`

Returns the details of vertex type.

## getEdgeCount
`getEdgeCount(sourceVertexType=None, sourceVertexId=None, edgeType=None, targetVertexType=None, targetVertexId=None, where="")`

Returns the number of edges.

Arguments:
- [`where`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter): Comma separated list of conditions that are all applied on each edge's attributes.
    The conditions are in [logical conjunction](https://en.wikipedia.org/wiki/Logical_conjunction) (i.e. they are "AND'ed" together).

Uses:
- If `edgeType` = "*": edge count of all edge types (no other arguments can be specified in this case).
- If `edgeType` is specified only: edge count of the given edge type.
- If `sourceVertexType`, `edgeType`, `targetVertexType` are specified: edge count of the given edge type between source and target vertex types.
- If `sourceVertexType`, `sourceVertexId` are specified: edge count of all edge types from the given vertex instance.
- If `sourceVertexType`, `sourceVertexId`, `edgeType` are specified: edge count of all edge types from the given vertex instance.
- If `sourceVertexType`, `sourceVertexId`, `edgeType`, `where` are specified: the edge count of the given edge type after filtered by `where` condition.

If `targetVertexId` is specified, then `targetVertexType` must also be specified.
If `targetVertexType` is specified, then `edgeType` must also be specified.

Returns a dictionary of `<edge_type>: <edge_count>` pairs.

Documentation: [GET /graph/{graph_name}/edges](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-edges) and
               [POST /builtins](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_edge_number)

## upsertEdge
`upsertEdge(sourceVertexType, sourceVertexId, edgeType, targetVertexType, targetVertexId, attributes={})`

Upserts an edge.

Data is upserted:
- If edge is not yet present in graph, it will be created (see special case below).
- If it's already in the graph, it is updated with the values specified in the request. An optional [operator](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data) controls how the attributes are updated.

The `attributes` argument is expected to be a dictionary in this format:
```python
{<attribute_name>, <attribute_value>|(<attribute_name>, <operator>), …}
```

Example:
```python
{"visits": (1482, "+"), "max_duration": (371, "max")}
```

Returns a single number of accepted (successfully upserted) edges (0 or 1).

Note: If operator is "vertex_must_exist" then edge will only be created if both vertex exists in graph.
      Otherwise missing vertices are created with the new edge.

Documentation: [POST /graph](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data        )

## upsertEdges
`upsertEdges(sourceVertexType, edgeType, targetVertexType, edges)`

Upserts multiple edges (of the same type).

See the description of `upsertEdge` for generic information.

The `edges` argument is expected to be a list in of tuples in this format:
```python
[
  (<source_vertex_id>, <target_vertex_id>, {<attribute_name>: <attribute_value>|(<attribute_name>, <operator>), …})
  ⋮
]
```

Example:
```python
[
  (17, "home_page", {"visits": (35, "+"), "max_duration": (93, "max")}),
  (42, "search", {"visits": (17, "+"), "max_duration": (41, "max")}),
]
```

Returns a single number of accepted (successfully upserted) edges (0 or positive integer).

Documentation: [POST /graph](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data        )

## getEdges
`getEdges(sourceVertexType, sourceVertexId, edgeType=None, targetVertexType=None, targetVertexId=None, select="", where="", limit="", sort="", timeout=0)`

Retrieves edges of the given edge type.

Only `sourceVertexType` and `sourceVertexId` are required.
If `targetVertexId` is specified, then `targetVertexType` must also be specified.
If `targetVertexType` is specified, then `edgeType` must also be specified.

Arguments:
- [`select`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select): Comma separated list of edge attributes to be retrieved or omitted.
- [`where`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter): Comma separated list of conditions that are all applied on each edge's attributes.
    The conditions are in [logical conjunction](https://en.wikipedia.org/wiki/Logical_conjunction) (i.e. they are "AND'ed" together).
- [`limit`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit): Maximum number of edge instances to be returned (after sorting).
- [`sort`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort): Comma separated list of attributes the results should be sorted by.

Documentation: [GET /graph/{graph_name}/vertices](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices)

## getEdgeStats
`getEdgeStats(edgeTypes, skipNA=False)`

Returns edge attribute statistics.

Arguments:
- `edgeTypes`: A single edge type name or a list of edges types names or '*' for all edges types.
- `skipNA`:    Skip those <u>n</u>on-<u>a</u>pplicable edges that do not have attributes or none of their attributes have statistics gathered.

Documentation: [POST /builtins](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_edge_attr)

## delEdges
`delEdges(sourceVertexType, sourceVertexId, edgeType=None, targetVertexType=None, targetVertexId=None, where="", limit="", sort="", timeout=0)`

Deletes edges from the graph.

Only `sourceVertexType` and `sourceVertexId` are required.
If `targetVertexId` is specified, then `targetVertexType` must also be specified.
If `targetVertexType` is specified, then `edgeType` must also be specified.

Arguments:
- [`where`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter): Comma separated list of conditions that are all applied on each edge's attributes.
    The conditions are in [logical conjunction](https://en.wikipedia.org/wiki/Logical_conjunction) (i.e. they are "AND'ed" together).
- [`limit`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit): Maximum number of edge instances to be returned (after sorting).
- [`sort`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort): Comma separated list of attributes the results should be sorted by.
- `timeout`: Time allowed for successful execution (0 = no limit, default).

Returns a dictionary of `<edge_type>: <deleted_edge_count>` pairs.

Documentation: [DELETE /graph/{/graph_name}/edges](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-graph-graph_name-edges)