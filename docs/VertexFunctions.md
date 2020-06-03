## getVertexTypes
`getVertexTypes()`

Returns the list of vertex type names of the graph.

## getVertexType
`getVertexType(vertexType)`

Returns the details of the specified vertex type.

## getVertexCount
`getVertexCount(vertexType, where="")`

Return the number of vertices.

Arguments:
- [`where`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter): Comma separated list of conditions that are all applied on each vertex' attributes.
    The conditions are in [logical conjunction](https://en.wikipedia.org/wiki/Logical_conjunction) (i.e. they are "AND'ed" together).

Uses:
- If `vertexType` = "*": vertex count of all vertex types (`where` cannot be specified in this case)
- If `vertexType` is specified only: vertex count of the given type
- If `vertexType` and `where` are specified: vertex count of the given type after filtered by `where` condition(s)

See [documentation](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter) for valid values of `where` condition.

Returns a dictionary of `<vertex_type>: <vertex_count>` pairs.

Documentation: [GET /graph/{graph_name}/vertices](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices) and
[POST /builtins](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_vertex_number)

## upsertVertex
`upsertVertex(vertexType, vertexId, attributes=None)`

Upserts a vertex.

Data is upserted:
- If vertex is not yet present in graph, it will be created.
- If it's already in the graph, its attributes are updated with the values specified in the request. An optional [operator](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data) controls how the attributes are updated.

The `attributes` argument is expected to be a dictionary in this format:

```python
{<attribute_name>, <attribute_value>|(<attribute_name>, <operator>), …}
```

Example:

```python
{"name": "Thorin", "points": (10, "+"), "bestScore": (67, "max")}
```

Returns a single number of accepted (successfully upserted) vertices (0 or 1).

Documentation: [POST /graph](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data        )

## upsertVertices
`upsertVertices(vertexType, vertices)`

Upserts multiple vertices (of the same type).

See the description of `upsertVertex` for generic information.

The `vertices` argument is expected to be a list of tuples in this format:
```python
[
  (<vertex_id>, {<attribute_name>, <attribute_value>|(<attribute_name>, <operator>), …}),
  ⋮
]
```

Example:
```python
[
   (2, {"name": "Balin", "points": (10, "+"), "bestScore": (67, "max")}),
   (3, {"name": "Dwalin", "points": (7, "+"), "bestScore": (35, "max")}),
]
```

Returns a single number of accepted (successfully upserted) vertices (0 or positive integer).

Documentation: [POST /graph](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data        )

## getVertices
`getVertices(vertexType, select="", where="", limit="", sort="", timeout=0)`

Retrieves vertices of the given vertex type.

Arguments:
- [`select`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select): Comma separated list of vertex attributes to be retrieved or omitted.
- [`where`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter): Comma separated list of conditions that are all applied on each vertex' attributes.
    The conditions are in [logical conjunction](https://en.wikipedia.org/wiki/Logical_conjunction) (i.e. they are "AND'ed" together).
- [`limit`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit): Maximum number of vertex instances to be returned (after sorting).
- [`sort`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort): Comma separated list of attributes the results should be sorted by.

NOTE: The primary ID of a vertex instance is **NOT** an attribute, thus cannot be used in above arguments.
      Use [`getVerticesById`](#getVerticesById) if you need to retrieve by vertex ID.

Documentation: [GET /graph/{graph_name}/vertices](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices)

## getVerticesById
`getVerticesById(vertexType, vertexIds)`

Retrieves vertices of the given vertex type, identified by their ID.

Arguments
- `vertexIds`: A single vertex ID or a list of vertex IDs.

Documentation: [GET /graph/{graph_name}/vertices](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices)

## getVertexStats
`getVertexStats(vertexTypes, skipNA=False)`

Returns vertex attribute statistics.

Arguments:
- `vertexTypes`: A single vertex type name or a list of vertex types names or '*' for all vertex types.
- `skipNA`:     Skip those <u>n</u>on-<u>a</u>pplicable vertices that do not have attributes or none of their attributes have statistics gathered.

Documentation: [POST /builtins](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_vertex_attr)

## delVertices
`delVertices(vertexType, where="", limit="", sort="", permanent=False, timeout=0)`

Deletes vertices from graph.

Arguments:
- [`where`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter): Comma separated list of conditions that are all applied on each vertex' attributes.
    The conditions are in [logical conjunction](https://en.wikipedia.org/wiki/Logical_conjunction) (i.e. they are "AND'ed" together).
- [`limit`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit): Maximum number of vertex instances to be returned (after sorting). _Must_ be used with `sort`.
- [`sort`](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort): Comma separated list of attributes the results should be sorted by. _Must_ be user with `limit`.
- `permanent`: If true, the deleted vertex IDs can never be inserted back, unless the graph is dropped or the graph store is cleared.
- `timeout`: Time allowed for successful execution (0 = no limit, default).

NOTE: The primary ID of a vertex instance is NOT an attribute, thus cannot be used in above arguments.
      Use [`delVerticesById`](#delVerticesById) if you need to delete by vertex ID.

Returns a single number of vertices deleted.

Documentation: [DELETE /graph/{graph_name}/vertices](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-graph-graph_name-vertices)

## delVerticesById
`delVerticesById(vertexType, vertexIds, permanent=False, timeout=0)`

Deletes vertices from graph identified by their ID.

Arguments:
- `vertexIds`: A single vertex ID or a list of vertex IDs.
- `permanent`: If true, the deleted vertex IDs can never be inserted back, unless the graph is dropped or the graph store is cleared.
- `timeout`: Time allowed for successful execution (0 = no limit, default).

Returns a single number of vertices deleted.

Documentation: [DELETE /graph/{graph_name}/vertices](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-graph-graph_name-vertices)