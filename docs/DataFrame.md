# Pandas DataFrame Support

This submodule provides results from various built-in endpoints in a Pandas DataFrame. For this module to work, you will have to install the Pandas module, by running `pip install pandas`. Take a look [here](https://github.com/parkererickson/pyTigerGraph/blob/master/examples/dataFrameDemos.ipynb) for some demos displaying some of the functionality.


## getVertexDataframe
`getVertexDataframe(vertexType, select="", where="", limit="", sort="", timeout=0)`

Returns the verticies of a given vertex type that conform to the various arguments. 

Arguments:

- `vertexType`: Type of vertex desired

- `select`:  Comma separated list of vertex attributes to be retrieved or omitted. [Details](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select)
- `where`:   Comma separated list of conditions that are all applied on each vertex' attributes. The conditions are in logical conjunction (i.e. they are "AND'ed" together). [Details](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter)
- `limit`:   Maximum number of vertex instances to be returned (after sorting). [Details](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit)
- `sort`:    Comma separated list of attributes the results should be sorted by. [Details](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort)
- `timeout`: Time allowed for successful execution (0 = no limit, default).

## getVertexDataframeByID
`getVertexDataframeByID(vertexType, vertexIds)`

Retrieves vertices of the given vertex type, identified by their ID.

Arguments:
- `vertexType`: Type of vertex desired
- `vertexIds`: A list of vertex IDs.

## upsertVertexDataFrame
`upsertVertexDataframe(df, vertexType, v_id=None, attributes=None)`

Upserts vertices from a Pandas DataFrame. 

Arguments:
- `df`:         The DataFrame to upsert.
- `vertexType`: The type of vertex to upsert data to.
- `v_id`:       The field name where the vertex primary id is given. If omitted the dataframe index will be used instead.
- `attributes`: A dictionary in the form of {target: source} where source is the column name in the dataframe and target is the attribute name in the graph vertex. When omitted all columns would be upserted with their current names. In this case column names must match the vertex's attribute names.

## vertexSetToDataFrame
`vertexSetToDataFrame(vertexSet, withId=True, withType=False)`

Converts a vertex set to Pandas DataFrame.

Arguments:
- `vertexSet`: A vertex set (a list of vertices of the same vertex type).
- `withId`:    Add a column with vertex IDs to the DataFrame.
- `withType`:  Add a column with vertex type to the DataFrame.

Vertex sets are used for both the input and output of `SELECT` statements. They contain instances of vertices of the same type.
For each vertex instance the vertex ID, the vertex type and the (optional) attributes are present (under `v_id`, `v_type` and `attributes` keys, respectively).
See example in `edgeSetToDataFrame`.

A vertex set has this structure:
```
[
    {
        "v_id": <vertex_id>,
        "v_type": <vertex_type_name>,
        "attributes":
            {
                "attr1": <value1>,
                "attr2": <value2>,
                 ⋮
            }
    },
        ⋮
]
```
See: https://docs.tigergraph.com/dev/gsql-ref/querying/declaration-and-assignment-statements#vertex-set-variable-declaration-and-assignment

## getEdgesDataframe
`getEdgesDataframe(sourceVertexType, sourceVerticies, edgeType=None, targetVertexType=None, targetVertexId=None, select="", where="", limit="", sort="", timeout=0)`

Retrieves edges of the given edge type originating from the list of source verticies.

Only `sourceVertexType` and `sourceVerticies` are required.
If `targetVertexId` is specified, then `targetVertexType` must also be specified.
If `targetVertexType` is specified, then `edgeType` must also be specified.

Arguments:

- `select`: Comma separated list of edge attributes to be retrieved or omitted. [Details](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select)
- `where`:  Comma separated list of conditions that are all applied on each edge's attributes. The conditions are in logical conjunction (i.e. they are "AND'ed" together). [Details](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter)
- `limit`:  Maximum number of edge instances to be returned (after sorting). [Details](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit)
- `sort`    Comma separated list of attributes the results should be sorted by. [Details](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort)

## upsertEdgeDataFrame
`upsertEdgeDataFrame(df, sourceVertexType, edgeType, targetVertexType, from_id=None, to_id=None, attributes=None)`

Upserts edges from a Pandas dataframe. 

Arguments:
- `df`:               The dataframe to upsert.
- `sourceVertexType`: The type of source vertex for the edge.
- `edgeType`:         The type of edge to upsert data to.
- `targetVertexType`: The type of target vertex for the edge.
- `from_id`:          The field name where the source vertex primary id is given. If omitted the dataframe index would be used instead. 
- `to_id`:            The field name where the target vertex primary id is given. If omitted the dataframe index would be used instead. 
- `attributes`:       A dictionary in the form of {target: source} where source is the column name in the dataframe and target is the attribute name in the graph vertex. When omitted all columns would be upserted with their current names. In this case column names must match the vertex's attribute names.

## edgeSetToDataFrame
`edgeSetToDataFrame(self, edgeSet, withId=True, withType=False)`

Converts an edge set to Pandas DataFrame.

Arguments:
- `edgeSet`:  An edge set (a list of edges of the same edge type).
- `withId`:   Add a column with edge IDs to the DataFrame.
  - Note: As edges do not have internal ID, this column will contain a generated composite ID, a combination of source and target vertex types and IDs (specifically: `[<source vertex type>, <source vertex ID>, <target vertex type>, <target vertex ID>]`).  This is unique within the vertex type, but not guaranteed to be globally (i.e. within the whole graph) unique. To get a globally unique edge id, the edge type needs to be added to the above combination (see `withType` below).
- `withType`: Add a column with edge type to the DataFrame.
  - Note: The value of this column should be combined with the value of ID column to get a globally unique edge ID.

Edge sets contain instances of the same edge type. Edge sets are not generated "naturally" like vertex sets, you need to collect edges in (global) accumulators, e.g. in case you want to visualise them in GraphStudio or by other tools.
Example:
```
SetAccum<EDGE> @@edges;
start = {Country.*};
result =
    SELECT t
    FROM   start:s -(PROVINCE_IN_COUNTRY:e)- Province:t
    ACCUM  @@edges += e;
PRINT start, result, @@edges;
```

The `@@edges` is an edge set.  It contains for each edge instance the source and target vertex type and ID, the edge type, an directedness indicator and the (optional) attributes.

Note: `start` and `result` are vertex sets.

An edge set has this structure:
```
[
    {
        "e_type": <edge_type_name>,
        "from_type": <source_vertex_type_name>,
        "from_id": <source_vertex_id>,
        "to_type": <target_vertex_type_name>,
        "to_id": <targe_vertex_id>,
        "directed": <true_or_false>,
        "attributes":
            {
                "attr1": <value1>,
                "attr2": <value2>,
                 ⋮
            }
    },
        ⋮
]
```
## getInstalledQueriesDataframe
`getInstalledQueriesDataframe()`

Returns dataframe of all installed queries, does not take any arguments.
