# Graph To DataFrame
The graphToDataFrame sub-module provides results from various built-in endpoints in a Pandas DataFrame. For this module to work, you will have to install the Pandas module, by running ```pip install pandas```. Take a look [here](https://github.com/parkererickson/pyTigerGraph/blob/master/examples/dataFrameDemos.ipynb) for some demos displaying some of the functionality.

## Getting Started
First, you will need to create a TigerGraphConnection:
```python
import pyTigerGraph as tg 

conn = tg.TigerGraphConnection(host="https://20bd42e3162a40db9ca0a2f0a4352948.i.tgcloud.io", graphname="CrunchBasePre_2013", apiToken=token)

```

## getVertexDataframe
```getVertexDataframe(vertexType, select="", where="", limit="", sort="", timeout=0)```
Returns the verticies of a given vertex type that conform to the various arguments. 

Arguments:

- `vertexType`: Type of vertex desired

- `select`: Comma separated list of vertex attributes to be retrieved or omitted.
            See [https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select)

- `where`:  Comma separated list of conditions that are all applied on each vertex' attributes.
            The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            See [https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter)

- `limit`:  Maximum number of vertex instances to be returned (after sorting).
            See [https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit)

- `sort`    Comma separated list of attributes the results should be sorted by.
            See [https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort)
Example:
```python
df = conn.getVertexDataframe("company", limit=100)
```

## getVertexDataframeByID
```getVertexDataframeByID(vertexType, vertexIds)```
Retrieves vertices of the given vertex type, identified by their ID.

Arguments:

- `vertexType`: Type of vertex desired

- `vertexIds`: A list of vertex IDs.

Example:
```python
df = conn.getVertexDataframeByID("company", ["c:1", "c:2"])
```

## upsertVertexDataframe
```upsertVertexDataframe(df, vertexType, v_id=None, attributes=None)```
Upserts vertices from a Pandas data frame. 

Arguments:
- `df`: The data frame to upsert.

- `vertexType`: The type of vertex to upsert data to.

- `v_id`: The field name where the vertex primary id is given. If omitted the dataframe index will be used instead.

- `attributes`: A dictionary in the form of {target: source} where source is the column name in the dataframe and target is the attribute name in the graph vertex. When omitted all columns would be upserted with their current names. In this case column names must match the vertex's attribute names.

```conn.upsertVertexDataframe(df=person, vertexType='person', v_id='name')```

## getEdgesDataframe
```getEdgesDataframe(sourceVertexType, sourceVerticies, edgeType=None, targetVertexType=None, targetVertexId=None, select="", where="", limit="", sort="", timeout=0)```

Retrieves edges of the given edge type originating from the list of source verticies.

Only `sourceVertexType` and `sourceVerticies` are required.
If `targetVertexId` is specified, then `targetVertexType` must also be specified.
If `targetVertexType` is specified, then `edgeType` must also be specified.

Arguments:

- `select`: Comma separated list of edge attributes to be retrieved or omitted.
            See [https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select)

- `where`:  Comma separated list of conditions that are all applied on each edge's attributes.
            The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            See [https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter)

- `limit`:  Maximum number of edge instances to be returned (after sorting).
            See [https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit)

- `sort`    Comma separated list of attributes the results should be sorted by.
            See [https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort)

Example:
```python
edgeDf = conn.getEdgesDataframe("company", ["c:1", "c:2"])
```

## upsertEdgesDataframe
```upsertEdgesDataframe(df, sourceVertexType, edgeType, targetVertexType, from_id=None, to_id=None, attributes=None)```

Upserts edges from a Pandas dataframe. 

Arguments:

- `df`: The dataframe to upsert.

- `sourceVertexType`: The type of source vertex for the edge.

- `edgeType`: The type of edge to upsert data to.

- `targetVertexType`: The type of target vertex for the edge.

- `from_id`: The field name where the source vertex primary id is given. If omitted the dataframe index would be used instead. 

- `to_id`: The field name where the target vertex primary id is given. If omitted the dataframe index would be used instead. 

- `attributes`:  A dictionary in the form of {target: source} where source is the column name in the dataframe and target is the attribute name in the graph vertex. When omitted all columns would be upserted with their current names. In this case column names must match the vertex's attribute names.

## getInstalledQueriesDataframe
```getInstalledQueriesDataframe()```
Returns dataframe of all installed queries, does not take any arguments.
Example:
```python
queries = conn.getInstalledQueriesDataframe()
```