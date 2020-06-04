# Graph To DataFrame
The graphToDataFrame sub-module provides results from various built-in endpoints in a Pandas DataFrame. For this module to work, you will have to install the Pandas module, by running ```pip install pandas```. Take a look [here](https://github.com/parkererickson/pyTigerGraph/blob/master/examples/dataFrameDemos.ipynb) for some demos displaying some of the functionality.

## Getting Started
First, you will need to import the submodule and pass in the connection object that is created with the core functionality. This will look something like this:
```python
import pyTigerGraph as tg 
from pyTigerGraph import graphToDataFrame as tgDf

conn = tg.TigerGraphConnection(host="https://20bd42e3162a40db9ca0a2f0a4352948.i.tgcloud.io", graphname="CrunchBasePre_2013", apiToken=token)

dfConn = tgDf.graphToDataFrame(conn)
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
df = dfConn.getVertexDataframe("company", limit=100)
```

## getVertexDataframeByID
```getVertexDataframeByID(vertexType, vertexIds)```
Retrieves vertices of the given vertex type, identified by their ID.

Arguments:

- `vertexType`: Type of vertex desired

- `vertexIds`: A list of vertex IDs.

Example:
```python
df = dfConn.getVertexDataframeByID("company", ["c:1", "c:2"])
```

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
edgeDf = dfConn.getEdgesDataframe("company", ["c:1", "c:2"])
```

## getInstalledQueriesDataframe
```getInstalledQueriesDataframe()```
Returns dataframe of all installed queries, does not take any arguments.
Example:
```python
queries = df.getInstalledQueriesDataframe()
```