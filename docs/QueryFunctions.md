# Query Functions

## getInstalledQueries
`getInstalledQueries(fmt="py")`

Returns a list of installed queries.

Arguments:
- `fmt`: Format of the results:
  - "py":   Python objects (default)
  - "json": JSON document
  - "df":   Pandas DataFrame

## runInstalledQuery
`runInstalledQuery(queryName, params=None, timeout=None, sizeLimit=None)`

Runs an installed query.

The query must be already created and installed in the graph.
Use [`getEndpoints(dynamic=True)`](#getEndpoints) or GraphStudio to find out the generated endpoint URL of the query, but only the query name needs to be specified here.

Arguments:
- `params`:    A string of `param1=value1&param2=value2` format or a dictionary.
- `timeout`:   [Maximum duration](https://docs.tigergraph.com/dev/restpp-api/restpp-requests#gsql-query-timeout) for successful query execution (in ms). Default: 16s.
- `sizeLimit`: [Maximum size](https://docs.tigergraph.com/dev/restpp-api/restpp-requests#response-size) of response (in bytes).

Documentation: [POST /query/{graph_name}/<query_name>](https://docs.tigergraph.com/dev/gsql-ref/querying/query-operations#running-a-query)

Example: `conn.runInstalledQuery("getUserInfo", {"userID": "user121"})`

## runInterpretedQuery
`runInterpretedQuery(queryText, params=None, timeout=None, sizeLimit=None)`

Runs an interpreted query.

You must provide the query text in this format:
```
INTERPRET QUERY (<params>) FOR GRAPH <graph_name> {
   <statements>
}'
```

Arguments:
- `params`:    A string of `param1=value1&param2=value2` format or a dictionary.
- `timeout`:   [Maximum duration](https://docs.tigergraph.com/dev/restpp-api/restpp-requests#gsql-query-timeout) for successful query execution (in ms).  Default: 16s.
- `sizeLimit`: [Maximum size](https://docs.tigergraph.com/dev/restpp-api/restpp-requests#response-size) of response (in bytes).

Documentation: [POST /gsqlserver/interpreted_query](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-gsqlserver-interpreted_query-run-an-interpreted-query)

## parseQueryOutput
`parseQueryOutput(output, graphOnly=True)`

Parses query output and separates vertex and edge data (and optionally other output) for easier use.

- The JSON output from a query can contain a mixture of results: vertex sets (the output of a `SELECT` statement), edge sets (e.g. collected in a global accumulator), printout of global and local variables and accumulators, including complex types (LIST, MAP, etc.). The type of the various output entries is not explicit, you need to inspect the content to find out what it is actually.
- This function "cleans" this output, separating and collecting vertices and edges in an easy to access way. It can also collect other output or ignore it.
- The output of this function can be used e.g. with the `vertexSetToDataFrame()` and `edgeSetToDataFrame()` functions or (after some transformation) to pass a subgraph to a visualisation component.

Arguments:
- `output`:    The data structure returned by `runInstalledQuery()` or `runInterpretedQuery()`
- `graphOnly`: Should output be restricted to vertices and edges (True, default) or should any other output (e.g. values of variables or accumulators, or plain text printed) be captured as well.

Returns: A dictionary with two (or three) keys: "vertices", "edges" and optionally "output". First two refer to another dictionary containing keys for each vertex and edge types found, and the instances of those vertex and edge types. "output" is a list of dictionaries containing the key/value pairs of any other output.
