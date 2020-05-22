### runInstalledQuery
`runInstalledQuery(queryName, params=None, timeout=16000, sizeLimit=32000000)`

Runs an installed query.

The query must be already created and installed in the graph.
Use [`getEndpoints(dynamic=True)`](#getEndpoints) or GraphStudio to find out the generated endpoint URL of the query, but only the query name needs to be specified here.

Arguments:
- `params`:    A string of `param1=value1&param2=value2` format or a dictionary.
- `timeout`:   Maximum duration for successful query execution.
- `sizeLimit`: Maximum size of response (in bytes).

Documentation: [POST /query/{graph_name}/<query_name>](https://docs.tigergraph.com/dev/gsql-ref/querying/query-operations#running-a-query)

### runInterpretedQuery
`runInterpretedQuery(queryText, params=None)`

Runs an interpreted query.

You must provide the query text in this format:
```
INTERPRET QUERY (<params>) FOR GRAPH <graph_name> {
   <statements>
}'
```

Arguments:
- `params`:    A string of `param1=value1&param2=value2` format or a dictionary.

Documentation: [POST /gsqlserver/interpreted_query](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-gsqlserver-interpreted_query-run-an-interpreted-query)