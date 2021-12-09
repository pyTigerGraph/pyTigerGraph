## getSchema
`getSchema(full=True)`

Retrieves the schema metadata of the graph.

Arguments:
- `full`: If `False`, returns metadata of vertices and edges only. If `True`, it will additionaly return additional info on vertices and egdes, plus info on UDTs, indices, loading jobs, queries, data sources, users and their roles, and proxy groups. The database user's privileges control how much data is returned for each object types.

This functions uses the [GSQL Submodule](Gsql.md) is `full` is `True`.

Documentation: [GET /gsqlserver/gsql/schema](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-the-graph-schema-get-gsql-schema)

## getUDTs
`getUDTs()`

Returns the list of User Defined Types (names only).

## getUDT
`getUDT(udtName)`

Returns the details of a specific User Defined Type.

## upsertData
`upsertData(data)`

Upserts data (vertices and edges) from a JSON document or equivalent object structure.

TigerGraph Documentation: [POST /graph](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data)
