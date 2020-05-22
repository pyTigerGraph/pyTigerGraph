## getSchema
`getSchema(udts=True)`

Retrieves the schema (all vertex and edge type and - if not disabled - the User Defined Type details) of the graph.

TigerGraph Documentation: [GET /gsqlserver/gsql/schema](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-the-graph-schema-get-gsql-schema)

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
