# The Functions

Common arguments used in methods:
- `vertexType`, `sourceVertexType`, `targetVertexType`: The name of a vertex type in the graph. Use [`getVertexTypes`](#getVertexTypes) to fetch the list of vertex types currently in the graph.
- `vertexId`, `sourceVertexId`, `targetVertexId`: The primary ID of a vertex instance (of the appropriate data type).
- `edgeType`: The name of the edge type in the graph. Use [`getEdgeTypes`](#getEdgeTypes) to fetch the list of edge types currently in the graph.


|**Schema related functions**                 |**Query related functions**                                 |**Vertex related functions**                         |**Edge related functions**                   |**Token management**                           |**Other functions**
|-------------------------------------------|------------------------------------------------------------|-----------------------------------------------------|---------------------------------------------|-----------------------------------------------|--------------------
|[getSchema](SchemaFunctions.md#getschema)  |[runInstalledQuery](QueryFunctions.md#runInstalledQuery)    |[getVertexTypes](VertexFunctions.md#getVertexTypes)  |[getEdgeTypes](EdgeFunctions.md#getEdgeTypes)|[getToken](TokenManagement.md#getToken)        |[echo](OtherFunctions#echo)
|[getUDTs](SchemaFunctions.md#getUDTs)      |[runInterpretedQuery](QueryFunctions.md#runInterpretedQuery)|[getVertexType](VertexFunctions.md#getVertexType)    |[getEdgeType](EdgeFunctions.md#getEdgeType)  |[refreshToken](TokenManagement.md#refreshToken)|[getEndpoints](OtherFunctions#getEndpoints)
|[getUDT](SchemaFunctions.md#getUDT)        |                                                            |[getVertexCount](VertexFunctions.md#getVertexCount)  |[getEdgeCount](EdgeFunctions.md#getEdgeCount)|[deleteToken](TokenManagement.md#deleteToken)  |[getStatistics](OtherFunctions#getStatistics)
|[upsertData](SchemaFunctions.md#upsertData)|                                                            |[upsertVertex](VertexFunctions.md#upsertVertex)      |[upsertEdge](EdgeFunctions.md#upsertEdge)    |                                               |[getVersion](OtherFunctions#getVersion)
|                                           |                                                            |[upsertVertices](VertexFunctions.md#upsertVertices)  |[upsertEdges](EdgeFunctions.md#upsertEdges)  |                                               |[getVer](OtherFunctions#getVer)
|                                           |                                                            |[getVertices](VertexFunctions.md#getVertices)        |[getEdges](EdgeFunctions.md#getEdges)        |                                               |[getLicenseInfo](OtherFunctions#getLicenseInfo)
|                                           |                                                            |[getVerticesById](VertexFunctions.md#getVerticesById)|[getEdgeStats](EdgeFunctions.md#getEdgeStats)|                                               |
|                                           |                                                            |[getVertexStats](VertexFunctions.md#getVertexStats)  |[delEdges](EdgeFunctions.md#delEdges)        |                                               |
|                                           |                                                            |[delVertices](VertexFunctions.md#delVertices)        |                                             |                                               |
|                                           |                                                            |[delVerticesById](VertexFunctions.md#delVerticesById)|                                             |                                               |
