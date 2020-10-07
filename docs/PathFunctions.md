# Path Finding Algorithms

## shortestPath
`shortestPath(sourceVertices, targetVertices, maxLength=None, vertexFilters=None, edgeFilters=None, allShortestPaths=False)`

Find the shortest path (or all shortest paths) between the source and target vertex sets.

Arguments:
- `sourceVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the source vertices of the shortest paths sought.
- `targetVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the target vertices of the shortest paths sought.
- `maxLength`:        The maximum length of a shortest path. Optional, default is 6.
- `vertexFilters`:    An optional list of (vertexType, condition) tuples or {"type": &lt;str&gt;, "condition": &lt;str&gt;} dictionaries.
- `edgeFilters`:      An optional list of (edgeType, condition) tuples or {"type": &lt;str&gt;, "condition": &lt;str&gt;} dictionaries.
- `allShortestPaths`: If true, the endpoint will return all shortest paths between the source and target.  Default is false, meaning that the endpoint will return only one path.

See [more](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#input-parameters-and-output-format-for-path-finding) information on filters.

Documentation: [POST /shortestpath/{graphName}](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-shortestpath-graphname-shortest-path-search)

## allPaths
`allPaths(sourceVertices, targetVertices, maxLength, vertexFilters=None, edgeFilters=None)`

Find all possible paths up to a given maximum path length between the source and target vertex sets.

Arguments:
- `sourceVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the source vertices of the shortest paths sought.
- `targetVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the target vertices of the shortest paths sought.
- `maxLength`:        The maximum length of the paths.
- `vertexFilters`:    An optional list of (vertexType, condition) tuples or {"type": &lt;str&gt;, "condition": &lt;str&gt;} dictionaries.
- `edgeFilters`:      An optional list of (edgeType, condition) tuples or {"type": &lt;str&gt;, "condition": &lt;str&gt;} dictionaries.

See [more](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#input-parameters-and-output-format-for-path-finding) information on filters.

Documentation: [POST /allpaths/{graphName}](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-allpaths-graphname-all-paths-search)
