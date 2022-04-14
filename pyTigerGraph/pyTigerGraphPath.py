"""Path Finding Functions."""

import json

from pyTigerGraph.pyTigerGraphBase import pyTigerGraphBase


class pyTigerGraphPath(pyTigerGraphBase):
    """Path Finding Functions."""

    def _preparePathParams(self, sourceVertices: [dict, tuple, list],
            targetVertices: [dict, tuple, list], maxLength: int = None,
            vertexFilters: [list, dict] = None, edgeFilters: [list, dict] = None,
            allShortestPaths: bool = False) -> str:
        """Prepares the input parameters by transforming them to the format expected by the path algorithms.

        See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_input_parameters_and_output_format_for_path_finding

        A vertex set is a dict that has three top-level keys: v_type, v_id, attributes (a dict).

        Args:
            sourceVertices:
                A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples;
                the source vertices of the shortest paths sought.
            targetVertices:
                A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples;
                the target vertices of the shortest paths sought.
            maxLength:
                The maximum length of a shortest path. Optional, default is 6.
            vertexFilters:
                An optional list of (vertexType, condition) tuples or
                {"type": <str>, "condition": <str>} dictionaries.
            edgeFilters:
                An optional list of (edgeType, condition) tuples or
                {"type": <str>, "condition": <str>} dictionaries.
            allShortestPaths:
                If True, the endpoint will return all shortest paths between the source and target.
                Default is False, meaning that the endpoint will return only one path.

        Returns:
            A string representation of the dictionary of end-point parameters.
        """

        def parseVertices(vertices: [dict, tuple, list]) -> list:
            """Parses vertex input parameters and converts it to the format required by the path
            finding endpoints.

            Args:
                vertices:
                    A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples;
                    the source or target vertices of the shortest paths sought.
            Returns:
                A list of vertices in the the format required by the path finding endpoints.
            """
            ret = []
            if not isinstance(vertices, list):
                vertices = [vertices]
            for v in vertices:
                if isinstance(v, tuple):
                    tmp = {"type": v[0], "id": v[1]}
                    ret.append(tmp)
                elif isinstance(v, dict) and "v_type" in v and "v_id" in v:
                    tmp = {"type": v["v_type"], "id": v["v_id"]}
                    ret.append(tmp)
                elif self.debug:
                    print("Invalid vertex type or value: " + str(v))
                    # TODO Proper logging
            return ret

        def parseFilters(filters: list) -> list:
            """Parses filter input parameters and converts it to the format required by the path
            finding endpoints.

            Args:
                filters:
                    A list of (vertexType, condition) tuples or {"type": <str>, "condition": <str>}
                    dictionaries.

            Returns:
                A list of filters in the format required by the path finding endpoints.
            """
            ret = []
            if not isinstance(filters, list):
                filters = [filters]
            for f in filters:
                if isinstance(f, tuple):
                    tmp = {"type": f[0], "condition": f[1]}
                    ret.append(tmp)
                elif isinstance(f, dict) and "type" in f and "condition" in f:
                    tmp = {"type": f["type"], "condition": f["condition"]}
                    ret.append(tmp)
                elif self.debug:
                    print("Invalid filter type or value: " + str(f))
                    # TODO Proper logging
            return ret

        # Assembling the input payload
        if not sourceVertices or not targetVertices:
            return None
            # TODO Should allow returning error instead of handling missing parameters here?
        data = {}
        data["sources"] = parseVertices(sourceVertices)
        data["targets"] = parseVertices(targetVertices)
        if vertexFilters:
            data["vertexFilters"] = parseFilters(vertexFilters)
        if edgeFilters:
            data["edgeFilters"] = parseFilters(edgeFilters)
        if maxLength:
            data["maxLength"] = maxLength
        if allShortestPaths:
            data["allShortestPaths"] = True

        return json.dumps(data)

    def shortestPath(self, sourceVertices: [dict, tuple, list], targetVertices: [dict, tuple, list],
            maxLength: int = None, vertexFilters: [list, dict] = None,
            edgeFilters: [list, dict] = None, allShortestPaths: bool = False) -> dict:
        """Find the shortest path (or all shortest paths) between the source and target vertex sets.

        A vertex set is a dict that has three top-level keys: `v_type`, `v_id`, `attributes` (a dict).

        Args:
            sourceVertices:
                A vertex set (a list of vertices) or a list of `(vertexType, vertexID)` tuples;
                the source vertices of the shortest paths sought.
            targetVertices:
                A vertex set (a list of vertices) or a list of `(vertexType, vertexID)` tuples;
                the target vertices of the shortest paths sought.
            maxLength:
                The maximum length of a shortest path. Optional, default is 6.
            vertexFilters:
                An optional list of (vertexType, condition) tuples or
                `{"type": <str>, "condition": <str>}` dictionaries.
            edgeFilters:
                An optional list of (edgeType, condition) tuples or
                `{"type": <str>, "condition": <str>}` dictionaries.
            allShortestPaths:
                If `True`, the endpoint will return all shortest paths between the source and target.
                Default is `False`, meaning that the endpoint will return only one path.

        Returns:
            The shortest path between the source and the target.
            The returned value is a subgraph: all vertices and edges that are part of the path(s);
            i.e. not a (list of individual) path(s).

        Examples:
            ```
            path = conn.shortestPath(("account", 10), ("person", 50), maxLength=3)

            path = conn.shortestPath(("account", 10), ("person", 50), allShortestPaths=True,
                vertexFilters=("transfer", "amount>950"), edgeFilters=("receive", "type=4"))
            ```

        Endpoint:
            - `POST /shortestpath/{graphName}`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_find_shortest_path
        """
        data = self._preparePathParams(sourceVertices, targetVertices, maxLength, vertexFilters,
            edgeFilters, allShortestPaths)
        return self._post(self.restppUrl + "/shortestpath/" + self.graphname, data=data)

    def allPaths(self, sourceVertices: [dict, tuple, list], targetVertices: [dict, tuple, list],
            maxLength: int, vertexFilters: [list, dict] = None,
            edgeFilters: [list, dict] = None) -> dict:
        """Find all possible paths up to a given maximum path length between the source and target
        vertex sets.

        A vertex set is a dict that has three top-level keys: v_type, v_id, attributes (a dict).

        Args:
            sourceVertices:
                A vertex set (a list of vertices) or a list of `(vertexType, vertexID)` tuples;
                the source vertices of the shortest paths sought.
            targetVertices:
                A vertex set (a list of vertices) or a list of `(vertexType, vertexID)` tuples;
                the target vertices of the shortest paths sought.
            maxLength:
                The maximum length of the paths.
            vertexFilters:
                An optional list of (vertexType, condition) tuples or
                `{"type": <str>, "condition": <str>}` dictionaries.
            edgeFilters:
                An optional list of (edgeType, condition) tuples or
                `{"type": <str>, "condition": <str>}` dictionaries.

        Returns:
            All paths between a source vertex (or vertex set) and target vertex (or vertex set).
            The returned value is a subgraph: all vertices and edges that are part of the path(s);
            i.e. not a (list of individual) path(s).

        Example:
            ```
            path = conn.allPaths(("account", 10), ("person", 50), allShortestPaths=True,
                vertexFilters=("transfer", "amount>950"), edgeFilters=("receive", "type=4"))
            ```


        Endpoint:
            - `POST /allpaths/{graphName}`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_find_all_paths
        """
        data = self._preparePathParams(sourceVertices, targetVertices, maxLength, vertexFilters,
            edgeFilters)
        return self._post(self.restppUrl + "/allpaths/" + self.graphname, data=data)
