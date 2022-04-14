"""Vertex Functions."""

import json

import pandas as pd

from pyTigerGraph.pyTigerGraphException import TigerGraphException
from pyTigerGraph.pyTigerGraphSchema import pyTigerGraphSchema
from pyTigerGraph.pyTigerGraphUtils import pyTigerGraphUtils


class pyTigerGraphVertex(pyTigerGraphUtils, pyTigerGraphSchema):
    """Vertex Functions."""

    def getVertexTypes(self, force: bool = False) -> list:
        """Returns the list of vertex type names of the graph.

        Args:
            force:
                If `True`, forces the retrieval the schema metadata again, otherwise returns a
                cached copy of vertex type metadata (if they were already fetched previously).

        Returns:
            The list of vertex types defined in the the current graph.
        """
        ret = []
        for vt in self.getSchema(force=force)["VertexTypes"]:
            ret.append(vt["Name"])
        return ret

    def getVertexType(self, vertexType: str, force: bool = False) -> dict:
        """Returns the details of the specified vertex type.

        Args:
            vertexType:
                The name of of the vertex type.
            force:
                If `True`, forces the retrieval the schema metadata again, otherwise returns a
                cached copy of vertex type details (if they were already fetched previously).

        Returns:
            The metadata of the vertex type.
        """
        for vt in self.getSchema(force=force)["VertexTypes"]:
            if vt["Name"] == vertexType:
                return vt
        return {}  # Vertex type was not found
        # TODO Should raise exception instead?

    def getVertexCount(self, vertexType: [str, list], where: str = "") -> [int, dict]:
        """Returns the number of vertices of the specified type.

        Args:
            vertexType:
                The name of the vertex type.
            where:
                A comma separated list of conditions that are all applied on each vertex's
                attributes. The conditions are in logical conjunction (i.e. they are "AND'ed"
                together).

        Returns:
            A dictionary of <vertex_type>: <vertex_count> pairs.

        Uses:
            - If `vertexType` == "*": the count of the instances of all vertex types (`where` cannot
                be specified in this case).
            - If `vertexType` is specified only: count of the instances of the given vertex type.
            - If `vertexType` and `where` are specified: count of the instances of the given vertex
                type after being filtered by `where` condition(s).

        Raises:
            `TigerGraphException` when "*" is specified as vertex type and a `where` condition is
            provided; or when invalid vertex type name is specified.

        Endpoints:
            - `GET /graph/{graph_name}/vertices`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_list_vertices
            - `POST /builtins`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_run_built_in_functions_on_graph
        """
        # If WHERE condition is not specified, use /builtins else use /vertices
        if isinstance(vertexType, str) and vertexType != "*":
            res = self._get(self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType
                + "?count_only=true" + ("&filter=" + where if where else ""))[0]
            return res["count"]
        if where:
            if vertexType == "*":
                raise TigerGraphException(
                    "VertexType cannot be \"*\" if where condition is specified.", None)
            else:
                raise TigerGraphException(
                    "VertexType cannot be a list if where condition is specified.", None)
        if vertexType == "*":
            # TODO Investigate: /builtins/stat_vertex_number: why it is not up-to-date after insert?
            # data = '{"function":"stat_vertex_number","type":"' + vertexType + '"}'
            # res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data)
            vertexType = self.getVertexTypes()
        ret = {}
        for vt in vertexType:
            res = self._get(self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vt
                + "?count_only=true")[0]
            ret[res["v_type"]] = res["count"]
        return ret

    def upsertVertex(self, vertexType: str, vertexId: str, attributes: dict = None) -> int:
        """Upserts a vertex.

        Data is upserted:

        - If vertex is not yet present in graph, it will be created.
        - If it's already in the graph, its attributes are updated with the values specified in
            the request. An optional operator controls how the attributes are updated.

        Args:
            vertexType:
                The name of the vertex type.
            vertexId:
                The primary ID of the vertex to be upserted.
            attributes:
                The attributes of the vertex to be upserted; a dictionary in this format:
                ```
                    {<attribute_name>: <attribute_value>|(<attribute_name>, <operator>), …}
                ```
                Example:
                ```
                    {"name": "Thorin", points: (10, "+"), "bestScore": (67, "max")}
                ```
                For valid values of `<operator>` see https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#operation-codes .

        Returns:
             A single number of accepted (successfully upserted) vertices (0 or 1).

        Endpoint:
            - `POST /graph/{graph_name}`
                See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#upsert-data-to-graph
        """
        if not isinstance(attributes, dict):
            return None
            # TODO Should return 0 or raise exception instead?
        vals = self._upsertAttrs(attributes)
        data = json.dumps({"vertices": {vertexType: {vertexId: vals}}})
        return self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0][
            "accepted_vertices"]

    def upsertVertices(self, vertexType: str, vertices: list) -> int:
        """Upserts multiple vertices (of the same type).

        See the description of ``upsertVertex`` for generic information.

        Args:
            vertexType:
                The name of the vertex type.
            vertices:
                A list of tuples in this format:
                ```
                [
                    (<vertex_id>, {<attribute_name>: <attribute_value>, …}),
                    (<vertex_id>, {<attribute_name>: (<attribute_value>, <operator>), …}),
                    ⋮
                ]
                ```
                Example:
                ```
                [
                    (2, {"name": "Balin", "points": (10, "+"), "bestScore": (67, "max")}),
                    (3, {"name": "Dwalin", "points": (7, "+"), "bestScore": (35, "max")})
                ]
                ```
                For valid values of `<operator>` see https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#operation-codes .

        Returns:
            A single number of accepted (successfully upserted) vertices (0 or positive integer).

        Endpoint:
            - `POST /graph/{graph_name}`
                See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#upsert-data-to-graph
        """
        if not isinstance(vertices, list):
            return None
            # TODO Should return 0 or raise exception instead?
        data = {}
        for v in vertices:
            vals = self._upsertAttrs(v[1])
            data[v[0]] = vals
        data = json.dumps({"vertices": {vertexType: data}})
        return self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0][
            "accepted_vertices"]

    def upsertVertexDataFrame(self, df: pd.DataFrame, vertexType: str, v_id: bool = None,
            attributes: dict = "") -> int:
        """Upserts vertices from a Pandas DataFrame.

        Args:
            df:
                The DataFrame to upsert.
            vertexType:
                The type of vertex to upsert data to.
            v_id:
                The field name where the vertex primary id is given. If omitted the dataframe index
                would be used instead.
            attributes:
                A dictionary in the form of `{target: source}` where source is the column name in
                the dataframe and target is the attribute name in the graph vertex. When omitted,
                all columns would be upserted with their current names. In this case column names
                must match the vertex's attribute names.

        Returns:
            The number of vertices upserted.
        """

        json_up = []

        for index in df.index:
            json_up.append(json.loads(df.loc[index].to_json()))
            json_up[-1] = (
                index if v_id is None else json_up[-1][v_id],
                json_up[-1] if attributes is None
                else {target: json_up[-1][source] for target, source in attributes.items()}
            )

        return self.upsertVertices(vertexType=vertexType, vertices=json_up)

    def getVertices(self, vertexType: str, select: str = "", where: str = "",
            limit: [int, str] = None, sort: str = "", fmt: str = "py", withId: bool = True,
            withType: bool = False, timeout: int = 0) -> [dict, str, pd.DataFrame]:
        """Retrieves vertices of the given vertex type.

        *Note*:
            The primary ID of a vertex instance is NOT an attribute, thus cannot be used in
            `select`, `where` or `sort` parameters (unless the `WITH primary_id_as_attribute` clause
            was used when the vertex type was created). /
            Use `getVerticesById()` if you need to retrieve vertices by their primary ID.

        Args:
            vertexType:
                The name of the vertex type.
            select:
                Comma separated list of vertex attributes to be retrieved.
            where:
                Comma separated list of conditions that are all applied on each vertex' attributes.
                The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            sort:
                Comma separated list of attributes the results should be sorted by.
                Must be used with `limit`.
            limit:
                Maximum number of vertex instances to be returned (after sorting).
                Must be used with `sort`.
            fmt:
                Format of the results:
                - "py":   Python objects
                - "json": JSON document
                - "df":   pandas DataFrame
            withId:
                (When the output format is "df") should the vertex ID be included in the dataframe?
            withType:
                (When the output format is "df") should the vertex type be included in the dataframe?
            timeout:
                Time allowed for successful execution (0 = no limit, default).

        Returns:
            The (selected) details of the (matching) vertex instances (sorted, limited) as
            dictionary, JSON or pandas DataFrame.

        Endpoint:
            - `GET /graph/{graph_name}/vertices/{vertex_type}`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_list_vertices
        """
        url = self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType
        isFirst = True
        if select:
            url += "?select=" + select
            isFirst = False
        if where:
            url += ("?" if isFirst else "&") + "filter=" + where
            isFirst = False
        if limit:
            url += ("?" if isFirst else "&") + "limit=" + str(limit)
            isFirst = False
        if sort:
            url += ("?" if isFirst else "&") + "sort=" + sort
            isFirst = False
        if timeout and timeout > 0:
            url += ("?" if isFirst else "&") + "timeout=" + str(timeout)

        ret = self._get(url)

        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return self.vertexSetToDataFrame(ret, withId, withType)
        return ret

    def getVertexDataFrame(self, vertexType: str, select: str = "", where: str = "",
            limit: str = "", sort: str = "", timeout: int = 0) -> pd.DataFrame:
        """Retrieves vertices of the given vertex type and returns them as pandas DataFrame.

        This is a shortcut to `getVertices(..., fmt="df", withId=True, withType=False)`.

        *Note*:
            The primary ID of a vertex instance is NOT an attribute, thus cannot be used in
            `select`, `where` or `sort` parameters (unless the `WITH primary_id_as_attribute` clause
            was used when the vertex type was created). /
            Use `getVerticesById()` if you need to retrieve vertices by their primary ID.

        Args:
            vertexType:
                The name of the vertex type.
            select:
                Comma separated list of vertex attributes to be retrieved.
            where:
                Comma separated list of conditions that are all applied on each vertex' attributes.
                The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            sort:
                Comma separated list of attributes the results should be sorted by.
                Must be used with 'limit'.
            limit:
                Maximum number of vertex instances to be returned (after sorting).
                Must be used with `sort`.
            timeout:
                Time allowed for successful execution (0 = no limit, default).

        Returns:
            The (selected) details of the (matching) vertex instances (sorted, limited) as pandas
            DataFrame.
        """
        return self.getVertices(vertexType, select=select, where=where, limit=limit, sort=sort,
            fmt="df", withId=True, withType=False, timeout=timeout)

    def getVertexDataframe(self, vertexType: str, select: str = "", where: str = "",
            limit: str = "", sort: str = "", timeout: int = 0) -> pd.DataFrame:
        """DEPRECATED

        Use `getVertexDataFrame()` instead.

        TODO Proper deprecation
        """
        return self.getVertexDataFrame(vertexType, select=select, where=where, limit=limit,
            sort=sort, timeout=timeout)

    def getVerticesById(self, vertexType: str, vertexIds: [int, str, list], select: str = "",
            fmt: str = "py", withId: bool = True, withType: bool = False,
            timeout: int = 0) -> [dict, str, pd.DataFrame]:
        """Retrieves vertices of the given vertex type, identified by their ID.

        Args:
            vertexType:
                The name of the vertex type.
            vertexIds:
                A single vertex ID or a list of vertex IDs.
            select:
                Comma separated list of vertex attributes to be retrieved.
            fmt:
                Format of the results:
                    "py":   Python objects
                    "json": JSON document
                    "df":   pandas DataFrame
            withId:
                (If the output format is "df") should the vertex ID be included in the dataframe?
            withType:
                (If the output format is "df") should the vertex type be included in the dataframe?
            timeout:
                Time allowed for successful execution (0 = no limit, default).

        Returns:
            The (selected) details of the (matching) vertex instances as dictionary, JSON or pandas
            DataFrame.

        Endpoint:
            - `GET /graph/{graph_name}/vertices/{vertex_type}/{vertex_id}`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_retrieve_a_vertex

        TODO Find out how/if select and timeout can be specified
        """
        if not vertexIds:
            raise TigerGraphException("No vertex ID was specified.", None)
        vids = []
        if isinstance(vertexIds, (int, str)):
            vids.append(vertexIds)
        elif not isinstance(vertexIds, list):
            return None
            # TODO Should return 0 or raise exception?
        else:
            vids = vertexIds
        url = self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType + "/"

        ret = []
        for vid in vids:
            ret += self._get(url + self._safeChar(vid))

        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return self.vertexSetToDataFrame(ret, withId, withType)
        return ret

    def getVertexDataFrameById(self, vertexType: str, vertexIds: [int, str, list],
            select: str = "") -> pd.DataFrame:
        """Retrieves vertices of the given vertex type, identified by their ID.

        This is a shortcut to ``getVerticesById(..., fmt="df", withId=True, withType=False)``.

        Args:
            vertexType:
                The name of the vertex type.
            vertexIds:
                A single vertex ID or a list of vertex IDs.
            select:
                Comma separated list of vertex attributes to be retrieved.

        Returns:
            The (selected) details of the (matching) vertex instances as pandas DataFrame.
        """
        return self.getVerticesById(vertexType, vertexIds, select, fmt="df", withId=True,
            withType=False)

    def getVertexDataframeById(self, vertexType: str, vertexIds: [int, str, list],
            select: str = "") -> pd.DataFrame:
        """DEPRECATED

        Use `getVertexDataFrameById()` instead.

        TODO Proper deprecation
        """
        return self.getVertexDataFrameById(vertexType, vertexIds, select)

    def getVertexStats(self, vertexTypes: [str, list], skipNA: bool = False) -> dict:
        """Returns vertex attribute statistics.

        Args:
            vertexTypes:
                A single vertex type name or a list of vertex types names or "*" for all vertex
                types.
            skipNA:
                Skip those non-applicable vertices that do not have attributes or none of their
                attributes have statistics gathered.

        Returns:
            A dictionary of various vertex stats for each vertex type specified.

        Endpoint:
            - `POST /builtins/{graph_name}`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_run_built_in_functions_on_graph
        """
        vts = []
        if vertexTypes == "*":
            vts = self.getVertexTypes()
        elif isinstance(vertexTypes, str):
            vts = [vertexTypes]
        elif isinstance(vertexTypes, list):
            vts = vertexTypes
        else:
            return None
            # TODO Should return {} or raise exception instead?
        ret = {}
        for vt in vts:
            data = '{"function":"stat_vertex_attr","type":"' + vt + '"}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data, resKey="",
                skipCheck=True)
            if res["error"]:
                if "stat_vertex_attr is skip" in res["message"]:
                    if not skipNA:
                        ret[vt] = {}
                else:
                    raise TigerGraphException(res["message"],
                        (res["code"] if "code" in res else None))
            else:
                res = res["results"]
                for r in res:
                    ret[r["v_type"]] = r["attributes"]
        return ret

    def delVertices(self, vertexType: str, where: str = "", limit: str = "", sort: str = "",
            permanent: bool = False, timeout: int = 0) -> int:
        """Deletes vertices from graph.

        *Note*:
            The primary ID of a vertex instance is NOT an attribute, thus cannot be used in
            `select`, `where` or `sort` parameters (unless the `WITH primary_id_as_attribute` clause
            was used when the vertex type was created). /
            Use `delVerticesById()` if you need to retrieve vertices by their primary ID.

        Args:
            vertexType:
                The name of the vertex type.
            where:
                Comma separated list of conditions that are all applied on each vertex' attributes.
                The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            sort:
                Comma separated list of attributes the results should be sorted by.
                Must be used with `limit`.
            limit:
                Maximum number of vertex instances to be returned (after sorting).
                Must be used with `sort`.
            permanent:
                If true, the deleted vertex IDs can never be inserted back, unless the graph is
                dropped or the graph store is cleared.
           timeout:
                Time allowed for successful execution (0 = no limit, default).

        Returns:
             A single number of vertices deleted.

        The primary ID of a vertex instance is NOT an attribute, thus cannot be used in above
            arguments.

        Endpoint:
            - `DELETE /graph/{graph_name}/vertices/{vertex_type}`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_delete_vertices
        """
        url = self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType
        isFirst = True
        if where:
            url += "?filter=" + where
            isFirst = False
        if limit and sort:  # These two must be provided together
            url += ("?" if isFirst else "&") + "limit=" + str(limit) + "&sort=" + sort
            isFirst = False
        if permanent:
            url += ("?" if isFirst else "&") + "permanent=true"
            isFirst = False
        if timeout and timeout > 0:
            url += ("?" if isFirst else "&") + "timeout=" + str(timeout)
        return self._delete(url)["deleted_vertices"]

    def delVerticesById(self, vertexType: str, vertexIds: [int, str, list], permanent: bool = False,
            timeout: int = 0) -> int:
        """Deletes vertices from graph identified by their ID.

        Args:
            vertexType:
                The name of the vertex type.
            vertexIds:
                A single vertex ID or a list of vertex IDs.
            permanent:
                If true, the deleted vertex IDs can never be inserted back, unless the graph is
                dropped or the graph store is cleared.
            timeout:
                Time allowed for successful execution (0 = no limit, default).

        Returns:
            A single number of vertices deleted.

        Endpoint:
            - `DELETE /graph/{graph_name}/vertices/{vertex_type}/{vertex_id}`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_delete_a_vertex
        """
        if not vertexIds:
            raise TigerGraphException("No vertex ID was not specified.", None)
        vids = []
        if isinstance(vertexIds, (int, str)):
            vids.append(self._safeChar(vertexIds))
        elif not isinstance(vertexIds, list):
            return None
            # TODO Should return 0 or raise an exception instead?
        else:
            vids = [self._safeChar(f) for f in vertexIds]
        url1 = self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType + "/"
        url2 = ""
        if permanent:
            url2 = "?permanent=true"
        if timeout and timeout > 0:
            url2 += ("&" if url2 else "?") + "timeout=" + str(timeout)
        ret = 0
        for vid in vids:
            ret += self._delete(url1 + str(vid) + url2)["deleted_vertices"]
        return ret

    # def delVerticesByType(self, vertexType: str, permanent: bool = False):
    # TODO Implementation
    # TODO DELETE /graph/{graph_name}/delete_by_type/vertices/{vertex_type}/
    # TODO Maybe call it truncateVertex[Type] or delAllVertices?

    # TODO GET /deleted_vertex_check/{graph_name}

    def vertexSetToDataFrame(self, vertexSet: list, withId: bool = True,
            withType: bool = False) -> pd.DataFrame:
        """Converts a vertex set to Pandas DataFrame.

        Vertex sets are used for both the input and output of `SELECT` statements. They contain
        instances of vertices of the same type.
        For each vertex instance the vertex ID, the vertex type and the (optional) attributes are
        present (under `v_id`, `v_type` and `attributes` keys, respectively). /
        See an example in `edgeSetToDataFrame()`.

        A vertex set has this structure (when serialised as JSON):
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
        For more information on vertex sets see https://docs.tigergraph.com/gsql-ref/current/querying/declaration-and-assignment-statements#_vertex_set_variables .

        Args:
            vertexSet:
                A JSON array containing a vertex set in the format returned by queries (see below).
            withId:
                Include vertex primary ID as a column?
            withType:
                Include vertex type info as a column?

        Returns:
            A pandas DataFrame containing the vertex attributes (and optionally the vertex primary
            ID and type).
        """
        df = pd.DataFrame(vertexSet)
        cols = []
        if withId:
            cols.append(df["v_id"])
        if withType:
            cols.append(df["v_type"])
        cols.append(pd.DataFrame(df["attributes"].tolist()))
        return pd.concat(cols, axis=1)
