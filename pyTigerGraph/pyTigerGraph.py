import requests
import json
import re

class TigerGraphException(Exception):
    """Generic TigerGraph specific exception.

    Where possible, error message and code returned by TigerGraph will be used.
    """

    def __init__(self, message, code=None):
        self.message = message
        self.code = code

class TigerGraphConnection:
    """Python wrapper for TigerGraph's REST++ API.

    Common arguments used in methods:
    vertexType, sourceVertexType, targetVertexType -- The name of a vertex type in the graph.
                                                      Use `getVertexTypes()` to fetch the list of vertex types currently in the graph.
    vertexId, sourceVertexId, targetVertexId       -- The PRIMARY_ID of a vertex instance (of the appropriate data type).
    edgeType                                       -- The name of the edge type in the graph.
                                                      Use `getEdgeTypes()` to fetch the list of edge types currently in the graph.
    """

    def __init__(self, host="http://localhost", graphname="MyGraph", username="tigergraph", password="tigergraph", restppPort = "9000", studioPort = "14240", gsqlPort = "8123", apiToken=""):
        self.host = host
        self.username = username
        self.password = password
        self.graphname = graphname
        self.restppPort = restppPort
        self.restppUrl = self.host + ":" + self.restppPort
        self.gsqlPort = gsqlPort
        self.gsqlUrl = self.host + ":" + self.gsqlPort
        self.studioPort = studioPort
        self.apiToken = "Bearer " + apiToken
        self.authHeader = {'Authorization':self.apiToken}
        self.debug = True

    # Private functions ========================================================

    def _errorCheck(self,res):
        """Checks if the JSON document returned by an endpoint has contains error: true; if so, it raises an exception"""
        if "error" in res and res["error"]:
            raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def _req(self, method, url, authMode="pwd", headers=None, data=None, resKey="results", skipCheck=False, params=None):
        """Generic REST++ API request

        Arguments:
        - `method`:    HTTP method, currently one of GET, POST, DELETE or PUT
        - `url`:       Complete RESP++ API URL including path and parameters
        - `authMode`:  Authentication mode, one of 'pwd' (default) or 'token'
        - `headers`:   Standard HTTP request headers (dict)
        - `data`:      Request payload, typically a JSON document
        - `resKey`:    the JSON subdocument to be returned, default is 'result'
        - `skipCheck`: Skip error checking? Some endpoints return error to indicate that the requested action is not applicable; a problem, but not really an error.
        """
        if self.debug:
            print(method + " " + url + (" => " + data if data else ""))
        if authMode == "pwd":
            _auth=(self.username, self.password)
        else:
            _auth = None
        if authMode == "token":
            _headers = self.authHeader
        else:
            _headers = {}
        if headers:
            _headers.update(headers)
        if method == "POST":
            _data = data
        else:
            _data = None
        res = requests.request(method, url, auth=_auth, headers=_headers, data=_data, params=params)

        if self.debug:
            print(res.url)
        if res.status_code != 200:
            res.raise_for_status()
        res = json.loads(res.text)
        if not skipCheck:
            self._errorCheck(res)
        if not resKey:
            if self.debug:
                print(res)
            return res
        if self.debug:
            print(res[resKey])
        return res[resKey]

    def _get(self, url, authMode="pwd", headers=None, resKey="results", skipCheck=False, params=None):
        """Generic GET method

        For argument details, see `_req`.
        """
        return self._req("GET", url, authMode, headers, None, resKey, skipCheck, params)

    def _post(self, url, authMode="pwd", headers=None, data=None, resKey="results", skipCheck=False, params=None):
        """Generic GET method

        For argument details, see `_req`.
        """
        return self._req("POST", url, authMode, headers, data, resKey, skipCheck, params)

    def _delete(self, url):
        """Generic GET method

        For argument details, see `_req`.
        """
        return self._req("DELETE", url)

    def _upsertAttrs(self, attributes):
        """Transforms attributes (provided as a table) into a hierarchy as expect by the upsert functions"""
        if not isinstance(attributes, dict):
            return {}
        vals = {}
        for attr in attributes:
            val = attributes[attr]
            if isinstance(val, tuple):
                vals[attr] = {"value": val[0], "op": val[1]}
            else:
                vals[attr] = {"value": val}
        return vals

    # Schema related functions =================================================

    def _getUDTs(self):
        """Retrieves all User Defined Types (UDTs) of the graph.

        Endpoint:      GET /gsqlserver/gsql/udtlist
        Documentation: Not documented publicly
        """
        return self._get(self.gsqlUrl + "/gsqlserver/gsql/udtlist?graph=" + self.graphname)

    def getSchema(self, udts=True):
        """Retrieves the schema (all vertex and edge type and - if not disabled - the User Defined Type details) of the graph.

        Calls `_getUDTs()` if udts=True (default).

        Endpoint:      GET /gsqlserver/gsql/schema
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-the-graph-schema-get-gsql-schema
        """
        res = self._get(self.gsqlUrl + "/gsqlserver/gsql/schema?graph=" + self.graphname)
        if not udts:
            return res
        res["UDTs"] = self._getUDTs()
        return res

    def getUDTs(self):
        """Returns the list of User Defined Types (names only)."""
        ret = []
        for udt in self._getUDTs():
            ret.append(udt["name"])
        return ret

    def getUDT(self, udtName):
        """Returns the details of a specific User Defined Type."""
        for udt in self._getUDTs():
            if udt["name"] == udtName:
                return udt["fields"]
        return [] # UDT was not found

    def upsertData(self, data):
        """Upserts data (vertices and edges) from a JSON document or equivalent object structure.

        Endpoint:      POST /gsqlserver/gsql/schema
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-the-graph-schema-get-gsql-schema
        """
        if not isinstance(data, str):
            data = json.dumps(data)
        return self._post(self.restppUrl +  "/graph/" + self.graphname, data=data)[0]

    # Vertex related functions =================================================

    def getVertexTypes(self):
        """Returns the list of vertex type names of the graph."""
        ret = []
        for vt in self.getSchema()["VertexTypes"]:
            ret.append(vt["Name"])
        return ret

    def getVertexType(self, vertexType):
        """Returns the details of the specified vertex type."""
        for vt in self.getSchema()["VertexTypes"]:
            if vt["Name"] == vertexType:
                return vt
        return {} # Vertex type was not found

    def getVertexCount(self, vertexType, where=""):
        """Return the number of vertices.

        Uses:
        - If `vertexType` = "*": vertex count of all vertex types (`where` cannot be specified in this case)
        - If `vertexType` is specified only: vertex count of the given type
        - If `vertexType` and `where` are specified: vertex count of the given type after filtered by `where` condition(s)

        For valid values of `where` condition, see https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter

        Endpoint:      GET /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices
        Endpoint:      POST /builtins
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_vertex_number
        """
        # If WHERE condition is not specified, use /builtins else user /vertices
        if where:
            if vertexType == "*":
                raise TigerGraphException("VertexType cannot be \"*\" if where condition is specified.", None)
            res = self._get(self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType + "?count_only=true&filter=" + where, "token")
        else:
            data = '{"function":"stat_vertex_number","type":"' + vertexType + '"}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, "token", data=data)
        if len(res) == 1 and res[0]["v_type"] == vertexType:
            return res[0]["count"]
        ret = {}
        for r in res:
            ret[r["v_type"]] = r["count"]
        return ret

    def upsertVertex(self, vertexType, vertexId, attributes=None):
        """Upserts a vertex.

        Data is upserted:
        - If vertex is not yet present in graph, it will be created.
        - If it's already in the graph, its attributes are updated with the values specified in the request. An optional operator controls how the attributes are updated.

        The `attributes` argument is expected to be a dictionary in this format:
            {<attribute_name>: <attribute_value>|(<attribute_name>, <operator>), …}

        Example:
            {"name": "Thorin", points: (10, "+"), "bestScore": (67, "max")}

        For valid values of <operator> see: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data

        Endpoint:      POST /graph
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data
        """
        if not isinstance(attributes, dict):
            return None
        vals = self._upsertAttrs(attributes)
        data = json.dumps({"vertices": {vertexType: {vertexId: vals}}})
        return self._post(self.restppUrl +  "/graph/" + self.graphname, data=data)[0]["accepted_vertices"]

    def upsertVertices(self, vertexType, vertices):
        """Upserts multiple vertices (of the same type).

        See the description of `upsertVertex` for generic information.

        The `vertices` argument is expected to be a list of tuples in this format:
            [
                (<vertex_id>, {<attribute_name>, <attribute_value>|(<attribute_name>, <operator>), …}),
                ⋮
            ]

        Example:
            [
               (2, {"name": "Balin", "points": (10, "+"), "bestScore": (67, "max")}),
               (3, {"name": "Dwalin", "points": (7, "+"), "bestScore": (35, "max")})
            ]

        For valid values of <operator> see: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data

        Endpoint:      POST /graph
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data
        """
        if not isinstance(vertices, list):
            return None
        data = {}
        for v in vertices:
            vals = self._upsertAttrs(v[1])
            data[v[0]] = vals
        data = json.dumps({"vertices": {vertexType: data}})
        return self._post(self.restppUrl +  "/graph/" + self.graphname, data=data)[0]["accepted_vertices"]

    def getVertices(self, vertexType, select="", where="", limit="", sort="", timeout=0):
        """Retrieves vertices of the given vertex type.

        Arguments:
        - `select`: Comma separated list of vertex attributes to be retrieved or omitted.
                    See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select
        - `where`:  Comma separated list of conditions that are all applied on each vertex' attributes.
                    The conditions are in logical conjunction (i.e. they are "AND'ed" together).
                    See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter
        - `limit`:  Maximum number of vertex instances to be returned (after sorting).
                    See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit
        - `sort`    Comma separated list of attributes the results should be sorted by.
                    See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort

        NOTE: The primary ID of a vertex instance is NOT an attribute, thus cannot be used in above arguments.
              Use `getVerticesById` if you need to retrieve by vertex ID.

        Endpoint:      GET /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices
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
        return self._get(url)

    def getVerticesById(self, vertexType, vertexIds):
        """Retrieves vertices of the given vertex type, identified by their ID.

        Arguments
        - `vertexIds`: A single vertex ID or a list of vertex IDs.

        Endpoint:      GET /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices
        """
        if not vertexIds:
            raise TigerGraphException("No vertex ID was not specified.", None)
        vids = []
        if isinstance(vertexIds, (int, str)):
            vids.append(vertexIds)
        elif not isinstance(vertexIds, list):
            return None # TODO: a better return value?
        else:
            vids = vertexIds
        url = self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType + "/"
        ret = []
        for vid in vids:
            ret += self._get(url + str(vid))
        return ret

    def getVertexStats(self, vertexTypes, skipNA=False):
        """Returns vertex attribute statistics.

        Arguments:
        vertexTypes -- A single vertex type name or a list of vertex types names or '*' for all vertex types.
        skipNA      -- Skip those non-applicable vertices that do not have attributes or none of their attributes have statistics gathered.

        Endpoint:      POST /builtins
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_vertex_attr
        """
        vts = []
        if vertexTypes == "*":
            vts = self.getVertexTypes()
        elif isinstance(vertexTypes, str):
            vts =[vertexTypes]
        elif isinstance(vertexTypes, list):
            vts = vertexTypes
        else:
            return None
        ret = {}
        for vt in vts:
            data = '{"function":"stat_vertex_attr","type":"' + vt + '"}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, "token", data=data, resKey=None, skipCheck=True)
            if res["error"]:
                if "stat_vertex_attr is skipped" in res["message"]:
                    if not skipNA:
                        ret[vt] = {}
                else:
                    raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))
            else:
                res = res["results"]
                for r in res:
                    ret[r["v_type"]] = r["attributes"]
        return ret

    def delVertices(self, vertexType, where="", limit="", sort="", permanent=False, timeout=0):
        """Deletes vertices from graph.

        Arguments:
        - `where`:     Comma separated list of conditions that are all applied on each vertex' attributes.
                       The conditions are in logical conjunction (i.e. they are "AND'ed" together).
                       See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter
        - `limit`:     Maximum number of vertex instances to be returned (after sorting).
                       See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit
                       Must be used with `sort`.
        - `sort`       Comma separated list of attributes the results should be sorted by.
                       See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort
                       Must be used with `limit`.
        - `permanent`: If true, the deleted vertex IDs can never be inserted back, unless the graph is dropped or the graph store is cleared.
        - `timeout`:   Time allowed for successful execution (0 = no limit, default).

        NOTE: The primary ID of a vertex instance is NOT an attribute, thus cannot be used in above arguments.
              Use `delVerticesById` if you need to delete by vertex ID.

        Returns: The actual number of vertices deleted

        Endpoint:      DELETE /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-graph-graph_name-vertices
        """
        url = self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType
        isFirst = True
        if where:
            url += "?filter=" + where
            isFirst = False
        if limit and sort: # These two must be provided together
            url += ("?" if isFirst else "&") + "limit=" + str(limit) + "&sort=" + sort
            isFirst = False
        if permanent:
            url += ("?" if isFirst else "&") + "permanent=true"
            isFirst = False
        if timeout and timeout > 0:
            url += ("?" if isFirst else "&") + "timeout=" + str(timeout)
        return self._delete(url)["deleted_vertices"]

    def delVerticesById(self, vertexType, vertexIds, permanent=False, timeout=0):
        """Deletes vertices from graph identified by their ID.

        Arguments:
        - `vertexIds`: A single vertex ID or a list of vertex IDs.
        - `permanent`: If true, the deleted vertex IDs can never be inserted back, unless the graph is dropped or the graph store is cleared.
        - `timeout`:   Time allowed for successful execution (0 = no limit, default).

        Returns: The actual number of vertices deleted

        Endpoint:      DELETE /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-graph-graph_name-vertices
        """
        if not vertexIds:
            raise TigerGraphException("No vertex ID was not specified.", None)
        vids = []
        if isinstance(vertexIds, (int, str)):
            vids.append(vertexIds)
        elif not isinstance(vertexIds, list):
            return None # TODO: a better return value?
        else:
            vids = vertexIds
        url1 = self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType + "/"
        url2 = ""
        if permanent:
            url2 = "?permanent=true"
        if timeout and timeout > 0:
            url2 +=  ("&" if url2 else "?") + "timeout=" + str(timeout)
        ret = 0
        for vid in vids:
            ret += self._delete(url1 + str(vid) + url2)["deleted_vertices"]
        return ret

    # Edge related functions ===================================================

    def getEdgeTypes(self):
        """Returns the list of edge type names of the graph."""
        ret = []
        ets = self.getSchema()["EdgeTypes"]
        for et in ets:
            ret.append(et["Name"])
        return ret

    def getEdgeType(self, typeName):
        """Returns the details of vertex type."""
        ets = self.getSchema()["EdgeTypes"]
        for et in ets:
            if et["Name"] == typeName:
                return et
        return {}

    def getEdgeCount(self, sourceVertexType=None, sourceVertexId=None, edgeType=None, targetVertexType=None, targetVertexId=None, where=""):
        """Return the number of edges.

        Arguments
        - `where`:  Comma separated list of conditions that are all applied on each edge's attributes.
                    The conditions are in logical conjunction (i.e. they are "AND'ed" together).
                    See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter
        Uses:
        - If `edgeType` = "*": edge count of all edge types (no other arguments can be specified in this case).
        - If `edgeType` is specified only: edge count of the given edge type.
        - If `sourceVertexType`, `edgeType`, `targetVertexType` are specified: edge count of the given edge type between source and target vertex types.
        - If `sourceVertexType`, `sourceVertexId` are specified: edge count of all edge types from the given vertex instance.
        - If `sourceVertexType`, `sourceVertexId`, `edgeType` are specified: edge count of all edge types from the given vertex instance.
        - If `sourceVertexType`, `sourceVertexId`, `edgeType`, `where` are specified: the edge count of the given edge type after filtered by `where` condition.

        If `targetVertexId` is specified, then `targetVertexType` must also be specified.
        If `targetVertexType` is specified, then `edgeType` must also be specified.

        For valid values of `where` condition, see https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter

        Endpoint:      GET /graph/{graph_name}/edges
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-edges
        Endpoint:      POST /builtins
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_edge_number
        """
        # If WHERE condition is not specified, use /builtins else user /vertices
        if where or (sourceVertexType and sourceVertexId):
            if not sourceVertexType or not sourceVertexId:
                raise TigerGraphException("If where condition is specified, then both sourceVertexType and sourceVertexId must be provided too.", None)
            url = self.restppUrl + "/graph/" + self.graphname + "/edges/" + sourceVertexType + "/" + str(sourceVertexId)
            if edgeType:
                url += "/" + edgeType
                if targetVertexType:
                    url += "/" + targetVertexType
                    if targetVertexId:
                        url += "/" + str(targetVertexId)
            url += "?count_only=true"
            if where:
                url += "&filter=" + where
            res = self._get(url)
        else:
            if not edgeType: # TODO is this a valid check?
                raise TigerGraphException("A valid edge type or \"*\" must be specified for edgeType if where condition is set.", None)
            data = '{"function":"stat_edge_number","type":"' + edgeType + '"' \
                + (',"from_type":"' + sourceVertexType + '"' if sourceVertexType else '')  \
                + (',"to_type":"' + targetVertexType + '"' if targetVertexType else '')  \
                + '}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, "token", data=data)
        if len(res) == 1 and res[0]["e_type"] == edgeType:
            return res[0]["count"]
        ret = {}
        for r in res:
            ret[r["e_type"]] = r["count"]
        return ret

    def upsertEdge(self, sourceVertexType, sourceVertexId, edgeType, targetVertexType, targetVertexId, attributes={}):
        """Upserts an edge.

        Data is upserted:
        - If edge is not yet present in graph, it will be created (see special case below).
        - If it's already in the graph, it is updated with the values specified in the request.

        The `attributes` argument is expected to be a dictionary in this format:
            {<attribute_name>, <attribute_value>|(<attribute_name>, <operator>), …}

        Example:
            {"visits": (1482, "+"), "max_duration": (371, "max")}

        For valid values of <operator> see: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data

        Note: If operator is "vertex_must_exist" then edge will only be created if both vertex exists in graph.
              Otherwise missing vertices are created with the new edge.

        Endpoint:      POST /graph
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data
        """
        if not isinstance(attributes, dict):
            return None
        vals = self._upsertAttrs(attributes)
        data = json.dumps({"edges": {sourceVertexType: {sourceVertexId: {edgeType: {targetVertexType: {targetVertexId: vals}}}}}})
        return self._post(self.restppUrl +  "/graph/" + self.graphname, data=data)[0]["accepted_edges"]

    def upsertEdges(self, sourceVertexType, edgeType, targetVertexType, edges):
        """Upserts multiple edges (of the same type).

        See the description of `upsertEdge` for generic information.

        The `edges` argument is expected to be a list in of tuples in this format:
        [
          (<source_vertex_id>, <target_vertex_id>, {<attribute_name>: <attribute_value>|(<attribute_name>, <operator>), …})
          ⋮
        ]

        Example:
            [
              (17, "home_page", {"visits": (35, "+"), "max_duration": (93, "max")}),
              (42, "search", {"visits": (17, "+"), "max_duration": (41, "max")}),
            ]

        For valid values of <operator> see: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data

        Endpoint:      POST /graph
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data
        """
        if not isinstance(edges, list):
            return None
        data = {}
        data[sourceVertexType] = {}
        l1 = data[sourceVertexType]
        for e in edges:
            vals = self._upsertAttrs(e[2])
            # fromVertexId
            if e[0] not in l1:
                l1[e[0]] = {}
            l2 = l1[e[0]]
            # edgeType
            if edgeType not in l2:
                l2[edgeType] = {}
            l3 = l2[edgeType]
            # targetVertexType
            if targetVertexType not in l3:
                l3[targetVertexType] = {}
            l4 = l3[targetVertexType]
            # targetVertexId
            l4[e[1]] = vals
        data = json.dumps({"edges": data})
        return self._post(self.restppUrl +  "/graph/" + self.graphname, data=data)[0]["accepted_edges"]

    def getEdges(self, sourceVertexType, sourceVertexId, edgeType=None, targetVertexType=None, targetVertexId=None, select="", where="", limit="", sort="", timeout=0):
        """Retrieves edges of the given edge type.

        Only `sourceVertexType` and `sourceVertexId` are required.
        If `targetVertexId` is specified, then `targetVertexType` must also be specified.
        If `targetVertexType` is specified, then `edgeType` must also be specified.

        Arguments:
        - `select`: Comma separated list of edge attributes to be retrieved or omitted.
                    See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select
        - `where`:  Comma separated list of conditions that are all applied on each edge's attributes.
                    The conditions are in logical conjunction (i.e. they are "AND'ed" together).
                    See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter
        - `limit`:  Maximum number of edge instances to be returned (after sorting).
                    See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit
        - `sort`    Comma separated list of attributes the results should be sorted by.
                    See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort

        Endpoint:      GET /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices
        """
        if not sourceVertexType or not sourceVertexId:
            raise TigerGraphException("Both sourceVertexType and sourceVertexId must be provided.", None)
        url = self.restppUrl + "/graph/" + self.graphname + "/edges/" + sourceVertexType + "/" + str(sourceVertexId)
        if edgeType:
            url += "/" + edgeType
            if targetVertexType:
                url += "/" + targetVertexType
                if targetVertexId:
                    url += "/" + str(targetVertexId)
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
        return self._get(url)

    def getEdgeStats(self, edgeTypes, skipNA=False):
        """Returns edge attribute statistics.

        Arguments:
        edgeTypes -- A single edge type name or a list of edges types names or '*' for all edges types
        skipNA    -- Skip those edges that do not have attributes or none of their attributes have statistics gathered

        Endpoint:      POST /builtins
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_edge_attr
        """
        ets = []
        if edgeTypes == "*":
            ets = self.getEdgeTypes()
        elif isinstance(edgeTypes, str):
            ets = [edgeTypes]
        elif isinstance(edgeTypes, list):
            ets = edgeTypes
        else:
            return None
        ret = {}
        for et in ets:
            data = '{"function":"stat_edge_attr","type":"' + et + '","from_type":"*","to_type":"*"}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, "token", data=data, resKey=None, skipCheck=True)
            if res["error"]:
                if "stat_edge_attr is skiped" in res["message"]:
                    if not skipNA:
                        ret[et] = {}
                else:
                    raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))
            else:
                res = res["results"]
                for r in res:
                    ret[r["e_type"]] = r["attributes"]
        return ret

    def delEdges(self, sourceVertexType, sourceVertexId, edgeType=None, targetVertexType=None, targetVertexId=None, where="", limit="", sort="", timeout=0):
        """Deletes edges from the graph.

        Only `sourceVertexType` and `sourceVertexId` are required.
        If `targetVertexId` is specified, then `targetVertexType` must also be specified.
        If `targetVertexType` is specified, then `edgeType` must also be specified.

        Arguments:
        - `where`:   Comma separated list of conditions that are all applied on each edge's attributes.
                     The conditions are in logical conjunction (i.e. they are "AND'ed" together).
                     See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter
        - `limit`:   Maximum number of edge instances to be returned (after sorting).
                     See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit
        - `sort`     Comma separated list of attributes the results should be sorted by.
                     See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort
        - `timeout`: Time allowed for successful execution (0 = no limit, default).

        Endpoint:      DELETE /graph/{/graph_name}/edges
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-graph-graph_name-edges
        """
        if not sourceVertexType or not sourceVertexId:
            raise TigerGraphException("Both sourceVertexType and sourceVertexId must be provided.", None)
        url = self.restppUrl + "/graph/" + self.graphname + "/edges/" + sourceVertexType + "/" + str(sourceVertexId)
        if edgeType:
            url += "/" + edgeType
            if targetVertexType:
                url += "/" + targetVertexType
                if targetVertexId:
                    url += "/" + str(targetVertexId)
        isFirst = True
        if where:
            url += ("?" if isFirst else "&") + "filter=" + where
            isFirst = False
        if limit and sort: # These two must be provided together
            url += ("?" if isFirst else "&") + "limit=" + str(limit) + "&sort=" + sort
            isFirst = False
        if timeout and timeout > 0:
            url += ("?" if isFirst else "&") + "timeout=" + str(timeout)
        res = self._delete(url)
        ret = {}
        for r in res:
            ret[r["e_type"]] = r["deleted_edges"]
        return ret

    # Query related functions ==================================================

    def runInstalledQuery(self, queryName, params=None, timeout=16000, sizeLimit=32000000):
        """Runs an installed query.

        The query must be already created and installed in the graph.
        Use `getEndpoints(dynamic=True)` or GraphStudio to find out the generated endpoint URL of the query, but only the query name needs to be specified here.

        Arguments:
        - `params`:    A string of param1=value1&param2=value2 format or a dictionary.
        - `timeout`:   Maximum duration for successful query execution.
        - `sizeLimit`: Maximum size of response (in bytes).

        Endpoint:      POST /query/{graph_name}/<query_name>
        Documentation: https://docs.tigergraph.com/dev/gsql-ref/querying/query-operations#running-a-query
        """
        return self._get(self.restppUrl + "/query/" + self.graphname + "/" + queryName, params=params, authMode="token", headers={"RESPONSE-LIMIT": str(sizeLimit), "GSQL-TIMEOUT": str(timeout)})

    def runInterpretedQuery(self, queryText, params=None):
        """Runs an interpreted query.

        You must provide the query text in this format:
            INTERPRET QUERY (<params>) FOR GRAPH <graph_name> {
               <statements>
            }'

        Arguments:
        - `params`:    A string of param1=value1&param2=value2 format or a dictionary.

        Endpoint:      POST /gsqlserver/interpreted_query
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-gsqlserver-interpreted_query-run-an-interpreted-query
        """
        return self._post(self.gsqlUrl +"/gsqlserver/interpreted_query", data=queryText, params=params)

    # Token management =========================================================

    def getToken(self, secret, lifetime=None):
        """Requests an authorisation token.

        Arguments:
        - `secret`:   Generated in GSQL using `CREATE SECRET`.
                      See https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#create-show-drop-secret
        - `lifetime`: Duration of token validity (in secs, default 30 days = 2,592,000 secs).

        Endpoint:      GET /requesttoken
        Documentation: https://docs.tigergraph.com/dev/restpp-api/restpp-requests#requesting-a-token-with-get-requesttoken
        """
        queryUrl = self.restppUrl + "/requesttoken?secret=" + secret + ("&lifetime=" + lifetime if lifetime else "")
        response = requests.request("GET", queryUrl, auth=(self.username, self.password))
        return json.loads(response.text)

    def refreshToken(self, secret, token, lifetime):
        """Extends a tokens lifetime.

        Arguments:
        - `secret`:   Generated in GSQL using `CREATE SECRET`.
                      See https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#create-show-drop-secret
        - `token`:    The token requested earlier.
        - `lifetime`: Duration of token validity (in secs, default 30 days = 2,592,000 secs).

        Endpoint:      PUT /requesttoken
        Documentation: https://docs.tigergraph.com/dev/restpp-api/restpp-requests#refreshing-tokens
        """
        queryUrl = self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token + "&lifetime=" + lifetime
        response = requests.request("PUT", queryUrl, auth=(self.username, self.password))
        return json.loads(response.text)

    def deleteToken(self, secret, token):
        """Deletes a token.

        Arguments:
        - `secret`:   Generated in GSQL using `CREATE SECRET`.
                      See https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#create-show-drop-secret
        - `token`:    The token requested earlier.

        Endpoint:      DELETE /requesttoken
        Documentation: https://docs.tigergraph.com/dev/restpp-api/restpp-requests#deleting-tokens
        """
        queryUrl = self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token
        response = requests.request("DELETE", queryUrl, auth=(self.username, self.password))
        return json.loads(response.text)

    # Other functions ==========================================================

    def echo(self):
        """Pings the database.

        Expected return value is "Hello GSQL"

        Endpoint:      GET /echo  and  POST /echo
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-echo-and-post-echo
        """
        return self._get(self.restppUrl + "/echo/" + self.graphname, resKey="message")

    def getEndpoints(self, builtin=False, dynamic=False, static=False):
        """Lists the RESP++ endpoints and their parameters.

        Arguments:
        - `builtin -- TigerGraph provided REST++ endpoints.
        - `dymamic -- Endpoints for user installed queries.
        - `static  -- Static endpoints.

        If none of the above arguments are specified, all endpoints are listed

        Endpoint:      GET /endpoints
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-endpoints
        """
        ret = {}
        if not (builtin or dynamic or static):
            bui = dyn = sta = True
        else:
            bui = builtin
            dyn = dynamic
            sta = static
        url = self.restppUrl + "/endpoints/" + self.graphname + "?"
        if bui:
            eps = {}
            res = self._get(url + "builtin=true", resKey=None)
            for ep in res:
                if not re.search(" /graph/", ep) or re.search(" /graph/{graph_name}/", ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if dyn:
            res = self._get(url + "dynamic=true", resKey=None)
            eps = {}
            for ep in res:
                if re.search("^GET /query/" + self.graphname, ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if sta:
            ret.update(self._get(url + "static=true", resKey=None))
        return ret

    def getStatistics(self, seconds=10, segment=10):
        """Retrieves real-time query performance statistics over the given time period.

        Endpoint:      GET /statistics
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-statistics
        """
        if not seconds or type(seconds) != "int":
            seconds = 10
        else:
            seconds = max(min(seconds,0),60)
        if not segment or type(segment) != "int":
            segment = 10
        else:
            segment = max(min(segment,0),100)
        return self._get(self.restppUrl + "/statistics/" + self.graphname + "?seconds=" + str(seconds) + "&segment=" + str(segment), resKey=None)

    def getVersion(self):
        """Retrieves the git versions of all components of the system.

        Endpoint:      GET /version
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-version
        """
        response = requests.request("GET", self.restppUrl + "/version/" + self.graphname, headers=self.authHeader)
        res = json.loads(response.text, strict=False)["message"].split("\n") # "strict=False" is why _get() was not used
        components = []
        for i in range(len(res)):
            if i > 2 and i < len(res) - 1:
                m = res[i].split()
                component = {"name": m[0], "version": m[1], "hash": m[2], "datetime": m[3] + " " + m[4] + " " + m[5]}
                components.append(component)
        return components

    def getVer(self, component="product", full=False):
        """Gets the version information of specific component

        Get the full list of components using `getVersion`.
        """
        ret = ""
        for v in self.getVersion():
            if v["name"] == component:
                ret = v["version"]
        if ret != "":
            if full:
                return ret
            ret = re.search("_.+_", ret)
            return ret.group().strip("_")
        else:
            raise TigerGraphException("\"" + component + "\" is not a valid component.", None)

    # A tale from the Loop

# EOF
