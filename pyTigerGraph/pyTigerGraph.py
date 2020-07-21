import requests
import json
import re
from datetime import datetime
import time
import pandas as pd


class TigerGraphException(Exception):
    """Generic TigerGraph specific exception.

    Where possible, error message and code returned by TigerGraph will be used.
    """

    def __init__(self, message, code=None):
        self.message = message
        self.code = code


class TigerGraphConnection(object):
    """Python wrapper for TigerGraph's REST++ API.

    Common arguments used in methods:
    vertexType, sourceVertexType, targetVertexType -- The name of a vertex type in the graph.
                                                      Use `getVertexTypes()` to fetch the list of vertex types currently in the graph.
    vertexId, sourceVertexId, targetVertexId       -- The PRIMARY_ID of a vertex instance (of the appropriate data type).
    edgeType                                       -- The name of the edge type in the graph.
                                                      Use `getEdgeTypes()` to fetch the list of edge types currently in the graph.
    """

    def __init__(self, host="http://localhost", graphname="MyGraph", username="tigergraph", password="tigergraph", restppPort="9000", gsPort="14240", apiToken=""):
        self.host = host
        self.username = username
        self.password = password
        self.graphname = graphname
        self.restppPort = str(restppPort)
        self.restppUrl = self.host + ":" + self.restppPort
        self.gsPort = str(gsPort)
        self.gsUrl = self.host + ":" + self.gsPort
        self.apiToken = apiToken
        self.authHeader = {'Authorization': "Bearer " + self.apiToken}
        self.debug = False
        self.schema = None
        self.ttkGetEF = None

    # Private functions ========================================================

    def _errorCheck(self, res):
        """Checks if the JSON document returned by an endpoint has contains error: true; if so, it raises an exception"""
        if "error" in res and res["error"]:
            raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def _req(self, method, url, authMode="token", headers=None, data=None, resKey="results", skipCheck=False, params=None):
        """Generic REST++ API request

        Arguments:
        - `method`:    HTTP method, currently one of GET, POST, DELETE or PUT
        - `url`:       Complete REST++ API URL including path and parameters
        - `authMode`:  Authentication mode, one of 'token' (default) or 'pwd'
        - `headers`:   Standard HTTP request headers (dict)
        - `data`:      Request payload, typically a JSON document
        - `resKey`:    the JSON subdocument to be returned, default is 'result'
        - `skipCheck`: Skip error checking? Some endpoints return error to indicate that the requested action is not applicable; a problem, but not really an error.
        - `params`:    Request URL parameters.
        """
        if self.debug:
            print(method + " " + url + (" => " + data if data else ""))
        if authMode == "pwd":
            _auth = (self.username, self.password)
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

    def _get(self, url, authMode="token", headers=None, resKey="results", skipCheck=False, params=None):
        """Generic GET method

        For argument details, see `_req`.
        """
        return self._req("GET", url, authMode, headers, None, resKey, skipCheck, params)

    def _post(self, url, authMode="token", headers=None, data=None, resKey="results", skipCheck=False, params=None):
        """Generic GET method

        For argument details, see `_req`.
        """
        return self._req("POST", url, authMode, headers, data, resKey, skipCheck, params)

    def _delete(self, url, authMode="token"):
        """Generic GET method

        For argument details, see `_req`.
        """
        return self._req("DELETE", url, authMode)

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
        return self._get(self.gsUrl + "/gsqlserver/gsql/udtlist?graph=" + self.graphname, authMode="pwd")

    def getSchema(self, udts=True, force=False):
        """Retrieves the schema (all vertex and edge type and - if not disabled - the User Defined Type details) of the graph.

        Arguments:
        - `udts`: If `True`, calls `_getUDTs()`, i.e. includes User Defined Types in the schema details.
        - `force`: If `True`, retrieves the schema details again, otherwise returns a cached copy of the schema details (if they were already fetched previously).

        Endpoint:      GET /gsqlserver/gsql/schema
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-the-graph-schema-get-gsql-schema
        """
        if not self.schema or force:
            self.schema = self._get(self.gsUrl + "/gsqlserver/gsql/schema?graph=" + self.graphname, authMode="pwd")
            if udts:
                self.schema["UDTs"] = self._getUDTs()
        return self.schema

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
        return []  # UDT was not found

    def upsertData(self, data):
        """Upserts data (vertices and edges) from a JSON document or equivalent object structure.

        Endpoint:      POST /graph
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-the-graph-schema-get-gsql-schema
        """
        if not isinstance(data, str):
            data = json.dumps(data)
        return self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0]

    # Vertex related functions =================================================

    def getVertexTypes(self, force=False):
        """Returns the list of vertex type names of the graph.
        
        Arguments:
        - `force`: If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of vertex type details (if they were already fetched previously).
        """
        ret = []
        for vt in self.getSchema(force=force)["VertexTypes"]:
            ret.append(vt["Name"])
        return ret

    def getVertexType(self, vertexType, force=False):
        """Returns the details of the specified vertex type.
        
        Arguments:
        - `vertexType`: The name of of the vertex type.
        - `force`: If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of vertex type details (if they were already fetched previously).
        """
        for vt in self.getSchema(force=force)["VertexTypes"]:
            if vt["Name"] == vertexType:
                return vt
        return {}  # Vertex type was not found

    def getVertexCount(self, vertexType, where=""):
        """Returns the number of vertices.

        Uses:
        - If `vertexType` = "*": vertex count of all vertex types (`where` cannot be specified in this case)
        - If `vertexType` is specified only: vertex count of the given type
        - If `vertexType` and `where` are specified: vertex count of the given type after filtered by `where` condition(s)

        For valid values of `where` condition, see https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter

        Returns a dictionary of <vertex_type>: <vertex_count> pairs.

        Endpoint:      GET /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices
        Endpoint:      POST /builtins
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_vertex_number
        """
        # If WHERE condition is not specified, use /builtins else user /vertices
        if where:
            if vertexType == "*":
                raise TigerGraphException("VertexType cannot be \"*\" if where condition is specified.", None)
            res = self._get(self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType + "?count_only=true&filter=" + where)
        else:
            data = '{"function":"stat_vertex_number","type":"' + vertexType + '"}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data)
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

        Returns a single number of accepted (successfully upserted) vertices (0 or 1).

        Endpoint:      POST /graph
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data
        """
        if not isinstance(attributes, dict):
            return None
        vals = self._upsertAttrs(attributes)
        data = json.dumps({"vertices": {vertexType: {vertexId: vals}}})
        return self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0]["accepted_vertices"]

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

        Returns a single number of accepted (successfully upserted) vertices (0 or positive integer).

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
        return self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0]["accepted_vertices"]

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
            raise TigerGraphException("No vertex ID was specified.", None)
        vids = []
        if isinstance(vertexIds, (int, str)):
            vids.append(vertexIds)
        elif not isinstance(vertexIds, list):
            return None  # TODO: a better return value?
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
            vts = [vertexTypes]
        elif isinstance(vertexTypes, list):
            vts = vertexTypes
        else:
            return None
        ret = {}
        for vt in vts:
            data = '{"function":"stat_vertex_attr","type":"' + vt + '"}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data, resKey=None, skipCheck=True)
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

        Returns a single number of vertices deleted.

        Endpoint:      DELETE /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-graph-graph_name-vertices
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

    def delVerticesById(self, vertexType, vertexIds, permanent=False, timeout=0):
        """Deletes vertices from graph identified by their ID.

        Arguments:
        - `vertexIds`: A single vertex ID or a list of vertex IDs.
        - `permanent`: If true, the deleted vertex IDs can never be inserted back, unless the graph is dropped or the graph store is cleared.
        - `timeout`:   Time allowed for successful execution (0 = no limit, default).

        Returns a single number of vertices deleted.

        Endpoint:      DELETE /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-graph-graph_name-vertices
        """
        if not vertexIds:
            raise TigerGraphException("No vertex ID was not specified.", None)
        vids = []
        if isinstance(vertexIds, (int, str)):
            vids.append(vertexIds)
        elif not isinstance(vertexIds, list):
            return None  # TODO: a better return value?
        else:
            vids = vertexIds
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

    # Edge related functions ===================================================

    def getEdgeTypes(self, force=False):
        """Returns the list of edge type names of the graph.
        
        Arguments:
        - `force`: If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of edge type details (if they were already fetched previously).
        """
        ret = []
        for et in self.getSchema(force=force)["EdgeTypes"]:
            ret.append(et["Name"])
        return ret

    def getEdgeType(self, edgeType, force=False):
        """Returns the details of vertex type.
        
        Arguments:
        - `edgeType`: The name of the edge type.
        - `force`: If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of edge type details (if they were already fetched previously).
        """
        for et in self.getSchema(force=force)["EdgeTypes"]:
            if et["Name"] == edgeType:
                return et
        return {}

    def getEdgeSourceVertexType(self, edgeType):
        """Returns the type of the edge type's source vertex.
        
        Arguments:
        - `edgeType`: The name of the edge type.
        """
        edgeTypeDetails = self.getEdgeType(edgeType)
        if edgeTypeDetails["FromVertexTypeName"] == "*":
            return "*"
        fromVertexTypes = edgeTypeDetails["FromVertexTypeList"]
        if len(fromVertexTypes) == 1:
            return fromVertexTypes[0]
        return fromVertexTypes

    def getEdgeTargetVertexType(self, edgeType):
        """Returns the type of the edge type's target vertex.
        
        Arguments:
        - `edgeType`: The name of the edge type.
        """
        edgeTypeDetails = self.getEdgeType(edgeType)
        if edgeTypeDetails["ToVertexTypeName"] == "*":
            return "*"
        toVertexTypes = self.getEdgeType(edgeType)["ToVertexTypeList"]
        if len(toVertexTypes) == 1:
            return toVertexTypes[0]
        return toVertexTypes

    def isDirected(self, edgeType):
        """Is the specified edge type directed?
        
        Arguments:
        - `edgeType`: The name of the edge type.
        """
        return self.getEdgeType(edgeType)["IsDirected"]

    def getReverseEdge(self, edgeType):
        """Returns the name of the reverse edge of the specified edge type, if applicable.
        
        Arguments:
        - `edgeType`: The name of the edge type.
        """
        if not self.isDirected(edgeType):
            return None
        config = self.getEdgeType(edgeType)["Config"]
        if "REVERSE_EDGE" in config:
            return config["REVERSE_EDGE"]
        return None

    def getEdgeCountFrom(self, sourceVertexType=None, sourceVertexId=None, edgeType=None, targetVertexType=None, targetVertexId=None, where=""):
        """Returns the number of edges from a specific vertex.

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

        Returns a dictionary of <edge_type>: <edge_count> pairs.
 
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
            if not edgeType:  # TODO is this a valid check?
                raise TigerGraphException("A valid edge type or \"*\" must be specified for edge type.", None)
            data = '{"function":"stat_edge_number","type":"' + edgeType + '"' \
                + (',"from_type":"' + sourceVertexType + '"' if sourceVertexType else '')  \
                + (',"to_type":"' + targetVertexType + '"' if targetVertexType else '')  \
                + '}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data)
        if len(res) == 1 and res[0]["e_type"] == edgeType:
            return res[0]["count"]
        ret = {}
        for r in res:
            ret[r["e_type"]] = r["count"]
        return ret

    def getEdgeCount(self, edgeType="*", sourceVertexType=None, targetVertexType=None):
        return self.getEdgeCountFrom(edgeType=edgeType, sourceVertexType=sourceVertexType, targetVertexType=targetVertexType)
    """Returns the number of edges of an edge type.
    
    This is a simplified version of `getEdgeCountFrom`, to be used when the total number of edges of a given type is needed, regardless which vertex instance they are originated from.
    See documentation of `getEdgeCountFrom` above for more details.
    """

    def upsertEdge(self, sourceVertexType, sourceVertexId, edgeType, targetVertexType, targetVertexId, attributes=None):
        """Upserts an edge.

        Data is upserted:
        - If edge is not yet present in graph, it will be created (see special case below).
        - If it's already in the graph, it is updated with the values specified in the request.

        The `attributes` argument is expected to be a dictionary in this format:
            {<attribute_name>, <attribute_value>|(<attribute_name>, <operator>), …}

        Example:
            {"visits": (1482, "+"), "max_duration": (371, "max")}

        For valid values of <operator> see: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data

        Returns a single number of accepted (successfully upserted) edges (0 or 1).

        Note: If operator is "vertex_must_exist" then edge will only be created if both vertex exists in graph.
              Otherwise missing vertices are created with the new edge.

        Endpoint:      POST /graph
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data
        """
        if attributes is None:
            attributes = {}
        if not isinstance(attributes, dict):
            return None
        vals = self._upsertAttrs(attributes)
        data = json.dumps({"edges": {sourceVertexType: {sourceVertexId: {edgeType: {targetVertexType: {targetVertexId: vals}}}}}})
        return self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0]["accepted_edges"]

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

        Returns a single number of accepted (successfully upserted) edges (0 or positive integer).

        Endpoint:      POST /graph
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data
        """
        if not isinstance(edges, list):
            return None
        data = {sourceVertexType: {}}
        l1 = data[sourceVertexType]
        for e in edges:
            if len(e)>2:
                vals = self._upsertAttrs(e[2])
            else:
                vals = {}
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
        return self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0]["accepted_edges"]

    def getEdges(self, sourceVertexType, sourceVertexId, edgeType=None, targetVertexType=None, targetVertexId=None, select="", where="", limit="", sort="", timeout=0):
        """Retrieves edges of the given edge type originating from a specific source vertex.

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
            raise TigerGraphException("Both source vertex type and source vertex ID must be provided.", None)
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

    def getEdgesByType(self, edgeType):
        """Retrieves edges of the given edge type regardless the source vertex.
        
        Note: Edge attributes are not currently returned.
        
        Arguments:
        - `edgeType`: The name of the edge type.
        """
        if not edgeType:
            return []
        
        # Check if ttk_getEdgesFrom query was installed
        if self.ttkGetEF == None:
            self.ttkGetEF = False
            eps = self.getEndpoints(dynamic=True)
            for ep in eps:
                if ep.endswith("ttk_getEdgesFrom"):
                    self.ttkGetEF = True
        
        sourceVertexType = self.getEdgeSourceVertexType(edgeType)
        if sourceVertexType == "*":
            raise TigerGraphException("Wildcard edges are not currently supported.", None)
        
        if self.ttkGetEF:  # If installed version is available, use it, as it can return edge attributes too.
            ret = self.runInstalledQuery("ttk_getEdgesFrom", {"edgeType": edgeType, "sourceVertexType": sourceVertexType})
        else:  # If installed version is not available, use interpreted version. Always available, but can't return attributes.
            queryText = \
            'INTERPRET QUERY () FOR GRAPH $graph { \
                SetAccum<EDGE> @@edges; \
                start = {ANY}; \
                res = \
                    SELECT s \
                    FROM   start:s-(:e)->ANY:t \
                    WHERE  e.type == "$edgeType" \
                       AND s.type == "$sourceEdgeType" \
                    ACCUM  @@edges += e; \
                PRINT @@edges AS edges; \
            }'
            
            queryText = queryText.replace("$graph",          self.graphname) \
                                 .replace('$sourceEdgeType', sourceVertexType) \
                                 .replace('$edgeType',       edgeType)
            ret = self.runInterpretedQuery(queryText)
        return ret[0]["edges"]

    def getEdgeStats(self, edgeTypes, skipNA=False):
        """Returns edge attribute statistics.

        Arguments:
        - `edgeTypes`: A single edge type name or a list of edges types names or '*' for all edges types
        - `skipNA`:    Skip those edges that do not have attributes or none of their attributes have statistics gathered

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
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data, resKey=None, skipCheck=True)
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

        Returns a dictionary of <edge_type>: <deleted_edge_count> pairs.

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
        if limit and sort:  # These two must be provided together
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
        return self._get(self.restppUrl + "/query/" + self.graphname + "/" + queryName, params=params, headers={"RESPONSE-LIMIT": str(sizeLimit), "GSQL-TIMEOUT": str(timeout)})

    def runInterpretedQuery(self, queryText, params=None):
        """Runs an interpreted query.

        You must provide the query text in this format:
            INTERPRET QUERY (<params>) FOR GRAPH <graph_name> {
               <statements>
            }'

        Use `$graphname` in the `FOR GRAPH` clause to avoid hard-coding it; it will be replaced by the actual graph name.

        Arguments:
        - `params`:    A string of param1=value1&param2=value2 format or a dictionary.

        Endpoint:      POST /gsqlserver/interpreted_query
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-gsqlserver-interpreted_query-run-an-interpreted-query
        """
        queryText = queryText.replace("$graphname", self.graphname)
        if self.debug:
            print(queryText)
        return self._post(self.gsUrl + "/gsqlserver/interpreted_query", data=queryText, params=params, authMode="pwd")

    # Token management =========================================================

    def getToken(self, secret, setToken=True, lifetime=None):
        """Requests an authorization token.

        This function returns a token only if REST++ authentication is enabled. If not, an exception will be raised.
        See: https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication

        Arguments:
        - `secret`:   The secret (string) generated in GSQL using `CREATE SECRET`.
                      See https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#create-show-drop-secret
        - `setToken`: Set the connection's API token to the new value (default: true).
        - `lifetime`: Duration of token validity (in secs, default 30 days = 2,592,000 secs).

        Returns a tuple of (<new_token>, <exporation_timestamp_unixtime>, <expiration_timestamp_ISO8601>).
                 Return value can be ignored.

        Note: expiration timestamp's time zone might be different from your computer's local time zone.

        Endpoint:      GET /requesttoken
        Documentation: https://docs.tigergraph.com/dev/restpp-api/restpp-requests#requesting-a-token-with-get-requesttoken
        """
        res = json.loads(requests.request("GET", self.restppUrl + "/requesttoken?secret=" + secret + ("&lifetime=" + str(lifetime) if lifetime else "")).text)
        if not res["error"]:
            if setToken:
                self.apiToken   = res["token"]
                self.authHeader = {'Authorization': "Bearer " + self.apiToken}
            return res["token"], res["expiration"], datetime.utcfromtimestamp(res["expiration"]).strftime('%Y-%m-%d %H:%M:%S')
        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't generate token.", None)
        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def refreshToken(self, secret, token=None, lifetime=2592000):
        """Extends a token's lifetime.

        This function works only if REST++ authentication is enabled. If not, an exception will be raised.
        See: https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication

        Arguments:
        - `secret`:   The secret (string) generated in GSQL using `CREATE SECRET`.
                      See https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#create-show-drop-secret
        - `token`:    The token requested earlier. If not specified, refreshes current connection's token.
        - `lifetime`: Duration of token validity (in secs, default 30 days = 2,592,000 secs) from current system timestamp.

        Returns a tuple of (<token>, <exporation_timestamp_unixtime>, <expiration_timestamp_ISO8601>).
                 Return value can be ignored.
                 Raises exception if specified token does not exists.

        Note:
        - New expiration timestamp will be now + lifetime seconds, _not_ current expiration timestamp + lifetime seconds.
        - Expiration timestamp's time zone might be different from your computer's local time zone.

        Endpoint:      PUT /requesttoken
        Documentation: https://docs.tigergraph.com/dev/restpp-api/restpp-requests#refreshing-tokens
        """
        if not token:
            token = self.apiToken
        res = json.loads(requests.request("PUT", self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token + ("&lifetime=" + str(lifetime) if lifetime else "")).text)
        if not res["error"]:
            exp = time.time() + res["expiration"]
            return res["token"], int(exp), datetime.utcfromtimestamp(exp).strftime('%Y-%m-%d %H:%M:%S')
        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't refresh token.", None)
        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def deleteToken(self, secret, token=None, skipNA=True):
        """Deletes a token.

        This function works only if REST++ authentication is enabled. If not, an exception will be raised.
        See: https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication

        Arguments:
        - `secret`:   The secret (string) generated in GSQL using `CREATE SECRET`.
                      See https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#create-show-drop-secret
        - `token`:    The token requested earlier. If not specified, deletes current connection's token, so be careful.
        - `skipNA`:   Don't raise exception if specified token does not exist.

        Returns `True` if deletion was successful or token did not exist but `skipNA` was `True`; raises exception otherwise.

        Endpoint:      DELETE /requesttoken
        Documentation: https://docs.tigergraph.com/dev/restpp-api/restpp-requests#deleting-tokens
        """
        if not token:
            token = self.apiToken
        res = json.loads(requests.request("DELETE", self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token).text)
        if not res["error"]:
            return True
        if res["code"] == "REST-3300" and skipNA:
            return True
        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't delete token.", None)
        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    # Other functions ==========================================================

    def echo(self):
        """Pings the database.

        Expected return value is "Hello GSQL"

        Endpoint:      GET /echo  and  POST /echo
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-echo-and-post-echo
        """
        return self._get(self.restppUrl + "/echo/" + self.graphname, resKey="message")

    def getEndpoints(self, builtin=False, dynamic=False, static=False):
        """Lists the REST++ endpoints and their parameters.

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
            eps = {}
            res = self._get(url + "dynamic=true", resKey=None)
            for ep in res:
                if re.search("^GET /query/" + self.graphname, ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if sta:
            ret.update(self._get(url + "static=true", resKey=None))
        return ret

    def getStatistics(self, seconds=10, segment=10):
        """Retrieves real-time query performance statistics over the given time period.

        Arguments:
        - `seconds`:  The duration of statistic collection period (the last n seconds before the function call).
        - `segments`: The number of segments of the latency distribution (shown in results as LatencyPercentile).
                      By default, segments is 10, meaning the percentile range 0-100% will be divided into ten equal segments: 0%-10%, 11%-20%, etc.
                      Segments must be [1, 100].

        Endpoint:      GET /statistics
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-statistics
        """
        if not seconds or type(seconds) != "int":
            seconds = 10
        else:
            seconds = max(min(seconds, 0), 60)
        if not segment or type(segment) != "int":
            segment = 10
        else:
            segment = max(min(segment,0),100)
        return self._get(self.restppUrl + "/statistics/" + self.graphname + "?seconds=" + str(seconds) + "&segment=" + str(segment), resKey=None)

    def getVersion(self, raw=False):
        """Retrieves the git versions of all components of the system.

        Endpoint:      GET /version
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-version
        """
        response = requests.request("GET", self.restppUrl + "/version/" + self.graphname, headers=self.authHeader)
        if raw:
            return response.text
        res = json.loads(response.text, strict=False)["message"].split("\n")  # "strict=False" is why _get() was not used
        components = []
        for i in range(len(res)):
            if 2 < i < len(res) - 1:
                m = res[i].split()
                component = {"name": m[0], "version": m[1], "hash": m[2], "datetime": m[3] + " " + m[4] + " " + m[5]}
                components.append(component)
        return components

    def getVer(self, component="product", full=False):
        """Gets the version information of specific component

        Arguments:
        - `component`: One of TigerGraph's components (e.g. product, gpe, gse).

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

    def getLicenseInfo(self):
        """Returns the expiration date and remaining days of the license.

        In case of evaluation/trial deployment, an information message and -1 remaining days are returned.
        """
        res = self._get(self.restppUrl + "/showlicenseinfo", resKey=None, skipCheck=True)
        ret = {}
        if not res["error"]:
            ret["message"]        = res["message"]
            ret["expirationDate"] = res["results"][0]["Expiration date"]
            ret["daysRemaining"]  = res["results"][0]["Days remaining"]
        elif "code" in res and res["code"] == "REST-5000":
            ret["message"]        = "This instance does not have a valid enterprise license. Is this a trial version?"
            ret["daysRemaining"]  = -1
        else:
            raise TigerGraphException(res["message"], res["code]"])
        return ret


    def getVertexDataframe(self, vertexType, select="", where="", limit="", sort="", timeout=0):
        """Retrieves vertices of the given vertex type.

        Arguments:
        - `vertexType`: Type of vertex desired
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
        data = self.getVertices(vertexType, select=select, where=where, limit=limit, sort=sort, timeout=timeout)
        df = pd.DataFrame(data)
        df = pd.concat([df["v_id"], pd.DataFrame(df["attributes"].tolist())], axis=1)
        return df

    def getVertexDataframeByID(self, vertexType, vertexIds):
        """Retrieves vertices of the given vertex type, identified by their ID.

        Arguments
        - `vertexType`: Type of vertex desired
        - `vertexIds`: A single vertex ID or a list of vertex IDs.

        Endpoint:      GET /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices
        """
        data = self.getVerticesById(vertexType, vertexIds)
        df = pd.DataFrame(data)
        df = pd.concat([df["v_id"], pd.DataFrame(df["attributes"].tolist())], axis=1)
        return df

    def getEdgesDataframe(self, sourceVertexType, sourceVerticies, edgeType=None, targetVertexType=None, targetVertexId=None, select="", where="", limit="", sort="", timeout=0):
        """Retrieves edges of the given edge type originating from the list of source verticies.

        Only `sourceVertexType` and `sourceVerticies` are required.
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
        """
        frames = []
        for vertex in sourceVerticies:
            data = self.getEdges(sourceVertexType, vertex, edgeType, targetVertexType, targetVertexId, select, where, limit, sort, timeout)
            df = pd.DataFrame(data)
            try:
                frames.append(pd.concat([
                    df.drop(["from_type","attributes"], axis=1),
                    pd.DataFrame(df["attributes"].tolist())
                ], axis=1))
            except:
                frames.append(df)
        return pd.concat(frames).reset_index().drop("index", axis=1)

    def getInstalledQueriesDataframe(self):
        """
        Returns dataframe of all installed queries. Does not take any arguments
        """
        data = self.getEndpoints(dynamic=True)
        df = pd.DataFrame(data).T
        return df
    

    def upsertVertexDataframe(self, df, vertexType, v_id=None, attributes=None):
        """Upserts vertices from a Pandas data frame. 

        Arguments:
        - `df`:          The data frame to upsert.
        - `vertexType`:  The type of vertex to upsert data to.
        - `v_id`:        The field name where the vertex primary id is given. If omitted the dataframe 
                         index would be used instead.
        - `attributes`:  A dictionary in the form of {target: source} where source is the column name 
                         in the dataframe and target is the attribute name in the graph vertex. When omitted
                         all columns would be upserted with their current names. In this case column names 
                         must match the vertex's attribute names.
        """
    
        json_up = []
    
        for index in df.index:
        
            json_up.append(json.loads(df.loc[index].to_json()))
            json_up[-1] = (
                index if v_id == None else json_up[-1][v_id],  
                json_up[-1] if attributes == None 
                else {target: json_up[-1][source] 
                      for target, source in attributes.items()}
            )
        
        return self.upsertVertices(vertexType=vertexType, vertices=json_up)

    
    def upsertEdgesDataframe(
        self, df, sourceVertexType, edgeType, targetVertexType, from_id=None, to_id=None, 
        attributes=None):
        """Upserts edges from a Pandas dataframe. 

        Arguments:
        - `df`:                The dataframe to upsert.
        - `sourceVertexType`:  The type of source vertex for the edge.
        - `edgeType`:          The type of edge to upsert data to.
        - `targetVertexType`:  The type of target vertex for the edge.
        - `from_id`:     The field name where the source vertex primary id is given. If omitted the 
                         dataframe index would be used instead. 
        - `to_id`:       The field name where the target vertex primary id is given. If omitted the 
                         dataframe index would be used instead. 
        - `attributes`:  A dictionary in the form of {target: source} where source is the column name 
                         in the dataframe and target is the attribute name in the graph vertex. When omitted
                         all columns would be upserted with their current names. In this case column names 
                         must match the vertex's attribute names.
        """
    
        json_up = []
    
        for index in df.index:
        
            json_up.append(json.loads(df.loc[index].to_json()))
            json_up[-1] = (
                index if from_id == None else json_up[-1][from_id],  
                index if to_id == None else json_up[-1][to_id],  
                json_up[-1] if attributes == None 
                else {target: json_up[-1][source] 
                      for target, source in attributes.items()}
            )
        
        return self.upsertEdges(
            sourceVertexType=sourceVertexType, 
            edgeType=edgeType,
            targetVertexType=targetVertexType,
            edges=json_up
        )


    
# EOF
