import requests
import json
import re
from datetime import datetime
import time
import pandas as pd
import os
# Added pyTigerDriver Client
from pyTigerDriver import GSQL_Client
import urllib3
import sys
import urllib
from urllib.parse import urlparse
import base64

urllib3.disable_warnings()


def excepthook(type, value, traceback):
    print(value)


class TigerGraphException(Exception):
    """Generic TigerGraph specific exception.

    Where possible, error message and code returned by TigerGraph will be used.
    """

    def __init__(self, message, code=None):
        self.message = message
        self.code = code


class TigerGraphConnection(object):
    """Python wrapper for TigerGraph's REST++ and GSQL APIs

    Common arguments used in methods:
    vertexType, sourceVertexType, targetVertexType
    -- The name of a vertex type in the graph.

    Use `getVertexTypes()` to fetch the list of vertex types currently in the graph.
    vertexId, sourceVertexId, targetVertexId
    -- The PRIMARY_ID of a vertex instance (of the appropriate data type).
    edgeType
    -- The name of the edge type in the graph.
    Use `getEdgeTypes()` to fetch the list of edge types currently in the graph.
    """

    def __init__(self, host="http://127.0.0.1", graphname="MyGraph", username="tigergraph", password="tigergraph",
                 restppPort="9000", gsPort="14240", gsqlVersion="", version="", apiToken="", useCert=True,
                 certPath=None,
                 debug=False, sslPort="443",gcp=False):
        """Initiate a connection object.

        Arguments

        - `host`:              The ip address of the TigerGraph server.
        - `graphname`:         The default graph for running queries.
        - `username`:          The username on the TigerGraph server.
        - `password`:          The password for that user.
        - `restppPort`:        The post for REST++ queries.
        - `gsPort`:            The port of all other queries.
        - `apiToken`:          A token to use when making queries.
        - `useCert`:           True if we need to use a certificate because the server is secure (such as on TigerGraph
                               Cloud). This needs to be False when connecting to an unsecure server such as TigerGraph
                               Developer.
                               When True the certificate would be downloaded when it is first needed.
                               on the first GSQL command.
        - `sslPort`:           Configurable port for ssl cert fetching in case of firewall on 443. Will use 14240.
        """
        inputHost = urlparse(host)
        # if inputHost.port not in [gsPort,restppPort,"80","443"] :
        #     raise("E-0002: Please verify the Ports configration ")
        if inputHost.scheme not in ["http", "https"]:
            raise ("E-0003: Please verify the host (http/https) schema")
        self.netloc = inputHost.netloc
        self.host = "{0}://{1}".format(inputHost.scheme, self.netloc)
        self.username = username
        self.password = password
        self.graphname = graphname
        self.beta = gcp
        if self.beta == True and (restppPort == "9000" or restppPort == "443"):
            self.restppPort = "443"
            self.restppUrl = self.host + ":443" + "/restpp"
        else:
            self.restppPort = str(restppPort)
            self.restppUrl = self.host + ":" + self.restppPort
        self.gsPort = ""
        if self.beta == True and (gsPort == "14240" or gsPort == "443"):
            self.gsPort = "443"

            self.gsUrl = self.host + ":443"
        else:
            self.gsPort = str(gsPort)
            self.gsUrl = self.host + ":" + self.gsPort

        self.debug = debug

        self.apiToken = apiToken
        if gsqlVersion != "":
            self.version = gsqlVersion
        elif version != "":
            self.version = version
        else:
            self.version = ""
        self.base64_credential = base64.b64encode(
            "{0}:{1}".format(self.username, self.password).encode("utf-8")).decode("utf-8")
        if self.apiToken:
            self.authHeader = {'Authorization': "Bearer " + self.apiToken}
        else:
            self.authHeader = {'Authorization': 'Basic {0}'.format(self.base64_credential)}

        if not self.debug:
            sys.excepthook = excepthook
            sys.tracebacklimit = None
        self.schema = None
        self.ttkGetEF = None  # TODO: this needs to be rethought, or at least renamed
        self.downloadCert = useCert
        if inputHost.scheme == "http":
            self.downloadCert = False
            self.useCert = False
            self.certPath = ''
        elif inputHost.scheme == "https":
            self.downloadCert = True
            self.useCert = True
            self.certPath = certPath
        self.downloadJar = False
        self.sslPort = sslPort

        self.gsqlInitiated = False

        self.Client = None

    # Private functions ========================================================
    def safeChar(self,inputString):
        return urllib.parse.quote(str(inputString), safe='')
    def _errorCheck(self, res):
        """
        Checks if the JSON document returned by an endpoint has contains error:
        true; if so, it raises an exception.
        """
        if "error" in res and res["error"] and res["error"] != "false":
            # Endpoint might return string "false" rather than Boolean false
            raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def _req(self, method, url, authMode="token", headers=None, data=None, resKey="results",
             skipCheck=False, params=None):
        """Generic REST++ API request

        Arguments:
        - `method`:    HTTP method, currently one of GET, POST or DELETE.
        - `url`:       Complete REST++ API URL including path and parameters.
        - `authMode`:  Authentication mode, one of 'token' (default) or 'pwd'.
        - `headers`:   Standard HTTP request headers (dict).
        - `data`:      Request payload, typically a JSON document.
        - `resKey`:    the JSON subdocument to be returned, default is 'result'.
        - `skipCheck`: Skip error checking? Some endpoints return error
        to indicate that the requested action is not applicable;
        a problem, but not really an error.
        - `params`:    Request URL parameters.
        """

        if authMode == "token" and str(self.apiToken) != "" :

            if type(self.apiToken) == type(()):
                self.apiToken = self.apiToken[0]
            self.authHeader = {'Authorization': "Bearer " + self.apiToken}
            _headers = self.authHeader
        else:
            self.authHeader = {'Authorization': 'Basic {0}'.format(self.base64_credential)}
            _headers = self.authHeader
            authMode = 'pwd'

        if authMode == "pwd":
            _auth = (self.username, self.password)
        else:
            _auth = None
        if headers:
            _headers.update(headers)
        # if authMode == 'pwd':
        #     print(headers)
            # if "token" in headers:
            #     del headers["token"]
        # print(_headers)
        if method == "POST":
            _data = data
        else:
            _data = None

        if self.useCert is True or self.certPath is not None:
            res = requests.request(method, url,  headers=_headers, data=_data, params=params, verify=False) # auth=_auth,
        else:
            res = requests.request(method, url, headers=_headers, data=_data, params=params) # , auth=_auth


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
        """Generic GET method.

        For argument details, see `_req`.
        """
        res = self._req("GET", url, authMode, headers, None, resKey, skipCheck, params)
        return res

    def _post(self, url, authMode="token", headers=None, data=None, resKey="results", skipCheck=False, params=None):
        """Generic POST method.

        For argument details, see `_req`.
        """
        return self._req("POST", url, authMode, headers, data, resKey, skipCheck, params)

    def _delete(self, url, authMode="token"):
        """Generic DELETE method.

        For argument details, see `_req`.
        """
        return self._req("DELETE", url, authMode)

    def _upsertAttrs(self, attributes):
        """Transforms attributes (provided as a table) into a hierarchy as expect by the upsert functions."""
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
        """Retrieves the schema (all vertex and edge type and - if not disabled - the User Defined Type details)
        of the graph.

        Arguments:
        - `udts`: If `True`, calls `_getUDTs()`, i.e. includes User Defined Types in the schema details.
        - `force`: If `True`, retrieves the schema details again, otherwise returns a cached copy of the schema
         details (if they were already fetched previously).

        Endpoint:      GET /gsqlserver/gsql/schema
        Documentation:
        https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-the-graph-schema-get-gsql-schema
        """
        if not self.schema or force:
            self.schema = self._get(self.gsUrl + "/gsqlserver/gsql/schema?graph=" + self.graphname, authMode="pwd")
        if udts and ("UDTs" not in self.schema or force):
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
        """
        Upserts data (vertices and edges) from a JSON document or equivalent object structure.

        Endpoint:      POST /graph
        Documentation:
        https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-the-graph-schema-get-gsql-schema
        """
        if not isinstance(data, str):
            data = json.dumps(data)
        return self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0]

    # Vertex related functions =================================================

    def getVertexTypes(self, force=False):
        """
        Returns the list of vertex type names of the graph.

        Arguments:
        - `force`: If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of
        vertex type details (if they were already fetched previously).
        """
        ret = []
        for vt in self.getSchema(force=force)["VertexTypes"]:
            ret.append(vt["Name"])
        return ret

    def getVertexType(self, vertexType, force=False):
        """
        Returns the details of the specified vertex type.

        Arguments:
        - `vertexType`: The name of of the vertex type.
        - `force`: If `True`, forces the retrieval the schema details again, otherwise returns a cached copy of vertex
        type details (if they were already fetched previously).
        """
        for vt in self.getSchema(force=force)["VertexTypes"]:
            if vt["Name"] == vertexType:
                return vt
        return {}  # Vertex type was not found

    def getVertexCount(self, vertexType, where=""):
        """
    Returns the number of vertices.

    Uses:
    - If `vertexType` = "*": vertex count of all vertex types (`where` cannot be specified in this case)
    - If `vertexType` is specified only: vertex count of the given type
    - If `vertexType` and `where` are specified:vertex count of the given type after filtered by `where` condition(s)

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
            res = self._get(self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType + "?count_only"
                                                                                                      "=true&filter="
                            + where)
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
        - If it's already in the graph, its attributes are updated with the values specified in the request.
        An optional operator controls how the attributes are updated.

        The `attributes` argument is expected to be a dictionary in this format:
            {<attribute_name>: <attribute_value>|(<attribute_name>, <operator>), …}

        Example:
            {"name": "Thorin", points: (10, "+"), "bestScore": (67, "max")}

        For valid values of <operator> see:
        https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data

        Returns a single number of accepted (successfully upserted) vertices (0 or 1).

        Endpoint:      POST /graph
        Documentation:
        https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data
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

        For valid values of <operator> see:
        https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data

        Returns a single number of accepted (successfully upserted) vertices (0 or positive integer).

        Endpoint:      POST /graph
        Documentation:
        https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-graph-graph_name-upsert-the-given-data
        """
        if not isinstance(vertices, list):
            return None
        data = {}
        for v in vertices:
            vals = self._upsertAttrs(v[1])
            data[v[0]] = vals
        data = json.dumps({"vertices": {vertexType: data}})
        return self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0]["accepted_vertices"]

    def getVertices(self, vertexType, select="", where="", limit="", sort="", fmt="py",
                    withId=True, withType=False, timeout=0):
        """Retrieves vertices of the given vertex type.

        Arguments:
        - `select`:   Comma separated list of vertex attributes to be retrieved or omitted.
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select
        - `where`:    Comma separated list of conditions that are all applied on each vertex' attributes.
                      The conditions are in logical conjunction (i.e. they are "AND'ed" together).
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter
        - `limit`:    Maximum number of vertex instances to be returned (after sorting).
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit
        - `sort`      Comma separated list of attributes the results should be sorted by.
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort
        - `fmt`:      Format of the results:
                      "py":   Python objects
                      "json": JSON document
                      "df":   Pandas DataFrame
        - `withId`:   (If the output format is "df") should the vertex ID be included in the dataframe?
        - `withType`: (If the output format is "df") should the vertex type be included in the dataframe?
        - `timeout`:  Time allowed for successful execution (0 = no limit, default).

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

        ret = self._get(url)

        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return self.vertexSetToDataFrame(ret, withId, withType)
        return ret

    def getVertexDataframe(self, vertexType, select="", where="", limit="", sort="", timeout=0):
        """Retrieves vertices of the given vertex type and returns them as Pandas DataFrame.

        For details on arguments see `getVertices` above.
        """
        return self.getVertices(vertexType, select=select, where=where, limit=limit, sort=sort, fmt="df",
                                withId=True, withType=False, timeout=timeout)


    def getVerticesById(self, vertexType, vertexIds, fmt="py", withId=True, withType=False):
        """Retrieves vertices of the given vertex type, identified by their ID.

        Arguments
        - `vertexIds`: A single vertex ID or a list of vertex IDs.
        - `fmt`:      Format of the results:
                      "py":   Python objects
                      "json": JSON document
                      "df":   Pandas DataFrame
        - `withId`:   (If the output format is "df") should the vertex ID be included in the dataframe?
        - `withType`: (If the output format is "df") should the vertex type be included in the dataframe?
        - `timeout`:  Time allowed for successful execution (0 = no limit, default).

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
            ret += self._get(url + self.safeChar(vid))

        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return self.vertexSetToDataFrame(ret, withId, withType)
        return ret

    def getVertexDataframeById(self, vertexType, vertexIds):
        """Retrieves vertices of the given vertex type, identified by their ID.

        For details on arguments see `getVerticesById` above.
        """
        return self.getVerticesById(vertexType, vertexIds, fmt="df", withId=True, withType=False)

    def getVertexStats(self, vertexTypes, skipNA=False):
        """Returns vertex attribute statistics.

        Arguments:
        - `vertexTypes`: A single vertex type name or a list of vertex types names or '*' for all vertex types.
        - `skipNA`:      Skip those non-applicable vertices that do not have attributes or none of their attributes
        have statistics gathered.

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
        - `sort`:      Comma separated list of attributes the results should be sorted by.
                       See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort
                       Must be used with `limit`.
        - `permanent`: If true, the deleted vertex IDs can never be inserted back, unless the graph is dropped or the
        graph store is cleared.
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
        - `permanent`: If true, the deleted vertex IDs can never be inserted back, unless the graph is dropped or
        the graph store is cleared.
        - `timeout`:   Time allowed for successful execution (0 = no limit, default).

        Returns a single number of vertices deleted.

        Endpoint:      DELETE /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-graph-graph_name-vertices
        """
        if not vertexIds:
            raise TigerGraphException("No vertex ID was not specified.", None)
        vids = []
        if isinstance(vertexIds, (int, str)):
            vids.append(self.safeChar(vertexIds))
        elif not isinstance(vertexIds, list):
            return None  # TODO: a better return value?
        else:
            vids = [ self.safeChar(f) for f in vertexIds]
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
        """Returns the type(s) of the edge type's source vertex.

        Arguments:
        - `edgeType`: The name of the edge type.

        Returns:
        - A single source vertex type name string if the edge has a single source vertex type.
        - "*" if the edge can originate from any vertex type (notation used in 2.6.1 and earlier versions).
            See https://docs.tigergraph.com/v/2.6/dev/gsql-ref/ddl-and-loading/defining-a-graph-schema#creating-an-edge-from-or-to-any-vertex-type
        - A set of vertex type name strings (unique values) if the edge has multiple source vertex types (notation used in 3.0 and later versions).
            Note: Even if the source vertex types were defined as "*", the REST API will list them as pairs (i.e. not as "*" in 2.6.1 and earlier versions),
                  just like as if there were defined one by one (e.g. `FROM v1, TO v2 | FROM v3, TO v4 | …`).
            Note: The returned set contains all source vertex types, but does not certainly mean that the edge is defined between all source and all target
                  vertex types. You need to look at the individual source/target pairs to find out which combinations are valid/defined.
        """
        edgeTypeDetails = self.getEdgeType(edgeType)

        # Edge type with a single source vertex type
        if edgeTypeDetails["FromVertexTypeName"] != "*":
            return edgeTypeDetails["FromVertexTypeName"]

        # Edge type with multiple source vertex types
        if "EdgePairs" in edgeTypeDetails:
            # v3.0 and later notation
            vts = set()
            for ep in edgeTypeDetails["EdgePairs"]:
                vts.add(ep["From"])
            return vts
        else:
            # 2.6.1 and earlier notation
            return "*"

    def getEdgeTargetVertexType(self, edgeType):
        """Returns the type(s) of the edge type's target vertex.

        Arguments:
        - `edgeType`: The name of the edge type.

        Returns:
        - A single target vertex type name string if the edge has a single target vertex type.
        - "*" if the edge can end in any vertex type (notation used in 2.6.1 and earlier versions).
            See https://docs.tigergraph.com/v/2.6/dev/gsql-ref/ddl-and-loading/defining-a-graph-schema#creating-an-edge-from-or-to-any-vertex-type
        - A set of vertex type name strings (unique values) if the edge has multiple target vertex types (notation used in 3.0 and later versions).
            Note: Even if the target vertex types were defined as "*", the REST API will list them as pairs (i.e. not as "*" in 2.6.1 and earlier versions),
                  just like as if there were defined one by one (e.g. `FROM v1, TO v2 | FROM v3, TO v4 | …`).
            Note: The returned set contains all target vertex types, but does not certainly mean that the edge is defined between all source and all target
                  vertex types. You need to look at the individual source/target pairs to find out which combinations are valid/defined..
        """
        edgeTypeDetails = self.getEdgeType(edgeType)

        # Edge type with a single target vertex type
        if edgeTypeDetails["ToVertexTypeName"] != "*":
            return edgeTypeDetails["ToVertexTypeName"]

        # Edge type with multiple target vertex types
        if "EdgePairs" in edgeTypeDetails:
            # v3.0 and later notation
            vts = set()
            for ep in edgeTypeDetails["EdgePairs"]:
                vts.add(ep["To"])
            return vts
        else:
            # 2.6.1 and earlier notation
            return "*"

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

    def getEdgeCountFrom(self, sourceVertexType=None, sourceVertexId=None, edgeType=None, targetVertexType=None,
                         targetVertexId=None, where=""):
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
                raise TigerGraphException(
                    "If where condition is specified, then both sourceVertexType and sourceVertexId must be provided too.",
                    None)
            url = self.restppUrl + "/graph/" + self.safeChar(self.graphname) + "/edges/" + self.safeChar(sourceVertexType) + "/" + self.safeChar(sourceVertexId)
            if edgeType:
                url += "/" + self.safeChar(edgeType)
                if targetVertexType:
                    url += "/" + self.safeChar(targetVertexType)
                    if targetVertexId:
                        url += "/" + self.safeChar(targetVertexId)
            url += "?count_only=true"
            if where:
                url += "&filter=" + self.safeChar(where)
            res = self._get(url)
        else:
            if not edgeType:  # TODO is this a valid check?
                raise TigerGraphException("A valid edge type or \"*\" must be specified for edge type.", None)
            data = '{"function":"stat_edge_number","type":"' + edgeType + '"' \
                   + (',"from_type":"' + sourceVertexType + '"' if sourceVertexType else '') \
                   + (',"to_type":"' + targetVertexType + '"' if targetVertexType else '') \
                   + '}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data)
        if len(res) == 1 and res[0]["e_type"] == edgeType:
            return res[0]["count"]
        ret = {}
        for r in res:
            ret[r["e_type"]] = r["count"]
        return ret

    def getEdgeCount(self, edgeType="*", sourceVertexType=None, targetVertexType=None):
        """Returns the number of edges of an edge type.

        This is a simplified version of `getEdgeCountFrom`, to be used when the total number of edges of a given type is needed, regardless which vertex instance they are originated from.
        See documentation of `getEdgeCountFrom` above for more details.
        """
        return self.getEdgeCountFrom(edgeType=edgeType, sourceVertexType=sourceVertexType,
                                     targetVertexType=targetVertexType)

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
        data = json.dumps(
            {"edges": {sourceVertexType: {sourceVertexId: {edgeType: {targetVertexType: {targetVertexId: vals}}}}}})
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
            if len(e) > 2:
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

    def getEdges(self, sourceVertexType, sourceVertexId, edgeType=None, targetVertexType=None, targetVertexId=None,
                 select="", where="", limit="", sort="", fmt="py", withId=True, withType=False, timeout=0):
        """Retrieves edges of the given edge type originating from a specific source vertex.

        Only `sourceVertexType` and `sourceVertexId` are required.
        If `targetVertexId` is specified, then `targetVertexType` must also be specified.
        If `targetVertexType` is specified, then `edgeType` must also be specified.

        Arguments:
        - `select`:   Comma separated list of edge attributes to be retrieved or omitted.
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#select
        - `where`:    Comma separated list of conditions that are all applied on each edge's attributes.
                      The conditions are in logical conjunction (i.e. they are "AND'ed" together).
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#filter
        - `limit`:    Maximum number of edge instances to be returned (after sorting).
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#limit
        - `sort`:     Comma separated list of attributes the results should be sorted by.
                      See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#sort
        - `fmt`:      Format of the results:
                      "py":   Python objects
                      "json": JSON document
                      "df":   Pandas DataFrame
        - `withId`:   (If the output format is "df") should the source and target vertex types and IDs be included in the dataframe?
        - `withType`: (If the output format is "df") should the edge type be included in the dataframe?
        - `timeout`:  Time allowed for successful execution (0 = no limit, default).

        Endpoint:      GET /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-edges
        """
        # TODO: change sourceVertexId to sourceVertexIds and allow passing both number and list as parameter
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
        ret = self._get(url)

        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return self.edgeSetToDataFrame(ret, withId, withType)
        return ret

    def getEdgesDataframe(self, sourceVertexType, sourceVertexId, edgeType=None, targetVertexType=None,
                          targetVertexId=None, select="", where="", limit="", sort="", timeout=0):
        """Retrieves edges of the given edge type originating from a specific source vertex.

        For details on arguments see `getEdges` above.
        """
        if type(sourceVertexId) == type([]):
            raise TigerGraphException("List is not yet supported")
        else:
            return self.getEdges(sourceVertexType, sourceVertexId, edgeType, targetVertexType, targetVertexId, select,
                             where, limit, sort, fmt="df", timeout=timeout)

    def getEdgesByType(self, edgeType, fmt="py", withId=True, withType=False):
        """Retrieves edges of the given edge type regardless the source vertex.

        Arguments:
        - `edgeType`: The name of the edge type.
        - `fmt`:      Format of the results:
                      "py":   Python objects
                      "json": JSON document
                      "df":   Pandas DataFrame
        - `withId`:   (If the output format is "df") should the source and target vertex types and IDs be included in the dataframe?
        - `withType`: (If the output format is "df") should the edge type be included in the dataframe?
        - `timeout`:  Time allowed for successful execution (0 = no limit, default).

        TODO: add limit parameter
        """
        if not edgeType:
            return []

        # Check if ttk_getEdgesFrom query was installed
        if self.ttkGetEF is None:
            self.ttkGetEF = False
            eps = self.getEndpoints(dynamic=True)
            for ep in eps:
                if ep.endswith("ttk_getEdgesFrom"):
                    self.ttkGetEF = True

        sourceVertexType = self.getEdgeSourceVertexType(edgeType)
        if isinstance(sourceVertexType,
                      set) or sourceVertexType == "*":  # TODO: support edges with multiple source vertex types
            raise TigerGraphException("Edges with multiple source vertex types are not currently supported.", None)

        if self.ttkGetEF:  # If installed version is available, use it, as it can return edge attributes too.
            ret = self.runInstalledQuery("ttk_getEdgesFrom",
                                         {"edgeType": edgeType, "sourceVertexType": sourceVertexType})
        else:  # If installed version is not available, use interpreted version. Always available, but couldn't return attributes before v3.0.
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

            queryText = queryText.replace("$graph", self.graphname) \
                .replace('$sourceEdgeType', sourceVertexType) \
                .replace('$edgeType', edgeType)
            ret = self.runInterpretedQuery(queryText)
        ret = ret[0]["edges"]

        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return self.edgeSetToDataFrame(ret, withId, withType)
        return ret

    # TODO: getEdgesDataframeByType

    def getEdgeStats(self, edgeTypes, skipNA=False):
        """Returns edge attribute statistics.

        Arguments:
        - `edgeTypes`: A single edge type name or a list of edges types names or '*' for all edges types.
        - `skipNA`:    Skip those edges that do not have attributes or none of their attributes have statistics gathered.

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

    def delEdges(self, sourceVertexType, sourceVertexId, edgeType=None, targetVertexType=None, targetVertexId=None,
                 where="", limit="", sort="", timeout=0):
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


    def getInstalledQueries(self, fmt="py"):
        """
        Returns a list of installed queries.
        
        Arguments:
        - `fmt`:      Format of the results:
                      "py":   Python objects (default)
                      "json": JSON document
                      "df":   Pandas DataFrame
        """
        ret = self.getEndpoints(dynamic=True)
        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return pd.DataFrame(ret).T
        return ret

    def runInstalledQuery(self, queryName, params=None, timeout=None, sizeLimit=None, usePost=False):

        """Runs an installed query.

        The query must be already created and installed in the graph.
        Use `getEndpoints(dynamic=True)` or GraphStudio to find out the generated endpoint URL of the query, but only the query name needs to be specified here.

        Arguments:
        - `params`:    A string of param1=value1&param2=value2 format or a dictionary.
        - `timeout`:   Maximum duration for successful query execution.
        - `sizeLimit`: Maximum size of response (in bytes).
                       See https://docs.tigergraph.com/dev/restpp-api/restpp-requests#request-body-size
        - `usePost`:   RestPP accepts a maximum URL length of 8192 characters. Use POST if params cause you to
                       exceed this limit.
                       See https://docs.tigergraph.com/dev/gsql-ref/querying/query-operations#running-a-query-as-a-rest-endpoint

        Endpoint:      POST /query/{graph_name}/<query_name>
        Documentation: https://docs.tigergraph.com/dev/gsql-ref/querying/query-operations#running-a-query
        """
        headers = {}
        if timeout:
            headers["GSQL-TIMEOUT"] = str(timeout)
        if sizeLimit:
            headers["RESPONSE-LIMIT"] = str(sizeLimit)
        if isinstance(params, dict):
            params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote,safe='')

        if usePost:
            return self._post(self.restppUrl + "/query/" + self.graphname + "/" + queryName, data=params, headers=headers)

        else:
            return self._get(self.restppUrl + "/query/" + self.graphname + "/" + queryName, params=params,
                             headers={"RESPONSE-LIMIT": str(sizeLimit), "GSQL-TIMEOUT": str(timeout)})

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
        return self._post(self.gsUrl + "/gsqlserver/interpreted_query", data=queryText, params=params, authMode="pwd")

    # Pandas DataFrame support =================================================

    def vertexSetToDataFrame(self, vertexSet, withId=True, withType=False):
        """
Converts a vertex set to Pandas DataFrame.

Vertex sets are used for both the input and output of `SELECT` statements. They contain instances of vertices
of the same type.
For each vertex instance the vertex ID, the vertex type and the (optional) attributes are present
(under `v_id`, `v_type` and `attributes` keys, respectively).
See example in `edgeSetToDataFrame`.

A vertex set has this structure:
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

See:
https://docs.tigergraph.com/dev/gsql-ref/querying/declaration-and-assignment-statements#vertex-set-variable-declaration-and-assignment
        """
        df = pd.DataFrame(vertexSet)
        cols = []
        if withId:
            cols.append(df["v_id"])
        if withType:
            cols.append(df["v_type"])
        cols.append(pd.DataFrame(df["attributes"].tolist()))
        return pd.concat(cols, axis=1)

    def edgeSetToDataFrame(self, edgeSet, withId=True, withType=False):
        """Converts an edge set to Pandas DataFrame

        Edge sets contain instances of the same edge type. Edge sets are not generated "naturally" like vertex sets, you need to collect edges in (global) accumulators,
            e.g. in case you want to visualise them in GraphStudio or by other tools.
        Example:

            SetAccum<EDGE> @@edges;
            start = {Country.*};
            result =
                SELECT t
                FROM   start:s -(PROVINCE_IN_COUNTRY:e)- Province:t
                ACCUM  @@edges += e;
            PRINT start, result, @@edges;

        The `@@edges` is an edge set.
        It contains for each edge instance the source and target vertex type and ID, the edge type, an directedness indicator and the (optional) attributes.
        Note: `start` and `result` are vertex sets.

        An edge set has this structure:
        [
            {
                "e_type": <edge_type_name>,
                "from_type": <source_vertex_type_name>,
                "from_id": <source_vertex_id>,
                "to_type": <target_vertex_type_name>,
                "to_id": <targe_vertex_id>,
                "directed": <true_or_false>,
                "attributes":
                    {
                        "attr1": <value1>,
                        "attr2": <value2>,
                         ⋮
                    }
            },
                ⋮
        ]
        """
        df = pd.DataFrame(edgeSet)
        cols = []
        if withId:
            cols.extend([df["from_type"], df["from_id"], df["to_type"], df["to_id"]])
        if withType:
            cols.append(df["e_type"])
        cols.append(pd.DataFrame(df["attributes"].tolist()))
        return pd.concat(cols, axis=1)

    def upsertVertexDataFrame(self, df, vertexType, v_id=None, attributes=None):
        """Upserts vertices from a Pandas DataFrame.

        Arguments:
        - `df`:          The DataFrame to upsert.
        - `vertexType`:  The type of vertex to upsert data to.
        - `v_id`:        The field name where the vertex primary id is given. If omitted the dataframe
                         index would be used instead.
        - `attributes`:  A dictionary in the form of {target: source} where source is the column name
                         in the dataframe and target is the attribute name in the graph vertex. When omitted
                         all columns would be upserted with their current names. In this case column names
                         must match the vertex's attribute names.

        Returns: The number of vertices upserted.
        """

        json_up = []

        for index in df.index:
            json_up.append(json.loads(df.loc[index].to_json()))
            json_up[-1] = (
                index if v_id is None else json_up[-1][v_id],
                json_up[-1] if attributes is None
                else {target: json_up[-1][source]
                      for target, source in attributes.items()}
            )

        return self.upsertVertices(vertexType=vertexType, vertices=json_up)

    def upsertEdgeDataFrame(
            self, df, sourceVertexType, edgeType, targetVertexType, from_id=None, to_id=None,
            attributes=None):
        """Upserts edges from a Pandas DataFrame.

        Arguments:
        - `df`:                The DataFrame to upsert.
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

        Returns: The number of edges upserted.
        """

        json_up = []

        for index in df.index:
            json_up.append(json.loads(df.loc[index].to_json()))
            json_up[-1] = (
                index if from_id is None else json_up[-1][from_id],
                index if to_id is None else json_up[-1][to_id],
                json_up[-1] if attributes is None
                else {target: json_up[-1][source]
                      for target, source in attributes.items()}
            )

        return self.upsertEdges(
            sourceVertexType=sourceVertexType,
            edgeType=edgeType,
            targetVertexType=targetVertexType,
            edges=json_up
        )

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

        if self.useCert is True and self.certPath is not None:
            res = json.loads(requests.request("GET", self.restppUrl + "/requesttoken?secret=" + secret + (
            "&lifetime=" + str(lifetime) if lifetime else "")).text)
        else:
            res = json.loads(requests.request("GET", self.restppUrl + "/requesttoken?secret=" + secret + (
                "&lifetime=" + str(lifetime) if lifetime else ""),verify=False).text)
        if not res["error"]:
            if setToken:
                self.apiToken = res["token"]
                self.authHeader = {'Authorization': "Bearer " + self.apiToken}
            else:
                self.apiToken = None
                self.authHeader = {'Authorization': 'Basic {0}'.format(self.base64_credential)}

            return res["token"], res["expiration"], datetime.utcfromtimestamp(res["expiration"]).strftime(
                '%Y-%m-%d %H:%M:%S')
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
        if self.useCert is True and self.certPath is not None:
            res = json.loads(requests.request("PUT",
                                          self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token + (
                                              "&lifetime=" + str(lifetime) if lifetime else ""),verify=False).text)
        else:
            res = json.loads(requests.request("PUT",
                                              self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token + (
                                                  "&lifetime=" + str(lifetime) if lifetime else "")).text)
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
        if self.useCert is True and self.certPath is not None:
            res = json.loads(
            requests.request("DELETE", self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token,
                             verify=False).text)
        else:
            res = json.loads(
                requests.request("DELETE", self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token).text)
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

    def getInstalledQueries(self, fmt="py"):
        """
        Returns installed queries.
        """
        ret = self.getEndpoints(dynamic=True)
        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return pd.DataFrame(ret).T
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
            segment = max(min(segment, 0), 100)
        return self._get(
            self.restppUrl + "/statistics/" + self.graphname + "?seconds=" + str(seconds) + "&segment=" + str(segment),
            resKey=None)

    def getVersion(self, raw=False):
        """Retrieves the git versions of all components of the system.

        Endpoint:      GET /version
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-version
        """
        if self.useCert is True and self.certPath is not None:
            response = requests.request("GET", self.restppUrl + "/version/" + self.graphname, headers=self.authHeader,
                                        verify=False)
        else:
            response = requests.request("GET", self.restppUrl + "/version/" + self.graphname, headers=self.authHeader)
        res = json.loads(response.text, strict=False)  # "strict=False" is why _get() was not used
        self._errorCheck(res)

        if raw:
            return response.text
        res = res["message"].split("\n")
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
        res = self._get(self.restppUrl + "/showlicenseinfo", resKey=None, skipCheck=True)  # noqa
        ret = {}
        if not res["error"]:
            ret["message"] = res["message"]
            ret["expirationDate"] = res["results"][0]["Expiration date"]
            ret["daysRemaining"] = res["results"][0]["Days remaining"]
        elif "code" in res and res["code"] == "REST-5000":
            ret["message"] = "This instance does not have a valid enterprise license. Is this a trial version?"
            ret["daysRemaining"] = -1
        else:
            raise TigerGraphException(res["message"], res["code"])
        return ret

    # GSQL support =================================================

    def initGsql(self, certLocation="~/.gsql/my-cert.txt"):  # , jarLocation="~/.gsql"
        
        self.certLocation = os.path.expanduser(certLocation)
        self.url = urlparse(self.gsUrl).netloc  # Getting URL with gsql port w/o https://
        sslhost = self.url.split(":")[0]
        if self.downloadCert:  # HTTP/HTTPS
            import ssl
            try:
                Res = ssl.get_server_certificate((sslhost, self.sslPort))
            except:
                Res = ssl.get_server_certificate((sslhost, "14240"))

            try:
                certcontent = open(self.certLocation, 'w')
                certcontent.write(Res)
                certcontent.close()
            except Exception:
                
                self.certLocation = "/tmp/my-cert.txt"
                
                certcontent = open(self.certLocation, 'w')
                certcontent.write(Res)
                certcontent.close()
            if os.stat(self.certLocation).st_size == 0:
                raise TigerGraphException("Certificate download failed. Please check that the server is online.", None)

        try:

            if self.downloadCert:
                if not (self.certPath):
                    self.certPath = self.certLocation
                self.Client = GSQL_Client(urlparse(self.host).netloc, version=self.version, username=self.username,
                                          password=self.password,
                                          cacert=self.certPath,gsPort=self.gsPort,
                                          restpp=self.restppPort,debug=self.debug)
            else:
                self.Client = GSQL_Client(urlparse(self.host).netloc, version=self.version, username=self.username,
                                          password=self.password,
                                          gsPort=self.gsPort,restpp=self.restppPort,debug=self.debug)
            self.Client.login()
            self.gsqlInitiated = True
            return True
        except Exception as e:
            print("Connection Failed check your Username/Password {}".format(e))
            self.gsqlInitiated = False

    def gsql(self, query, graphname=None,options=None):
        """Runs a GSQL query and process the output.

        Arguments:
        - `query`:      The text of the query to run as one string.
        - `options`:    A list of strings that will be passed as options the the gsql_client. Use
                        `options=[]` to overide the default graph.
        """
        if graphname is None:
            graphname = self.graphname
        if str(graphname).upper() == "GLOBAL" or str(graphname).upper() == "" :
            graphname = ""
        if not self.gsqlInitiated:
            self.gsqlInitiated = self.initGsql()
        if self.gsqlInitiated:
            if "\n" not in query:
                res = self.Client.query(query,graph=graphname)
                if type(res) == type([]):
                    return "\n".join(res)
                else:
                    return res
            else:
                res = self.Client.run_multiple(query.split("\n"))
                if type(res) == type([]):
                    return "\n".join(res)
                else:
                    return res
        else:
            print("Couldn't Initialize the client see Above Error")
            exit(0)

        return

    # TODO: showSecret()

    # TODO: GET /showprocesslist/{graph_name}
    #       https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-running-queries-showprocesslist-graph_name

    def parseQueryOutput(self, output, graphOnly=True):
        """Parses query output and separates vertex and edge data (and optionally other output) for easier use.

        The JSON output from a query can contain a mixture of results: vertex sets (the output of a SELECT statement),
            edge sets (e.g. collected in a global accumulator), printout of global and local variables and accumulators,
            including complex types (LIST, MAP, etc.). The type of the various output entries is not explicit, you need
            to inspect the content to find out what it is actually.
        This function "cleans" this output, separating and collecting vertices and edges in an easy to access way.
            It can also collect other output or ignore it.
        The output of this function can be used e.g. with the `vertexSetToDataFrame()` and `edgeSetToDataFrame()` functions or
            (after some transformation) to pass a subgraph to a visualisation component.

        Arguments:
        - `output`:    The data structure returned by `runInstalledQuery()` or `runInterpretedQuery()`
        - `graphOnly`: Should output be restricted to vertices and edges (True, default) or should any other output (e.g. values of
                       variables or accumulators, or plain text printed) be captured as well.

        Returns: A dictionary with two (or three) keys: "vertices", "edges" and optionally "output". First two refer to another dictionary
            containing keys for each vertex and edge types found, and the instances of those vertex and edge types. "output" is a list of
            dictionaries containing the key/value pairs of any other output.
        """

        def attCopy(src, trg):
            """Copies the attributes of a vertex or edge into another vertex or edge, respectively."""
            srca = src["attributes"]
            trga = trg["attributes"]
            for att in srca:
                trga[att] = srca[att]

        def addOccurrences(obj, src):
            """Counts and lists te occurrences of a vertex or edge.
            A given vertex or edge can appear multiple times (in different vertex or edge sets) in the output of a query.
            Each output has a label (either the variable name or an alias used in the PRINT statement), `x_sources` contains a list of these labels.
            """
            if "x_occurrences" in obj:
                obj["x_occurrences"] += 1
            else:
                obj["x_occurrences"] = 1
            if "x_sources" in obj:
                obj["x_sources"].append(src)
            else:
                obj["x_sources"] = [src]

        vs = {}
        es = {}
        ou = []

        # Outermost data type is a list
        for o1 in output:
            # Next level data type is dictionary that could be vertex sets, edge sets or generic output (of simple or complex data types)
            for o2 in o1:
                _o2 = o1[o2]
                if isinstance(_o2, list) and len(_o2) > 0 and isinstance(_o2[0],
                                                                         dict):  # Is it an array of dictionaries?
                    for o3 in _o2:  # Iterate through the array
                        if "v_type" in o3:  # It's a vertex!

                            # Handle vertex type first
                            vType = o3["v_type"]
                            vtm = {}
                            if vType in vs:  # Do we have this type of vertices in our list (which is a dictionary, really)?
                                vtm = vs[vType]
                            else:  # No, let's create a dictionary for them and add to the list
                                vtm = {}
                                vs[vType] = vtm

                            # Then handle the vertex itself
                            vId = o3["v_id"]
                            if vId in vtm:  # Do we have this specific vertex (identified by the ID) in our list?
                                tmp = vtm[vId]
                                attCopy(o3, tmp)
                                addOccurrences(tmp, o2)
                            else:  # No, add it
                                addOccurrences(o3, o2)
                                vtm[vId] = o3

                        elif "e_type" in o3:  # It's an edge!

                            # Handle edge type first
                            eType = o3["e_type"]
                            etm = {}
                            if eType in es:  # Do we have this type of edges in our list (which is a dictionary, really)?
                                etm = es[eType]
                            else:  # No, let's create a dictionary for them and add to the list
                                etm = {}
                                es[eType] = etm

                            # Then handle the edge itself
                            eId = o3["from_type"] + "(" + o3["from_id"] + ")->" + o3["to_type"] + "(" + o3[
                                "to_id"] + ")"
                            o3["e_id"] = eId

                            # Add reverse edge name, if applicable
                            if self.isDirected(eType):
                                rev = self.getReverseEdge(eType)
                                if rev:
                                    o3["reverse_edge"] = rev

                            if eId in etm:  # Do we have this specific edge (identified by the composite ID) in our list?
                                tmp = etm[eId]
                                attCopy(o3, tmp)
                                addOccurrences(tmp, o2)
                            else:  # No, add it
                                addOccurrences(o3, o2)
                                etm[eId] = o3

                        else:  # It's a ... something else
                            ou.append({"label": o2, "value": _o2})
                else:  # It's a ... something else
                    ou.append({"label": o2, "value": _o2})

        ret = {"vertices": vs, "edges": es}
        if not graphOnly:
            ret["output"] = ou
        return ret

    # Path-finding algorithms ==================================================

    def _preparePathParams(self, sourceVertices, targetVertices, maxLength=None, vertexFilters=None, edgeFilters=None,
                           allShortestPaths=False):
        """Prepares the input parameters by transforming them to the format expected by the path algorithms.

        Arguments:
        - `sourceVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the source vertices of the shortest paths sought.
        - `targetVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the target vertices of the shortest paths sought.
        - `maxLength`:        The maximum length of a shortest path. Optional, default is 6.
        - `vertexFilters`:    An optional list of (vertexType, condition) tuples or {"type": <str>, "condition": <str>} dictionaries.
        - `edgeFilters`:      An optional list of (edgeType, condition) tuples or {"type": <str>, "condition": <str>} dictionaries.
        - `allShortestPaths`: If true, the endpoint will return all shortest paths between the source and target.
                              Default is false, meaning that the endpoint will return only one path.

        See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#input-parameters-and-output-format-for-path-finding for information on filters.
        """

        def parseVertices(vertices):
            """Parses vertex input parameters and converts it to the format required by the path finding endpoints"""
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
            print(ret)
            return ret

        def parseFilters(filters):
            """Parses filter input parameters and converts it to the format required by the path finding endpoints"""
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
            print(ret)
            return ret

        # Assembling the input payload
        if not sourceVertices or not targetVertices:
            return None  # Should allow TigerGraph to return error instead of handling missing parameters here?
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

    def shortestPath(self, sourceVertices, targetVertices, maxLength=None, vertexFilters=None, edgeFilters=None,
                     allShortestPaths=False):
        """Find the shortest path (or all shortest paths) between the source and target vertex sets.

        Arguments:
        - `sourceVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the source vertices of the shortest paths sought.
        - `targetVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the target vertices of the shortest paths sought.
        - `maxLength`:        The maximum length of a shortest path. Optional, default is 6.
        - `vertexFilters`:    An optional list of (vertexType, condition) tuples or {"type": <str>, "condition": <str>} dictionaries.
        - `edgeFilters`:      An optional list of (edgeType, condition) tuples or {"type": <str>, "condition": <str>} dictionaries.
        - `allShortestPaths`: If true, the endpoint will return all shortest paths between the source and target.
                              Default is false, meaning that the endpoint will return only one path.

        See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#input-parameters-and-output-format-for-path-finding for information on filters.

        Endpoint:      POST /shortestpath/{graphName}
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-shortestpath-graphname-shortest-path-search
        """
        data = self._preparePathParams(sourceVertices, targetVertices, maxLength, vertexFilters, edgeFilters,
                                       allShortestPaths)
        return self._post(self.restppUrl + "/shortestpath/" + self.graphname, data=data)

    def allPaths(self, sourceVertices, targetVertices, maxLength, vertexFilters=None, edgeFilters=None):
        """Find all possible paths up to a given maximum path length between the source and target vertex sets.

        Arguments:
        - `sourceVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the source vertices of the shortest paths sought.
        - `targetVertices`:   A vertex set (a list of vertices) or a list of (vertexType, vertexID) tuples; the target vertices of the shortest paths sought.
        - `maxLength`:        The maximum length of the paths.
        - `vertexFilters`:    An optional list of (vertexType, condition) tuples or {"type": <str>, "condition": <str>} dictionaries.
        - `edgeFilters`:      An optional list of (edgeType, condition) tuples or {"type": <str>, "condition": <str>} dictionaries.

        See https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#input-parameters-and-output-format-for-path-finding for information on filters.

        Endpoint:      POST /allpaths/{graphName}
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#post-allpaths-graphname-all-paths-search
        """
        data = self._preparePathParams(sourceVertices, targetVertices, maxLength, vertexFilters, edgeFilters)
        return self._post(self.restppUrl + "/allpaths/" + self.graphname, data=data)

    def showSecrets(self):
        """Issues a `CREATE SECRET` GSQL statement and returns the secret generated by that statement."""
        if not self.gsqlInitiated:
            self.initGsql()

        response = self.gsql("""
                USE GRAPH {}
                SHOW SECRET """.format(self.graphname))
        return response

    def createSecret(self, alias=""):
        """Issues a `CREATE SECRET` GSQL statement and returns the secret generated by that statement."""
        if not self.gsqlInitiated:
            self.initGsql()

        response = self.gsql("""
        USE GRAPH {}
        CREATE SECRET {} """.format(self.graphname, alias))
        try:
            # print(response)
            if ("already exists" in response):
                # get the sec
                errorMsg = "E-00001 : the secret "
                if alias != "":
                    errorMsg += "with alias {} ".format(alias)
                errorMsg += "exists."
                raise Exception(errorMsg)
            secret = "".join(response).replace('\n', '').split('The secret: ')[1].split(" ")[0]
            return secret
        except:
            raise

    def uploadFile(self, filePath, fileTag, jobName="", timeout=16000, sizeLimit=128000000, attributes=None):
        """DDL Upload File .

        Endpoint:      POST /graph

        """
        try:
            data = open(filePath, 'rb').read()
            params = {
                "tag": jobName,
                "filename": fileTag
            }
        except:
            return None
        return self._post(self.restppUrl + "/ddl/" + self.graphname, params=params, data=data,
                          headers={"RESPONSE-LIMIT": str(sizeLimit), "GSQL-TIMEOUT": str(timeout)})

# EOF
