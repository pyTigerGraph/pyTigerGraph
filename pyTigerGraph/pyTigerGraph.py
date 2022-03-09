import base64
import json
import os
import re
import sys
import time
import urllib
import warnings
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import requests
import urllib3
# Added pyTigerDriver Client
from pyTigerDriver import GSQL_Client

urllib3.disable_warnings()  # TODO Anything less risky approach?
warnings.filterwarnings("default", category=DeprecationWarning)


def excepthook(type, value, traceback):
    """This function prints out a given traceback and exception to sys.stderr.

    See: https://docs.python.org/3/library/sys.html#sys.excepthook
    """
    print(value)


class TigerGraphException(Exception):
    """Generic TigerGraph specific exception.

    Where possible, error message and code returned by TigerGraph will be used.
    """

    def __init__(self, message, code=None):
        self.message = message
        self.code = code


class TigerGraphConnection(object):
    """Python wrapper for TigerGraph's REST++ and GSQL APIs"""

    def __init__(self, host: str = "http://127.0.0.1", graphname: str = "MyGraph",
            username: str = "tigergraph", password: str = "tigergraph",
            restppPort: [int, str] = "9000", gsPort: [int, str] = "14240", gsqlVersion: str = "",
            version: str = "", apiToken: str = "", useCert: bool = True, certPath: str = None,
            debug: bool = False, sslPort: [int, str] = "443", gcp: bool = False):
        """Initiate a connection object.

        Args:
            host:
                The host name or IP address of the TigerGraph server.
            graphname:
                The default graph for running queries.
            username:
                The username on the TigerGraph server.
            password:
                The password for that user.
            restppPort:
                The port for REST++ queries.
            gsPort:
                The port of all other queries.
            gsqlVersion:
                The version of the GSQL client to be used. Effectively the version of the database
                being connected to.
            version:
                DEPRECATED; use gsqlVersion.
            apiToken:
                DEPRECATED; use getToken() with a secret to get a session token.
            useCert:
                DEPRECATED; the need for CA certificate is now determined by URL scheme.
            certPath:
                The filesystem path to the CA certificate. Required in case of https connections.
            debug:
                Enable debug messages.
            sslPort:
                Port for fetching SSL certificate in case of firewall.
            gcp:
                Is firewall used?

        Raises:
            TigerGraphException: In case on invalid URL scheme.

        TODO Rename/generalise `gcp`
        """
        inputHost = urlparse(host)
        if inputHost.scheme not in ["http", "https"]:
            raise TigerGraphException("Invalid URL scheme. Supported schemes are http and https.",
                "E-0003")
        self.netloc = inputHost.netloc
        self.host = "{0}://{1}".format(inputHost.scheme, self.netloc)
        self.username = username
        self.password = password
        self.graphname = graphname

        # TODO Use more generic name (e.g. `onCloud` or `viaFirewall`; not `beta` or `cgp`
        self.beta = gcp
        if self.beta == True and (restppPort == "9000" or restppPort == "443"):
            # TODO Should not `sslPort` be used instead of hard coded value?
            self.restppPort = "443"
            self.restppUrl = self.host + ":443" + "/restpp"
        else:
            self.restppPort = str(restppPort)
            self.restppUrl = self.host + ":" + self.restppPort
        self.gsPort = ""
        if self.beta == True and (gsPort == "14240" or gsPort == "443"):
            # TODO Should not `sslPort` be used instead of hard coded value?
            self.gsPort = "443"
            self.gsUrl = self.host + ":443"
        else:
            self.gsPort = str(gsPort)
            self.gsUrl = self.host + ":" + self.gsPort

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

        self.debug = debug
        if not self.debug:
            sys.excepthook = excepthook
            sys.tracebacklimit = None
        self.schema = None
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
    def _safeChar(self, inputString: str) -> str:
        """Replace special characters in string using the %xx escape.

        Args:
            inputString:
                The string to process

        Returns:
            Processed string.

        Documentation:
            https://docs.python.org/3/library/urllib.parse.html#url-quoting
        """
        return urllib.parse.quote(str(inputString), safe='')

    def _errorCheck(self, res: dict):
        """Checks if the JSON document returned by an endpoint has contains ``error: true``. If so,
            it raises an exception.

        Args:
            res:
                The output from a request.

        Raises:
            TigerGraphException: if request returned with error, indicated in the returned JSON.
        """
        if "error" in res and res["error"] and res["error"] != "false":
            # Endpoint might return string "false" rather than Boolean false
            raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def _req(self, method: str, url: str, authMode: str = "token", headers: dict = None,
            data: [dict, list, str] = None, resKey: str = "results", skipCheck: bool = False,
            params: [dict, list, str] = None) -> [dict, list]:
        """Generic REST++ API request.

        Args:
            method:
                HTTP method, currently one of GET, POST or DELETE.
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, one of "token" (default) or "pwd".
            headers:
                Standard HTTP request headers.
            data:
                Request payload, typically a JSON document.
            resKey:
                The JSON subdocument to be returned, default is "result".
            skipCheck:
                Skip error checking? Some endpoints return error to indicate that the requested
                action is not applicable; a problem, but not really an error.
            params:
                Request URL parameters.

        Returns:
            The (relevant part of the) response from the request (as a dictionary).
        """
        if authMode == "token" and str(self.apiToken) != "":
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
        if method == "POST":
            _data = data
        else:
            _data = None

        if self.useCert is True or self.certPath is not None:
            res = requests.request(method, url, headers=_headers, data=_data, params=params,
                verify=False)
        else:
            res = requests.request(method, url, headers=_headers, data=_data, params=params)

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

    def _get(self, url: str, authMode: str = "token", headers: dict = None, resKey: str = "results",
            skipCheck: bool = False, params: [dict, list, str] = None) -> [dict, list]:
        """Generic GET method.

        Args:
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, one of "token" (default) or "pwd".
            headers:
                Standard HTTP request headers.
            resKey:
                The JSON subdocument to be returned, default is "result".
            skipCheck:
                Skip error checking? Some endpoints return error to indicate that the requested
                action is not applicable; a problem, but not really an error.
            params:
                Request URL parameters.

        Returns:
            The (relevant part of the) response from the request (as a dictionary).
       """
        res = self._req("GET", url, authMode, headers, None, resKey, skipCheck, params)
        return res

    def _post(self, url: str, authMode: str = "token", headers: dict = None,
            data: [dict, list, str] = None, resKey: str = "results", skipCheck: bool = False,
            params: [dict, list, str] = None) -> [dict, list]:
        """Generic POST method.

        Args:
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, one of "token" (default) or "pwd".
            headers:
                Standard HTTP request headers.
            data:
                Request payload, typically a JSON document.
            resKey:
                The JSON subdocument to be returned, default is "result".
            skipCheck:
                Skip error checking? Some endpoints return error to indicate that the requested
                action is not applicable; a problem, but not really an error.
            params:
                Request URL parameters.

        Returns:
            The (relevant part of the) response from the request (as a dictionary).
        """
        return self._req("POST", url, authMode, headers, data, resKey, skipCheck, params)

    def _delete(self, url: str, authMode: str = "token") -> [dict, list]:
        """Generic DELETE method.

        Args:
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, one of "token" (default) or "pwd".

        Returns:
            The response from the request (as a dictionary).
       """
        return self._req("DELETE", url, authMode)

    def _upsertAttrs(self, attributes: dict) -> dict:
        """Transforms attributes (provided as a table) into a hierarchy as expect by the upsert
            functions.

        Args:
            attributes: A dictionary of attribute/value pairs (with an optional operator) in this
                format:
                    {<attribute_name>: <attribute_value>|(<attribute_name>, <operator>), …}

        Returns:
            A dictionary in this format:
                {
                    <attribute_name>: {"value": <attribute_value>},
                    <attribute_name>: {"value": <attribute_value>, "op": <operator>}
                }

        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#operation-codes
        """
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

    def _getUDTs(self) -> dict:
        """Retrieves all User Defined Types (UDTs) of the graph.

        Returns:
            The list of names of UDTs (defined in the global scope, i.e. not in queries).

        Endpoint:
            GET /gsqlserver/gsql/udtlist
        """
        return self._get(self.gsUrl + "/gsqlserver/gsql/udtlist?graph=" + self.graphname,
            authMode="pwd")

    def getSchema(self, udts: bool = True, force: bool = False) -> dict:
        """Retrieves the schema metadata (of all vertex and edge type and – if not disabled – the
            User Defined Type details) of the graph.

        Args:
            udts:
                If `True`, calls `_getUDTs()`, i.e. includes User Defined Types in the schema
                details.
            force:
                If `True`, retrieves the schema metadata again, otherwise returns a cached copy of
                the schema metadata (if they were already fetched previously).

        Returns:
            The schema metadata.

        Endpoint:
            GET /gsqlserver/gsql/schema
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#show-graph-schema-metadata
        """
        if not self.schema or force:
            self.schema = self._get(self.gsUrl + "/gsqlserver/gsql/schema?graph=" + self.graphname,
                authMode="pwd")
        if udts and ("UDTs" not in self.schema or force):
            self.schema["UDTs"] = self._getUDTs()
        return self.schema

    def getUDTs(self) -> list:
        """Returns the list of User Defined Types (names only).

        Returns:
            The list of names of UDTs (defined in the global scope, i.e. not in queries).

        Documentation:
            https://docs.tigergraph.com/dev/gsql-ref/ddl-and-loading/system-and-language-basics#typedef-tuple
        """
        ret = []
        for udt in self._getUDTs():
            ret.append(udt["name"])
        return ret

    def getUDT(self, udtName: str) -> list:
        """Returns the details of a specific User Defined Type (defined in the global scope).

        Args:
            udtName:
                The name of the User Defined Type.

        Returns:
            The metadata (details of the fields) of the UDT.

        Documentation:
            https://docs.tigergraph.com/dev/gsql-ref/ddl-and-loading/system-and-language-basics#typedef-tuple
        """
        for udt in self._getUDTs():
            if udt["name"] == udtName:
                return udt["fields"]
        return []  # UDT was not found
        # TODO Should raise exception instead?

    def upsertData(self, data: [str, object]) -> dict:
        """Upserts data (vertices and edges) from a JSON document or equivalent object structure.

        Args:
            data:
                The data of vertex and edge instances, in a specific format.

        Returns:
            The result of upsert (number of vertices and edges accepted/upserted).

        Endpoint:
            POST /graph
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#upsert-data-to-graph
        """
        if not isinstance(data, str):
            data = json.dumps(data)
        return self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0]

    # Vertex related functions =================================================

    def getVertexTypes(self, force: bool = False) -> list:
        """Returns the list of vertex type names of the graph.

        Args:
            force:
                If `True`, forces the retrieval the schema metadata again, otherwise returns a
                cached copy of vertex type details (if they were already fetched previously).

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

    def getVertexCount(self, vertexType: str, where: str = "") -> dict:
        """Returns the number of vertices of the specified type.

        Uses:
            If ``vertexType`` == "*": vertex count of all vertex types (`where` cannot be specified
                in this case).
            If ``vertexType`` is specified only: vertex count of the given type.
            If ``vertexType`` and ``where`` are specified: vertex count of the given type after
                filtered by ``where`` condition(s).

        Args:
            vertexType:
                The name of the vertex type.
            where:
                A comma separated list of conditions that are all applied on each vertex's
                attributes. The conditions are in logical conjunction (i.e. they are "AND'ed"
                together).
                See: filter at https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#parameters-15

        Returns:
             A dictionary of <vertex_type>: <vertex_count> pairs.

        Endpoint:
            GET /graph/{graph_name}/vertices
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#list-vertices

        Endpoint:
            POST /builtins
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#run-built-in-functions-on-graph
        """
        # If WHERE condition is not specified, use /builtins else use /vertices
        if where:
            if vertexType == "*":
                raise TigerGraphException(
                    "VertexType cannot be \"*\" if where condition is specified.", None)
            res = self._get(
                self.restppUrl + "/graph/" + self.graphname + "/vertices/" + vertexType +
                "?count_only=true&filter=" + where)
        else:
            data = '{"function":"stat_vertex_number","type":"' + vertexType + '"}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data)
        if len(res) == 1 and res[0]["v_type"] == vertexType:
            return res[0]["count"]
        ret = {}
        for r in res:
            ret[r["v_type"]] = r["count"]
        return ret

    def upsertVertex(self, vertexType: str, vertexId: str, attributes: dict = None) -> int:
        """Upserts a vertex.

        Data is upserted:
            If vertex is not yet present in graph, it will be created.
            If it's already in the graph, its attributes are updated with the values specified in
                the request. An optional operator controls how the attributes are updated.

        Args:
            vertexType:
                The name of the vertex type.
            vertexId:
                The primary ID of the vertex to be upserted.
            attributes:
                The attributes of the vertex to be upserted; a dictionary in this format:
                    {<attribute_name>: <attribute_value>|(<attribute_name>, <operator>), …}
                Example:
                    {"name": "Thorin", points: (10, "+"), "bestScore": (67, "max")}
                For valid values of <operator> see:
                    https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#operation-codes

        Returns:
             A single number of accepted (successfully upserted) vertices (0 or 1).

        Endpoint:
            POST /graph/{graph_name}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#upsert-data-to-graph
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
                    [
                        (<vertex_id>, {<attribute_name>, <attribute_value>, …}),
                        (<vertex_id>, {<attribute_name>, (<attribute_name>, <operator>), …}),
                        ⋮
                    ]
                Example:
                    [
                        (2, {"name": "Balin", "points": (10, "+"), "bestScore": (67, "max")}),
                        (3, {"name": "Dwalin", "points": (7, "+"), "bestScore": (35, "max")})
                    ]
                For valid values of <operator> see:
                    https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#operation-codes

        Returns:
            A single number of accepted (successfully upserted) vertices (0 or positive integer).

        Endpoint:
            POST /graph/{graph_name}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#upsert-data-to-graph
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

    def getVertices(self, vertexType: str, select: str = "", where: str = "",
            limit: [int, str] = None, sort: str = "", fmt: str = "py", withId: bool = True,
            withType: bool = False, timeout: int = 0) -> [dict, str, pd.DataFrame]:
        """Retrieves vertices of the given vertex type.

        Notes:
            The primary ID of a vertex instance is NOT an attribute, thus cannot be used in
            ``select``, ``where`` or ``sort`` parameters (unless the
            ``WITH primary_id_as_attribute`` clause was used when the vertex type was created).
            Use ``getVerticesById()`` if you need to retrieve vertices by their primary ID.

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
            The (selected) details of the (matching) vertex instances (sorted, limited) as
            dictionary, JSON or pandas DataFrame.

        Endpoint:
            GET /graph/{graph_name}/vertices/{vertex_type}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#list-vertices
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

    def getVertexDataframe(self, vertexType: str, select: str = "", where: str = "",
            limit: str = "", sort: str = "", timeout: int = 0) -> pd.DataFrame:
        """Retrieves vertices of the given vertex type and returns them as pandas DataFrame.

        This is a shortcut to ``getVertices(..., fmt="df", withId=True, withType=False)``.

        Notes:
            The primary ID of a vertex instance is NOT an attribute, thus cannot be used in
            ``select``, ``where`` or ``sort`` parameters (unless the
            ``WITH primary_id_as_attribute`` clause was used when the vertex type was created).
            Use ``getVerticesById()`` if you need to retrieve vertices by their primary ID.

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
            GET /graph/{graph_name}/vertices/{vertex_type}/{vertex_id}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#retrieve-a-vertex
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

    def getVertexDataframeById(self, vertexType: str, vertexIds: [int, str, list],
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
        return self.getVerticesById(vertexType, vertexIds, fmt="df", withId=True, withType=False)

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
            POST /builtins/{graph_name}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#run-built-in-functions-on-graph
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
                if "stat_vertex_attr is skipped" in res["message"]:
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

        Notes:
            The primary ID of a vertex instance is NOT an attribute, thus cannot be used in
            ``where`` or ``sort`` parameters (unless the ``WITH primary_id_as_attribute`` clause was
            used when the vertex type was created).
            Use ``delVerticesById`` if you need to delete by vertex ID.

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
            DELETE /graph/{graph_name}/vertices/{vertex_type}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-vertices
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
            DELETE /graph/{graph_name}/vertices/{vertex_type}/{vertex_id}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-a-vertex
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

    # Edge related functions ===================================================

    def getEdgeTypes(self, force: bool = False) -> list:
        """Returns the list of edge type names of the graph.

        Args:
            force:
                If `True`, forces the retrieval the schema details again, otherwise returns a cached
                copy of edge type metadata (if they were already fetched previously).

        Returns:
            The list of edge types defined in the the current graph.
        """
        ret = []
        for et in self.getSchema(force=force)["EdgeTypes"]:
            ret.append(et["Name"])
        return ret

    def getEdgeType(self, edgeType: str, force: bool = False) -> dict:
        """Returns the details of vertex type.

        Args:
            edgeType:
                The name of the edge type.
            force:
                If `True`, forces the retrieval the schema details again, otherwise returns a cached
                copy of edge type details (if they were already fetched previously).

        Returns:
            The metadata of the edge type.
        """
        for et in self.getSchema(force=force)["EdgeTypes"]:
            if et["Name"] == edgeType:
                return et
        return {}

    def getEdgeSourceVertexType(self, edgeType: str) -> [str, set]:
        """Returns the type(s) of the edge type's source vertex.

        Args:
            edgeType: The name of the edge type.

        Returns:
            A single source vertex type name string if the edge has a single source vertex type.
            "*" if the edge can originate from any vertex type (notation used in 2.6.1 and earlier
                versions).
                See https://docs.tigergraph.com/v/2.6/dev/gsql-ref/ddl-and-loading/defining-a-graph-schema#creating-an-edge-from-or-to-any-vertex-type
            A set of vertex type name strings (unique values) if the edge has multiple source vertex
                types (notation used in 3.0 and later versions).
            Even if the source vertex types were defined as "*", the REST API will list them as
                pairs (i.e. not as "*" in 2.6.1 and earlier versions), just like as if there were
                defined one by one (e.g. `FROM v1, TO v2 | FROM v3, TO v4 | …`).
            The returned set contains all source vertex types, but does not certainly mean that the
                edge is defined between all source and all target vertex types. You need to look at
                the individual source/target pairs to find out which combinations are valid/defined.
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

    def getEdgeTargetVertexType(self, edgeType: str) -> [str, set]:
        """Returns the type(s) of the edge type's target vertex.

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            A single target vertex type name string if the edge has a single target vertex type.
            "*" if the edge can end in any vertex type (notation used in 2.6.1 and earlier versions).
                See https://docs.tigergraph.com/v/2.6/dev/gsql-ref/ddl-and-loading/defining-a-graph-schema#creating-an-edge-from-or-to-any-vertex-type
            A set of vertex type name strings (unique values) if the edge has multiple target vertex
                types (notation used in 3.0 and later versions).
            Even if the target vertex types were defined as "*", the REST API will list them as
                pairs (i.e. not as "*" in 2.6.1 and earlier versions), just like as if there were
                defined one by one (e.g. `FROM v1, TO v2 | FROM v3, TO v4 | …`).
            The returned set contains all target vertex types, but does not certainly mean that the
                edge is defined between all source and all target vertex types. You need to look at
                the individual source/target pairs to find out which combinations are valid/defined.
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

    def isDirected(self, edgeType: str) -> bool:
        """Is the specified edge type directed?

        Args:
            edgeType:
                The name of the edge type.

        Returns:
            `True` if the edge is directed.
        """
        return self.getEdgeType(edgeType)["IsDirected"]

    def getReverseEdge(self, edgeType: str) -> str:
        """Returns the name of the reverse edge of the specified edge type, if applicable.

        Args:
           edgeType:
                The name of the edge type.

        Returns:
            The name of the reverse edge, if specified.
        """
        if not self.isDirected(edgeType):
            return ""
            # TODO Should return some other value or raise exception?
        config = self.getEdgeType(edgeType)["Config"]
        if "REVERSE_EDGE" in config:
            return config["REVERSE_EDGE"]
        return ""
        # TODO Should return some other value or raise exception?

    def getEdgeCountFrom(self, sourceVertexType: str = None, sourceVertexId: str = None,
            edgeType: str = None, targetVertexType: str = None, targetVertexId: str = None,
            where: str = "") -> dict:
        """Returns the number of edges from a specific vertex.

        Args:
            sourceVertexType:
                The name of the source vertex type.
            sourceVertexId:
                The primary ID value of the source vertex instance.
            edgeType:
                The name of the edge type.
            targetVertexType:
                The name of the target vertex type.
            targetVertexId:
                The primary ID value of the target vertex instance.
            where:
                A comma separated list of conditions that are all applied on each edge's attributes.
                The conditions are in logical conjunction (i.e. they are "AND'ed" together).

        Returns:
            A dictionary of <edge_type>: <edge_count> pairs.

        Uses:
            If `edgeType` = "*": edge count of all edge types (no other arguments can be specified
                in this case).
            If `edgeType` is specified only: edge count of the given edge type.
            If `sourceVertexType`, `edgeType`, `targetVertexType` are specified: edge count of the
                given edge type between source and target vertex types.
            If `sourceVertexType`, `sourceVertexId` are specified: edge count of all edge types from
                the given vertex instance.
            If `sourceVertexType`, `sourceVertexId`, `edgeType` are specified: edge count of all
                edge types from the given vertex instance.
            If `sourceVertexType`, `sourceVertexId`, `edgeType`, `where` are specified: the edge
                count of the given edge type after filtered by `where` condition.
            If `targetVertexId` is specified, then `targetVertexType` must also be specified.
            If `targetVertexType` is specified, then `edgeType` must also be specified.

        Endpoint:
            GET /graph/{graph_name}/edges/{source_vertex_type}/{source_vertex_id}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#list-edges-of-a-vertex

        Endpoint:
            POST /builtins/{graph_name}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#stat_edge_number
        """
        # If WHERE condition is not specified, use /builtins else user /vertices
        if where or (sourceVertexType and sourceVertexId):
            if not sourceVertexType or not sourceVertexId:
                raise TigerGraphException(
                    "If where condition is specified, then both sourceVertexType and sourceVertexId"
                    " must be provided too.", None)
            url = self.restppUrl + "/graph/" + self._safeChar(self.graphname) + \
                  "/edges/" + self._safeChar(sourceVertexType) + "/" + self._safeChar(
                sourceVertexId)
            if edgeType:
                url += "/" + self._safeChar(edgeType)
                if targetVertexType:
                    url += "/" + self._safeChar(targetVertexType)
                    if targetVertexId:
                        url += "/" + self._safeChar(targetVertexId)
            url += "?count_only=true"
            if where:
                url += "&filter=" + self._safeChar(where)
            res = self._get(url)
        else:
            if not edgeType:  # TODO is this a valid check?
                raise TigerGraphException(
                    "A valid edge type or \"*\" must be specified for edge type.", None)
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

    def getEdgeCount(self, edgeType: str = "*", sourceVertexType: str = None,
            targetVertexType: str = None) -> dict:
        """Returns the number of edges of an edge type.

        This is a simplified version of ``getEdgeCountFrom()``, to be used when the total number of
        edges of a given type is needed, regardless which vertex instance they are originated from.
        See documentation of `getEdgeCountFrom` above for more details.

        Args:
            edgeType:
                The name of the edge type.
            sourceVertexType:
                The name of the source vertex type.
            targetVertexType:
                The name of the target vertex type.

        Returns:
            A dictionary of <edge_type>: <edge_count> pairs.
        """
        return self.getEdgeCountFrom(edgeType=edgeType, sourceVertexType=sourceVertexType,
            targetVertexType=targetVertexType)

    def upsertEdge(self, sourceVertexType: str, sourceVertexId: str, edgeType: str,
            targetVertexType: str, targetVertexId: str, attributes: dict = None) -> int:
        """Upserts an edge.

        Data is upserted:
            If edge is not yet present in graph, it will be created (see special case below).
            If it's already in the graph, it is updated with the values specified in the request.
            If operator is "vertex_must_exist" then edge will only be created if both vertex exists
            in graph. Otherwise missing vertices are created with the new edge; the newly created
            vertices' attributes (if any) will be created with default values.

        Args:
            sourceVertexType:
                The name of the source vertex type.
            sourceVertexId:
                The primary ID value of the source vertex instance.
            edgeType:
                The name of the edge type.
            targetVertexType:
                The name of the target vertex type.
            targetVertexId:
                The primary ID value of the target vertex instance.
            attributes:
                A dictionary in this format:
                    {<attribute_name>, <attribute_value>|(<attribute_name>, <operator>), …}
                Example:
                    {"visits": (1482, "+"), "max_duration": (371, "max")}
                For valid values of <operator> see: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#operation-codes

        Returns:
            A single number of accepted (successfully upserted) edges (0 or 1).

        Endpoint:
            POST /graph/{graph_name}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#upsert-data-to-graph
        """
        if attributes is None:
            attributes = {}
        if not isinstance(attributes, dict):
            return None
            # TODO Should return 0 or raise an exception instead?
        vals = self._upsertAttrs(attributes)
        data = json.dumps(
            {"edges": {sourceVertexType: {
                sourceVertexId: {edgeType: {targetVertexType: {targetVertexId: vals}}}}}})
        return self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0][
            "accepted_edges"]

    def upsertEdges(self, sourceVertexType: str, edgeType: str, targetVertexType: str,
            edges: list) -> int:
        """Upserts multiple edges (of the same type).

            sourceVertexType:
                The name of the source vertex type.
            edgeType:
                The name of the edge type.
            targetVertexType:
                The name of the target vertex type.
            edges:
                A list in of tuples in this format:
                    [
                        (<source_vertex_id>, <target_vertex_id>, {<attribute_name>: <attribute_value>, …})
                        (<source_vertex_id>, <target_vertex_id>, {<attribute_name>: (<attribute_name>, <operator>), …})
                        ⋮
                    ]
                Example:
                    [
                        (17, "home_page", {"visits": (35, "+"), "max_duration": (93, "max")}),
                        (42, "search", {"visits": (17, "+"), "max_duration": (41, "max")}),
                    ]
                For valid values of <operator> see: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#operation-codes

        Returns
            A single number of accepted (successfully upserted) edges (0 or positive integer).

        Endpoint:
            POST /graph/{graph_name}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#upsert-data-to-graph
        """
        if not isinstance(edges, list):
            return None
            # TODO Should return 0 or raise an exception instead?
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
        return self._post(self.restppUrl + "/graph/" + self.graphname, data=data)[0][
            "accepted_edges"]

    def getEdges(self, sourceVertexType: str, sourceVertexId: str, edgeType: str = None,
            targetVertexType: str = None, targetVertexId: str = None, select: str = "",
            where: str = "", limit: str = "", sort: str = "", fmt: str = "py", withId: bool = True,
            withType: bool = False, timeout: int = 0) -> [dict, str, pd.DataFrame]:
        """Retrieves edges of the given edge type originating from a specific source vertex.

        Only `sourceVertexType` and `sourceVertexId` are required.
        If `targetVertexId` is specified, then `targetVertexType` must also be specified.
        If `targetVertexType` is specified, then `edgeType` must also be specified.

        Args:
            sourceVertexType:
                The name of the source vertex type.
            sourceVertexId:
                The primary ID value of the source vertex instance.
            edgeType:
                The name of the edge type.
            targetVertexType:
                The name of the target vertex type.
            targetVertexId:
                The primary ID value of the target vertex instance.
            select:
                Comma separated list of edge attributes to be retrieved or omitted.
            where:
                Comma separated list of conditions that are all applied on each edge's attributes.
                The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            sort:
                Comma separated list of attributes the results should be sorted by.
            limit:
                Maximum number of edge instances to be returned (after sorting).
            fmt:
                Format of the results:
                    "py":   Python objects
                    "json": JSON document
                    "df":   pandas DataFrame
            withId:
                (If the output format is "df") should the source and target vertex types and IDs be
                included in the dataframe?
            withType:
                (If the output format is "df") should the edge type be included in the dataframe?
            timeout:
                Time allowed for successful execution (0 = no time limit, default).

        Returns:
            The (selected) details of the (matching) edge instances (sorted, limited) as dictionary,
            JSON or pandas DataFrame.

        Endpoint:
            GET /graph/{graph_name}/edges/{source_vertex_type}/{source_vertex_id}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#list-edges-of-a-vertex
        """
        # TODO Change sourceVertexId to sourceVertexIds and allow passing both str and list<str> as
        #   parameter
        if not sourceVertexType or not sourceVertexId:
            raise TigerGraphException(
                "Both source vertex type and source vertex ID must be provided.", None)
        url = self.restppUrl + "/graph/" + self.graphname + "/edges/" + sourceVertexType + "/" + \
              str(sourceVertexId)
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

    def getEdgesDataframe(self, sourceVertexType: str, sourceVertexId: str, edgeType: str = "",
            targetVertexType: str = "", targetVertexId: str = "", select: str = "", where: str = "",
            limit: str = "", sort: str = "", timeout: int = 0) -> pd.DataFrame:
        """Retrieves edges of the given edge type originating from a specific source vertex.

        This is a shortcut to ``getEdges(..., fmt="df", withId=True, withType=False)``.
        Only ``sourceVertexType`` and ``sourceVertexId`` are required.
        If ``targetVertexId`` is specified, then ``targetVertexType`` must also be specified.
        If ``targetVertexType`` is specified, then ``edgeType`` must also be specified.

        Args:
            sourceVertexType:
                The name of the source vertex type.
            sourceVertexId:
                The primary ID value of the source vertex instance.
            edgeType:
                The name of the edge type.
            targetVertexType:
                The name of the target vertex type.
            targetVertexId:
                The primary ID value of the target vertex instance.
            select:
                Comma separated list of edge attributes to be retrieved or omitted.
            where:
                Comma separated list of conditions that are all applied on each edge's attributes.
                The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            sort:
                Comma separated list of attributes the results should be sorted by.
            limit:
                Maximum number of edge instances to be returned (after sorting).
            timeout:
                Time allowed for successful execution (0 = no limit, default).

        Returns:
            The (selected) details of the (matching) edge instances (sorted, limited) as dictionary,
            JSON or pandas DataFrame.
        """
        if isinstance(sourceVertexId, list):
            raise TigerGraphException("List is not yet supported", None)
        else:
            return self.getEdges(sourceVertexType, sourceVertexId, edgeType, targetVertexType,
                targetVertexId, select, where, limit, sort, fmt="df", timeout=timeout)

    def getEdgesByType(self, edgeType: str, fmt: str = "py", withId: bool = True,
            withType: bool = False) -> [dict, str, pd.DataFrame]:
        """Retrieves edges of the given edge type regardless the source vertex.

        Args:
            edgeType:
                The name of the edge type.
            fmt:
                Format of the results:
                    "py":   Python objects
                    "json": JSON document
                    "df":   pandas DataFrame
            withId:
                (If the output format is "df") should the source and target vertex types and IDs be
                    included in the dataframe?
            withType:
                (If the output format is "df") should the edge type be included in the dataframe?

        TODO Add limit parameter
        """
        if not edgeType:
            return []

        sourceVertexType = self.getEdgeSourceVertexType(edgeType)
        # TODO Support edges with multiple source vertex types
        if isinstance(sourceVertexType, set) or sourceVertexType == "*":
            raise TigerGraphException(
                "Edges with multiple source vertex types are not currently supported.", None)

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

    # TODO getEdgesDataframeByType

    def getEdgeStats(self, edgeTypes: [str, list], skipNA: bool = False) -> dict:
        """Returns edge attribute statistics.

        Args:
            edgeTypes:
                A single edge type name or a list of edges types names or '*' for all edges types.
            skipNA:
                Skip those edges that do not have attributes or none of their attributes have
                statistics gathered.

        Returns:
            Attribute statistics of edges; a dictionary of dictionaries.

        Endpoint:
            POST /builtins/{graph_name}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#run-built-in-functions-on-graph
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
            # TODO Should return {} or raise exception?
        ret = {}
        for et in ets:
            data = '{"function":"stat_edge_attr","type":"' + et + '","from_type":"*","to_type":"*"}'
            res = self._post(self.restppUrl + "/builtins/" + self.graphname, data=data, resKey="",
                skipCheck=True)
            if res["error"]:
                if "stat_edge_attr is skiped" in res["message"]:
                    if not skipNA:
                        ret[et] = {}
                else:
                    raise TigerGraphException(res["message"],
                        (res["code"] if "code" in res else None))
            else:
                res = res["results"]
                for r in res:
                    ret[r["e_type"]] = r["attributes"]
        return ret

    def delEdges(self, sourceVertexType: str, sourceVertexId: str, edgeType: str = None,
            targetVertexType: str = None, targetVertexId: str = None, where: str = "",
            limit: str = "", sort: str = "", timeout: int = 0):
        """Deletes edges from the graph.

        Only ``sourceVertexType`` and ``sourceVertexId`` are required.
        If ``targetVertexId`` is specified, then ``targetVertexType`` must also be specified.
        If ``targetVertexType`` is specified, then ``edgeType`` must also be specified.

        Args:
            sourceVertexType:
                The name of the source vertex type.
            sourceVertexId:
                The primary ID value of the source vertex instance.
            edgeType:
                The name of the edge type.
            targetVertexType:
                The name of the target vertex type.
            targetVertexId:
                The primary ID value of the target vertex instance.
            where:
                Comma separated list of conditions that are all applied on each edge's attributes.
                The conditions are in logical conjunction (i.e. they are "AND'ed" together).
            limit:
                Maximum number of edge instances to be returned (after sorting).
            sort:
                Comma separated list of attributes the results should be sorted by.
            timeout:
                Time allowed for successful execution (0 = no limit, default).

        Returns:
             A dictionary of <edge_type>: <deleted_edge_count> pairs.

        Endpoint:
            DELETE /graph/{graph_name}/edges/{source_vertex_type}/{source_vertex_id}/{edge_type}/{target_vertex_type}/{target_vertex_id}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#delete-an-edge
        """
        if not sourceVertexType or not sourceVertexId:
            raise TigerGraphException("Both sourceVertexType and sourceVertexId must be provided.",
                None)
        url = self.restppUrl + "/graph/" + self.graphname + "/edges/" + sourceVertexType + "/" + str(
            sourceVertexId)
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

    def getInstalledQueries(self, fmt: str = "py") -> [dict, json, pd.DataFrame]:
        """
        Returns a list of installed queries.

        Args:
            fmt:
                Format of the results:
                    "py":   Python objects (default)
                    "json": JSON document
                    "df":   pandas DataFrame

        Returns:
            The names of the installed queries.
        """
        ret = self.getEndpoints(dynamic=True)
        if fmt == "json":
            return json.dumps(ret)
        if fmt == "df":
            return pd.DataFrame(ret).T
        return ret

    # TODO getQueryMetadata()
    #   GET /gsqlserver/gsql/queryinfo
    #   https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-query-metadata

    def runInstalledQuery(self, queryName: str, params: [str, dict] = None, timeout: int = None,
            sizeLimit: int = None, usePost: bool = False) -> list:
        """Runs an installed query.

        The query must be already created and installed in the graph.
        Use ``getEndpoints(dynamic=True)`` or GraphStudio to find out the generated endpoint URL of
        the query, but only the query name needs to be specified here.

        Args:
            queryName:
                The name of the query to be executed.
            params:
                Query parameters. A string of param1=value1&param2=value2 format or a dictionary.
            timeout:
                Maximum duration for successful query execution (in milliseconds).
                See: https://docs.tigergraph.com/dev/restpp-api/intro#gsql-query-timeout
            sizeLimit:
                Maximum size of response (in bytes).
                See: https://docs.tigergraph.com/dev/restpp-api/intro#response-size
            usePost:
                The RESTPP accepts a maximum URL length of 8192 characters. Use POST if params cause
                you to exceed this limit.

        Returns:
            The output of the query, a list of output elements (vertex sets, edge sets, variables,
            accumulators, etc.

        Endpoint:
            GET /query/{graph_name}/{query_name}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#run-an-installed-query-get

        Endpoint:
            POST /query/{graph_name}/{query_name}
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#run-an-installed-query-post

        TODO Specify replica: GSQL-REPLICA
        TODO Specify thread limit: GSQL-THREAD-LIMIT
        TODO Detached mode
        """
        headers = {}
        if timeout and timeout > 0:
            headers["GSQL-TIMEOUT"] = str(timeout)
        if sizeLimit:
            headers["RESPONSE-LIMIT"] = str(sizeLimit)

        if isinstance(params, dict):
            params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote, safe='')

        if usePost:
            return self._post(self.restppUrl + "/query/" + self.graphname + "/" + queryName,
                data=params, headers=headers)
        else:
            return self._get(self.restppUrl + "/query/" + self.graphname + "/" + queryName,
                params=params, headers=headers)

    # TODO checkQueryStatus()

    # TODO getQueryResult()

    def runInterpretedQuery(self, queryText: str, params: [str, dict] = None) -> list:
        """Runs an interpreted query.

        Use ``$graphname`` in the ``FOR GRAPH`` clause to avoid hard-coding it; it will be replaced
        by the actual graph name.

        Args:
            queryText:
                The text of the GSQL query:
                You must provide the query text in this format:
                    INTERPRET QUERY (<params>) FOR GRAPH <graph_name> {
                        <statements>
                    }
            params:
                A string of param1=value1&param2=value2 format or a dictionary.

        Endpoint:
            POST /gsqlserver/interpreted_query
        Documentation:
            https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#run-an-interpreted-query
        """
        queryText = queryText.replace("$graphname", self.graphname)
        return self._post(self.gsUrl + "/gsqlserver/interpreted_query", data=queryText,
            params=params, authMode="pwd")

    # TODO getRunningQueries()
    #  GET /showprocesslist/{graph_name}
    #  https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#list-running-queries

    def parseQueryOutput(self, output: list, graphOnly: bool = True) -> dict:
        """Parses query output and separates vertex and edge data (and optionally other output) for
            easier use.

        Args:
            output:
                The data structure returned by `runInstalledQuery()` or `runInterpretedQuery()`.
            graphOnly:
                Should output be restricted to vertices and edges (True, default) or should any
                other output (e.g. values of variables or accumulators, or plain text printed) be
                captured as well.

        Returns:
            A dictionary with two (or three) keys: "vertices", "edges" and optionally "output".
            First two refer to another dictionary containing keys for each vertex and edge types
            found, and the instances of those vertex and edge types. "output" is a list of
            dictionaries containing the key/value pairs of any other output.

        The JSON output from a query can contain a mixture of results: vertex sets (the output of a
            SELECT statement), edge sets (e.g. collected in a global accumulator), printout of
            global and local variables and accumulators, including complex types (LIST, MAP, etc.).
            The type of the various output entries is not explicit, you need to inspect the content
            to find out what it is actually.
        This function "cleans" this output, separating and collecting vertices and edges in an easy
            to access way. It can also collect other output or ignore it.
        The output of this function can be used e.g. with the `vertexSetToDataFrame()` and
            `edgeSetToDataFrame()` functions or (after some transformation) to pass a subgraph to a
            visualisation component.
        """

        def attCopy(src: dict, trg: dict):
            """Copies the attributes of a vertex or edge into another vertex or edge, respectively.

            args:
                src:
                    Source vertex or edge instance.
                trg:
                    Target vertex or edge instance.
            """
            srca = src["attributes"]
            trga = trg["attributes"]
            for att in srca:
                trga[att] = srca[att]

        def addOccurrences(obj: dict, src: str):
            """Counts and lists te occurrences of a vertex or edge.

            Args:
                obj:
                    The vertex or edge that was found in the output.
                src:
                    The the label (variable name or alias) of the source where the vertex or edge
                    was found.

            A given vertex or edge can appear multiple times (in different vertex or edge sets) in
            the output of a query. Each output has a label (either the variable name or an alias
            used in the PRINT statement), `x_sources` contains a list of these labels.
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
            # Next level data type is dictionary that could be vertex sets, edge sets or generic
            # output (of simple or complex data types)
            for o2 in o1:
                _o2 = o1[o2]
                # Is it an array of dictionaries?
                if isinstance(_o2, list) and len(_o2) > 0 and isinstance(_o2[0], dict):
                    # Iterate through the array
                    for o3 in _o2:
                        if "v_type" in o3:  # It's a vertex!

                            # Handle vertex type first
                            vType = o3["v_type"]
                            vtm = {}
                            # Do we have this type of vertices in our list
                            # (which is actually a dictionary)?
                            if vType in vs:
                                vtm = vs[vType]
                            # No, let's create a dictionary for them and add to the list
                            else:
                                vtm = {}
                                vs[vType] = vtm

                            # Then handle the vertex itself
                            vId = o3["v_id"]
                            # Do we have this specific vertex (identified by the ID) in our list?
                            if vId in vtm:
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
                            # Do we have this type of edges in our list
                            # (which is actually a dictionary)?
                            if eType in es:
                                etm = es[eType]
                            # No, let's create a dictionary for them and add to the list
                            else:
                                etm = {}
                                es[eType] = etm

                            # Then handle the edge itself
                            eId = o3["from_type"] + "(" + o3["from_id"] + ")->" + o3["to_type"] + \
                                  "(" + o3["to_id"] + ")"
                            o3["e_id"] = eId

                            # Add reverse edge name, if applicable
                            if self.isDirected(eType):
                                rev = self.getReverseEdge(eType)
                                if rev:
                                    o3["reverse_edge"] = rev

                            # Do we have this specific edge (identified by the composite ID) in our
                            # list?
                            if eId in etm:
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

    # GSQL support =================================================

    def initGsql(self, certLocation: str = "~/.gsql/my-cert.txt") -> bool:
        """Initialises the GSQL support.

        Args:
            certLocation:
                The path and file of the CA certificate.

        Returns:
            `True` if initialisation was successful.

        Raises:
            Exception if initialisation was unsuccessful.
        """
        if not certLocation:
            if not os.path.isdir(os.path.expanduser("~/.gsql")):
                os.mkdir(os.path.expanduser("~/.gsql"))
            certLocation = "~/.gsql/my-cert.txt"

        self.certLocation = os.path.expanduser(certLocation)
        self.url = urlparse(self.gsUrl).netloc  # Getting URL with gsql port w/o https://
        sslhost = self.url.split(":")[0]

        if self.downloadCert:  # HTTP/HTTPS
            import ssl
            try:
                Res = ssl.get_server_certificate((sslhost, self.sslPort))
            except:
                Res = ssl.get_server_certificate((sslhost, 14240))

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
                raise TigerGraphException(
                    "Certificate download failed. Please check that the server is online.", None)

        try:

            if self.downloadCert:
                if not (self.certPath):
                    self.certPath = self.certLocation
                self.Client = GSQL_Client(urlparse(self.host).netloc, version=self.version,
                    username=self.username,
                    password=self.password,
                    cacert=self.certPath, gsPort=self.gsPort,
                    restpp=self.restppPort, debug=self.debug)
            else:
                self.Client = GSQL_Client(urlparse(self.host).netloc, version=self.version,
                    username=self.username,
                    password=self.password,
                    gsPort=self.gsPort, restpp=self.restppPort, debug=self.debug)
            self.Client.login()
            self.gsqlInitiated = True
            return True
        except Exception as e:
            print("Connection Failed check your Username/Password {}".format(e))
            self.gsqlInitiated = False

    def gsql(self, query: str, graphname: str = None, options=None) -> [str, dict]:
        """Runs a GSQL query and process the output.

        Args:
            query:
                The text of the query to run as one string. The query is one or more GSQL statement.
            graphname:
                The name of the graph to attach to. If not specified, the graph name provided at the
                time of establishing the connection will be used.
            options:
                DEPRECATED

        Returns:
            The output of the statement(s) executed.
        """
        if graphname is None:
            graphname = self.graphname
        if str(graphname).upper() == "GLOBAL" or str(graphname).upper() == "":
            graphname = ""
        if not self.gsqlInitiated:
            self.gsqlInitiated = self.initGsql()
        if self.gsqlInitiated:
            if "\n" not in query:
                res = self.Client.query(query, graph=graphname)
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
            print("Couldn't Initialize the client see above error.")
            sys.exit(1)

        return
        # TODO Return something?

    # Pandas DataFrame support =================================================

    def vertexSetToDataFrame(self, vertexSet: list, withId: bool = True,
            withType: bool = False) -> pd.DataFrame:
        """Converts a vertex set to Pandas DataFrame.

        Vertex sets are used for both the input and output of ``SELECT`` statements. They contain
        instances of vertices of the same type.
        For each vertex instance the vertex ID, the vertex type and the (optional) attributes are
        present (under ``v_id``, ``v_type`` and ``attributes`` keys, respectively).
        See an example in ``edgeSetToDataFrame()``.

        A vertex set has this structure (when serialised as JSON):

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

        Documentation:
            https://docs.tigergraph.com/gsql-ref/current/querying/declaration-and-assignment-statements#_vertex_set_variables
            https://docs.tigergraph.com/gsql-ref/current/querying/output-statements-and-file-objects#_examples_of_printing_various_data_types

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

    def edgeSetToDataFrame(self, edgeSet: list, withId: bool = True,
            withType: bool = False) -> pd.DataFrame:
        """Converts an edge set to Pandas DataFrame

        Edge sets contain instances of the same edge type. Edge sets are not generated "naturally"
        like vertex sets, you need to collect edges in (global) accumulators, e.g. in case you want
        to visualise them in GraphStudio or by other tools.

        For example:
            SetAccum<EDGE> @@edges;

            start = {Country.*};

            result =
                SELECT t
                FROM   start:s -(PROVINCE_IN_COUNTRY:e)- Province:t
                ACCUM  @@edges += e;

            PRINT start, result, @@edges;

        The ``@@edges`` is an edge set.
        It contains for each edge instance the source and target vertex type and ID, the edge type,
        an directedness indicator and the (optional) attributes.
        Note: ``start`` and ``result`` are vertex sets.

        An edge set has this structure (when serialised as JSON):
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

        Documentation:
            https://docs.tigergraph.com/gsql-ref/current/querying/declaration-and-assignment-statements#_vertex_set_variables

        Args:
            edgeSet:
                A JSON array containing an edge set in the format returned by queries (see below).
            withId:
                Include the type and primary ID of source and target vertices as a columns?
            withType:
                Include edge type info as a column?

        Returns:
            A pandas DataFrame containing the edge attributes (and optionally the type and primary
            ID or source and target vertices, and the edge type).

        """
        df = pd.DataFrame(edgeSet)
        cols = []
        if withId:
            cols.extend([df["from_type"], df["from_id"], df["to_type"], df["to_id"]])
        if withType:
            cols.append(df["e_type"])
        cols.append(pd.DataFrame(df["attributes"].tolist()))
        return pd.concat(cols, axis=1)

    def upsertVertexDataFrame(self, df: pd.DataFrame, vertexType: str, v_id: bool = None,
            attributes: str = "") -> int:
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
                A dictionary in the form of {target: source} where source is the column name in the
                dataframe and target is the attribute name in the graph vertex. When omitted, all
                columns would be upserted with their current names. In this case column names must
                match the vertex's attribute names.

        Returns:
            The number of vertices upserted.
        """

        json_up = []

        for index in df.index:
            json_up.append(json.loads(df.loc[index].to_json()))
            json_up[-1] = (
                index if v_id is None else json_up[-1][v_id],
                json_up[-1] if attributes is None
                else {target: json_up[-1][source]
                    for target, source in attributes.items()}  # TODO ["items"]
            )

        return self.upsertVertices(vertexType=vertexType, vertices=json_up)

    def upsertEdgeDataFrame(self, df: pd.DataFrame, sourceVertexType: str, edgeType: str,
            targetVertexType: str, from_id: str = "", to_id: str = "",
            attributes: str = None) -> int:
        """Upserts edges from a Pandas DataFrame.

        Args:
            df:
                The DataFrame to upsert.
            sourceVertexType:
                The type of source vertex for the edge.
            edgeType:
                The type of edge to upsert data to.
            targetVertexType:
                The type of target vertex for the edge.
            from_id:
                The field name where the source vertex primary id is given. If omitted, the
                dataframe index would be used instead.
            to_id:
                The field name where the target vertex primary id is given. If omitted, the
                dataframe index would be used instead.
            attributes:
                A dictionary in the form of {target: source} where source is the column name in the
                dataframe and target is the attribute name in the graph vertex. When omitted, all
                columns would be upserted with their current names. In this case column names must
                match the vertex's attribute names.

        Returns:
            The number of edges upserted.
        """

        json_up = []

        for index in df.index:
            json_up.append(json.loads(df.loc[index].to_json()))
            json_up[-1] = (
                index if from_id is None else json_up[-1][from_id],
                index if to_id is None else json_up[-1][to_id],
                json_up[-1] if attributes is None
                else {target: json_up[-1][source]
                    for target, source in attributes.items()}  # TODO ["items"]
            )

        return self.upsertEdges(
            sourceVertexType=sourceVertexType,
            edgeType=edgeType,
            targetVertexType=targetVertexType,
            edges=json_up
        )

    # Path-finding algorithms ==================================================

    def _preparePathParams(self, sourceVertices, targetVertices, maxLength=None, vertexFilters=None,
            edgeFilters=None,
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

    def shortestPath(self, sourceVertices, targetVertices, maxLength=None, vertexFilters=None,
            edgeFilters=None,
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
        data = self._preparePathParams(sourceVertices, targetVertices, maxLength, vertexFilters,
            edgeFilters,
            allShortestPaths)
        return self._post(self.restppUrl + "/shortestpath/" + self.graphname, data=data)

    def allPaths(self, sourceVertices, targetVertices, maxLength, vertexFilters=None,
            edgeFilters=None):
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
        data = self._preparePathParams(sourceVertices, targetVertices, maxLength, vertexFilters,
            edgeFilters)
        return self._post(self.restppUrl + "/allpaths/" + self.graphname, data=data)

    # Security related functions ===============================================

    # TODO GET /showprocesslist/{graph_name}
    #       https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-running-queries-showprocesslist-graph_name

    def showSecrets(self) -> dict:
        """Issues a `CREATE SECRET` GSQL statement and returns the secret generated by that
            statement.

        Returns:
            A dictionary of <alias>: <secret_string> pairs.

        This function return the masked version of secrets. The original value of secrets cannot be
            retrieved after creation. So, this function is not very useful.

        Documentation:
            https://docs.tigergraph.com/admin/admin-guide/user-access/managing-credentials#create-a-secret
        """
        if not self.gsqlInitiated:
            self.initGsql()

        response = self.gsql("""
                USE GRAPH {}
                SHOW SECRET """.format(self.graphname))
        return response
        # TODO Process response, return a dictionary of alias/secret pairs

    # TODO showSecret()

    def createSecret(self, alias: str = "") -> str:
        """Issues a `CREATE SECRET` GSQL statement and returns the secret generated by that statement.

        Args:
            alias:
                The alias of the secret.
                Beginning with TigerGraph 3.1.4, the system will generate a random alias for the
                secret if the user does not provide an alias for that secret. Randomly generated
                aliases begin with AUTO_GENERATED_ALIAS_ and include a random 7-character string.

        Returns:
            The secret string.

        Documentation:
            https://docs.tigergraph.com/admin/admin-guide/user-access/managing-credentials#create-a-secret
        """
        if not self.gsqlInitiated:
            self.initGsql()

        response = self.gsql("""
        USE GRAPH {}
        CREATE SECRET {} """.format(self.graphname, alias))
        try:
            # print(response)
            if ("already exists" in response):
                # get the sec
                errorMsg = "The secret "
                if alias != "":
                    errorMsg += "with alias {} ".format(alias)
                errorMsg += "already exists."
                raise TigerGraphException(errorMsg, "E-00001")
            secret = "".join(response).replace('\n', '').split('The secret: ')[1].split(" ")[0]
            return secret.strip()
        except:
            raise

    # def dropSecret(self, alias: str) -> bool:
    """
        Args:
            alias:
                The alias of the secret.

    Documentation:
        https://docs.tigergraph.com/admin/admin-guide/user-access/managing-credentials#drop-a-secret
    """
    # TODO Implementation

    def getToken(self, secret: str, setToken: bool = True, lifetime: int = None) -> tuple:
        """Requests an authorization token.

        This function returns a token only if REST++ authentication is enabled. If not, an exception
        will be raised.
        See: https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication

        Args:
            secret:
                The secret (string) generated in GSQL using `CREATE SECRET`.
                See https://docs.tigergraph.com/tigergraph-server/current/user-access/managing-credentials#_create_a_secret
            setToken:
                Set the connection's API token to the new value (default: True).
            lifetime:
                Duration of token validity (in secs, default 30 days = 2,592,000 secs).

        Returns:
            A tuple of (<token>, <expiration_timestamp_unixtime>, <expiration_timestamp_ISO8601>).
            Return value can be ignored.
            Expiration timestamp's time zone might be different from your computer's local time zone.

        Raises:
            `TigerGraphException` if REST++ authentication is not enabled or authentication error
            occurred.

        Endpoint:
            GET /requesttoken
        Documentation:
            https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_request_a_token
        """
        s, m, i = (0, 0, 0)
        res = {}
        if self.version:
            s, m, i = self.version.split(".")
        success = False
        if int(s) < 3 or (int(s) >= 3 and int(m) < 5):
            try:
                if self.useCert and self.certPath:
                    res = json.loads(requests.request("GET", self.restppUrl +
                                                             "/requesttoken?secret=" + secret +
                                                             ("&lifetime=" + str(
                                                                 lifetime) if lifetime else "")).text)
                else:
                    res = json.loads(requests.request("GET", self.restppUrl +
                                                             "/requesttoken?secret=" + secret +
                                                             ("&lifetime=" + str(
                                                                 lifetime) if lifetime else ""),
                        verify=False).text)
                if not res["error"]:
                    success = True
            except:
                success = False
        if not success:
            try:
                data = {"secret": secret}

                if lifetime:
                    data["lifetime"] = str(lifetime)
                if self.useCert is True and self.certPath is not None:
                    res = json.loads(requests.post(self.restppUrl + "/requesttoken",
                        data=json.dumps(data)).text)
                else:
                    res = json.loads(requests.post(self.restppUrl + "/requesttoken",
                        data=json.dumps(data), verify=False).text)
            except:
                success = False
        if not res["error"]:
            if setToken:
                self.apiToken = res["token"]
                self.authHeader = {'Authorization': "Bearer " + self.apiToken}
            else:
                self.apiToken = None
                self.authHeader = {'Authorization': 'Basic {0}'.format(self.base64_credential)}

            return res["token"], res["expiration"], \
                datetime.utcfromtimestamp(float(res["expiration"])).strftime('%Y-%m-%d %H:%M:%S')
        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't generate token.",
                None)
        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def refreshToken(self, secret: str, token: str = "", lifetime: int = None) -> tuple:
        """Extends a token's lifetime.

        This function works only if REST++ authentication is enabled. If not, an exception will be
        raised.
        See: https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication

        Args:
            secret:
                The secret (string) generated in GSQL using `CREATE SECRET`.
                See https://docs.tigergraph.com/tigergraph-server/current/user-access/managing-credentials#_create_a_secret
            token:
                The token requested earlier. If not specified, refreshes current connection's token.
            lifetime:
                Duration of token validity (in secs, default 30 days = 2,592,000 secs) from current
                system timestamp.

        Returns:
            A tuple of (<token>, <expiration_timestamp_unixtime>, <expiration_timestamp_ISO8601>).
            Return value can be ignored.
            Expiration timestamp's time zone might be different from your computer's local time zone.
            New expiration timestamp will be now + lifetime seconds, _not_ current expiration
            timestamp + lifetime seconds.

        Raises:
            `TigerGraphException` if REST++ authentication is not enabled or authentication error
            occurred, e.g. specified token does not exists.

        Note:

        Endpoint:
            PUT /requesttoken
        Documentation:
            https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_refresh_a_token
        TODO Rework lifetime parameter handling the same as in getToken()
        """
        if not token:
            token = self.apiToken
        if self.useCert and self.certPath:
            res = json.loads(requests.request("PUT", self.restppUrl + "/requesttoken?secret=" +
                                                     secret + "&token=" + token +
                                                     ("&lifetime=" + str(
                                                         lifetime) if lifetime else ""),
                verify=False).text)
        else:
            res = json.loads(requests.request("PUT", self.restppUrl + "/requesttoken?secret=" +
                                                     secret + "&token=" + token +
                                                     ("&lifetime=" + str(
                                                         lifetime) if lifetime else "")).text)
        if not res["error"]:
            exp = time.time() + res["expiration"]
            return res["token"], int(exp), datetime.utcfromtimestamp(exp).strftime(
                '%Y-%m-%d %H:%M:%S')
        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't refresh token.",
                None)
        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def deleteToken(self, secret, token=None, skipNA=True):
        """Deletes a token.

        This function works only if REST++ authentication is enabled. If not, an exception will be
        raised.
        See: https://docs.tigergraph.com/tigergraph-server/current/user-access/enabling-user-authentication#_enable_restpp_authentication

        Args:
            secret:
                The secret (string) generated in GSQL using `CREATE SECRET`.
                See https://docs.tigergraph.com/tigergraph-server/current/user-access/managing-credentials#_create_a_secret
            token:
                The token requested earlier. If not specified, deletes current connection's token,
                so be careful.
            skipNA:
                Don't raise exception if specified token does not exist.

        Returns:
            `True` if deletion was successful, or token did not exist but `skipNA` was `True`.

        Raises:
            `TigerGraphException` if REST++ authentication is not enabled or authentication error
            occurred, e.g. specified token does not exists.

        Endpoint:
            DELETE /requesttoken
        Documentation:
            https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_delete_a_token
        """
        if not token:
            token = self.apiToken
        if self.useCert is True and self.certPath is not None:
            res = json.loads(
                requests.request("DELETE",
                    self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token,
                    verify=False).text)
        else:
            res = json.loads(
                requests.request("DELETE",
                    self.restppUrl + "/requesttoken?secret=" + secret + "&token=" + token).text)
        if not res["error"]:
            return True
        if res["code"] == "REST-3300" and skipNA:
            return True
        if "Endpoint is not found from url = /requesttoken" in res["message"]:
            raise TigerGraphException("REST++ authentication is not enabled, can't delete token.",
                None)
        raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    # Other functions ==========================================================

    def echo(self, usePost: bool = False) -> str:
        """Pings the database.

        Args:
            usePost:
                Use POST instead of GET

        Returns:
            "Hello GSQL" if everything was OK.

        Endpoint:
            GET /echo
            POST /echo
        Documentation:
            https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_echo

        TODO Implement POST
        """
        if usePost:
            return self._post(self.restppUrl + "/echo/" + self.graphname, resKey="message")
        return self._get(self.restppUrl + "/echo/" + self.graphname, resKey="message")

    def getEndpoints(self, builtin: bool = False, dynamic: bool = False,
            static: bool = False) -> dict:
        """Lists the REST++ endpoints and their parameters.

        Args:
            builtin:
                List TigerGraph provided REST++ endpoints.
            dynamic:
                List endpoints for user installed queries.
            static:
                List static endpoints.

        If none of the above arguments are specified, all endpoints are listed

        Endpoint:
            GET /endpoints/{graph_name}
        Documentation:
            https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_list_all_endpoints
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
            res = self._get(url + "builtin=true", resKey="")
            for ep in res:
                if not re.search(" /graph/", ep) or re.search(" /graph/{graph_name}/", ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if dyn:
            eps = {}
            res = self._get(url + "dynamic=true", resKey="")
            for ep in res:
                if re.search("^GET /query/" + self.graphname, ep):
                    eps[ep] = res[ep]
            ret.update(eps)
        if sta:
            ret.update(self._get(url + "static=true", resKey=""))
        return ret

    def getStatistics(self, seconds: int = 10, segments: int = 10) -> dict:
        """Retrieves real-time query performance statistics over the given time period.

        Args:
            seconds:
                The duration of statistic collection period (the last n seconds before the function
                call).
            segments:
                The number of segments of the latency distribution (shown in results as
                LatencyPercentile). By default, segments is 10, meaning the percentile range 0-100%
                will be divided into ten equal segments: 0%-10%, 11%-20%, etc.
                Segments must be [1, 100].

        Endpoint:
            GET /statistics/{graph_name}
        Documentation:
            https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_show_query_performance
        """
        if not seconds:
            seconds = 10
        else:
            seconds = max(min(seconds, 0), 60)
        if not segments:
            segment = 10
        else:
            segment = max(min(segments, 0), 100)
        return self._get(self.restppUrl + "/statistics/" + self.graphname + "?seconds=" +
                         str(seconds) + "&segment=" + str(segments), resKey="")

    def getVersion(self, raw: bool = False) -> [str, list]:
        """Retrieves the git versions of all components of the system.

        Args:
            raw:
                Return unprocessed version info string, or extract version info for each components
                into a list.

        Returns:
            Either an unprocessed string containing the version info details, or a list with version
            info for each components.

        Endpoint:
            GET /version
        Documentation:
            https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_show_component_versions
        """
        if self.useCert and self.certPath:
            response = requests.request("GET", self.restppUrl + "/version/" + self.graphname,
                headers=self.authHeader, verify=False)
        else:
            response = requests.request("GET", self.restppUrl + "/version/" + self.graphname,
                headers=self.authHeader)
        res = json.loads(response.text, strict=False)  # "strict=False" is why _get() was not used
        self._errorCheck(res)

        if raw:
            return response.text
        res = res["message"].split("\n")
        components = []
        for i in range(len(res)):
            if 2 < i < len(res) - 1:
                m = res[i].split()
                component = {"name": m[0], "version": m[1], "hash": m[2],
                    "datetime": m[3] + " " + m[4] + " " + m[5]}
                components.append(component)
        return components

    def getVer(self, component: str = "product", full: bool = False) -> str:
        """Gets the version information of specific component.

        Args:
            component:
                One of TigerGraph's components (e.g. product, gpe, gse).
            full:
                Return the full version string (with timestamp, etc.) or just X.Y.Z.

        Returns:
            Version info for specified component.

        Raises:
            TigerGraphException if invalid/non-existent component is specified.

        Get the full list of components using `getVersion`.
        """
        ret = ""
        for v in self.getVersion():
            if v["name"] == component.lower():
                ret = v["version"]
        if ret != "":
            if full:
                return ret
            ret = re.search("_.+_", ret)
            return ret.group().strip("_")
        else:
            raise TigerGraphException("\"" + component + "\" is not a valid component.", None)

    def getLicenseInfo(self) -> dict:
        """Returns the expiration date and remaining days of the license.

        Returns:
            In case of evaluation/trial deployment, an information message and -1 remaining days are
            returned; otherwise the license details.

        TODO Check if this endpoint was still available.
        """
        res = self._get(self.restppUrl + "/showlicenseinfo", resKey="", skipCheck=True)
        ret = {}
        if not res["error"]:
            ret["message"] = res["message"]
            ret["expirationDate"] = res["results"][0]["Expiration date"]
            ret["daysRemaining"] = res["results"][0]["Days remaining"]
        elif "code" in res and res["code"] == "REST-5000":
            ret["message"] = \
                "This instance does not have a valid enterprise license. Is this a trial version?"
            ret["daysRemaining"] = -1
        else:
            raise TigerGraphException(res["message"], res["code"])
        return ret

    def uploadFile(self, filePath, fileTag, jobName="", sep=None, eol=None, timeout=16000,
            sizeLimit=128000000):
        """DDL Upload File .

        Endpoint:      POST /graph

        Arguments:
        - `filePath`:   File variable name or file path for the file containing the data
        - `fileTag`:    Name of file variable in DDL loading job
        - `jobName`:    Loading job name defined in your DDL loading job
        - `sep`:        Separator of CSV data. If your data is JSON, you do not need to specify this parameter. The default separator is a comma","
        - `eol`:        End-of-line character. Only one or two characters are allowed, except for the special case "\r\n". The default value is "\n"
        - `timeout`:    Timeout in seconds. If set to 0, use system-wide endpoint timeout setting.
        - `sizeLimit`:  Maximum size for input file

        """
        try:
            data = open(filePath, 'rb').read()
            params = {
                "tag": jobName,
                "filename": fileTag,
            }
            if sep != None:
                params["sep"] = sep
            if eol != None:
                params["eol"] = eol
        except:
            return None
        return self._post(self.restppUrl + "/ddl/" + self.graphname, params=params, data=data,
            headers={"RESPONSE-LIMIT": str(sizeLimit), "GSQL-TIMEOUT": str(timeout)})

# EOF
