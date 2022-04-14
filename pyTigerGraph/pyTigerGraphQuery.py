"""Query Functions."""

import json
import urllib
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd

from pyTigerGraph.pyTigerGraphException import TigerGraphException
from pyTigerGraph.pyTigerGraphSchema import pyTigerGraphSchema
from pyTigerGraph.pyTigerGraphUtils import pyTigerGraphUtils


class pyTigerGraphQuery(pyTigerGraphUtils, pyTigerGraphSchema):
    """Query Functions."""

    # TODO getQueries()  # List _all_ query names

    def getInstalledQueries(self, fmt: str = "py") -> [dict, json, pd.DataFrame]:
        """Returns a list of installed queries.

        Args:
            fmt:
                Format of the results:
                - "py":   Python objects (default)
                - "json": JSON document
                - "df":   pandas DataFrame

        Returns:
            The names of the installed queries.

        TODO This function returns all (installed and non-installed) queries
             Modify to return only installed ones
        TODO Return with query name as key rather than REST endpoint as key?
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

    def _parseQueryParameters(self, params: dict) -> str:
        """Parse a dictionary of query parameters and convert them to query string.

        While most of the values provided for various query parameter types can be easily converted
        to query string (key1=value1&key2=value2), SET and BAG parameter types, and especially
        VERTEX and SET<VERTEX> (i.e. vertex primary ID types without vertex type specification)
        require special handling.
        See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_query_parameter_passing

        TODO Accept this format for SET<VERTEX>:
            "key": [([p_id1, p_id2, ...], "vtype"), ...]
            I.e. multiple primary IDs of the same vertex type
        """
        ret = ""
        for k, v in params.items():
            if isinstance(v, tuple):
                if len(v) == 2 and isinstance(v[1], str):
                    ret += k + "=" + str(v[0]) + "&" + k + ".type=" + self._safeChar(v[1]) + "&"
                else:
                    raise TigerGraphException(
                        "Invalid parameter value: (vertex_primary_id, vertex_type) was expected.")
            elif isinstance(v, list):
                i = 0
                for vv in v:
                    if isinstance(vv, tuple):
                        if len(vv) == 2 and isinstance(vv[1], str):
                            ret += k + "[" + str(i) + "]=" + self._safeChar(vv[0]) + "&" + \
                                   k + "[" + str(i) + "].type=" + vv[1] + "&"
                        else:
                            raise TigerGraphException(
                                "Invalid parameter value: (vertex_primary_id, vertex_type) was expected.")
                    else:
                        ret += k + "=" + self._safeChar(vv) + "&"
                    i += 1
            elif isinstance(v, datetime):
                ret += k + "=" + self._safeChar(v.strftime("%Y-%m-%d %H:%M:%S")) + "&"
            else:
                ret += k + "=" + self._safeChar(v) + "&"
        return ret[:-1]

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
                See below for special rules for dictionaries.
            timeout:
                Maximum duration for successful query execution (in milliseconds).
                See https://docs.tigergraph.com/tigergraph-server/current/api/#_gsql_query_timeout
            sizeLimit:
                Maximum size of response (in bytes).
                See https://docs.tigergraph.com/tigergraph-server/current/api/#_response_size
            usePost:
                The RESTPP accepts a maximum URL length of 8192 characters. Use POST if params cause
                you to exceed this limit.

        Returns:
            The output of the query, a list of output elements (vertex sets, edge sets, variables,
            accumulators, etc.

        Notes:
            When specifying parameter values in a dictionary:

            - For primitive parameter types use
                `"key": value`
            - For `SET` and `BAG` parameter types with primitive values, use
                `"key": [value1, value2, ...]`
            - For `VERTEX<type>` use
                `"key": primary_id`
            - For `VERTEX` (no vertex type specified) use
                `"key": (primary_id, "vertex_type")`
            - For `SET<VERTEX<type>>` use
                `"key": [primary_id1, primary_id2, ...]`
            - For `SET<VERTEX>` (no vertex type specified) use
                `"key": [(primary_id1, "vertex_type1"), (primary_id2, "vertex_type2"), ...]`


        Endpoints:
            - `GET /query/{graph_name}/{query_name}`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_run_an_installed_query_get
            - `POST /query/{graph_name}/{query_name}`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_run_an_installed_query_post

        TODO Specify replica: GSQL-REPLICA
        TODO Specify thread limit: GSQL-THREAD-LIMIT
        TODO Detached mode
        """
        headers = {}
        if timeout and timeout > 0:
            headers["GSQL-TIMEOUT"] = str(timeout)
        if sizeLimit and sizeLimit > 0:
            headers["RESPONSE-LIMIT"] = str(sizeLimit)

        if isinstance(params, dict):
            params = self._parseQueryParameters(params)

        if usePost:
            return self._post(self.restppUrl + "/query/" + self.graphname + "/" + queryName,
                data=params, headers=headers)
        else:
            return self._get(self.restppUrl + "/query/" + self.graphname + "/" + queryName,
                params=params, headers=headers)

    # TODO checkQueryStatus()
    # GET /query_status/{graph_name}

    # TODO getQueryResult()
    # GET /query_result/{requestid}

    def runInterpretedQuery(self, queryText: str, params: [str, dict] = None) -> list:
        """Runs an interpreted query.

        Use ``$graphname`` or ``@graphname@`` in the ``FOR GRAPH`` clause to avoid hard coding the
        name of the graph in your app; it will be replaced by the actual graph name.

        Args:
            queryText:
                The text of the GSQL query that must be provided in this format:
                ```
                INTERPRET QUERY (<params>) FOR GRAPH <graph_name> {
                    <statements>
                }
                ```
            params:
                A string of `param1=value1&param2=value2...` format or a dictionary.
                See below for special rules for dictionaries.

        Returns:
            The output of the query, a list of output elements (vertex sets, edge sets, variables,
            accumulators, etc.

        Notes:
            When specifying parameter values in a dictionary:

            - For primitive parameter types use
                `"key": value`
            - For `SET` and `BAG` parameter types with primitive values, use
                `"key": [value1, value2, ...]`
            - For `VERTEX<type>` use
                `"key": primary_id`
            - For `VERTEX` (no vertex type specified) use
                `"key": (primary_id, "vertex_type")`
            - For `SET<VERTEX<type>>` use
                `"key": [primary_id1, primary_id2, ...]`
            - For `SET<VERTEX>` (no vertex type specified) use
                `"key": [(primary_id1, "vertex_type1"), (primary_id2, "vertex_type2"), ...]`


        Endpoint:
            - `POST /gsqlserver/interpreted_query`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_run_an_interpreted_query

        TODO Add "GSQL-TIMEOUT: <timeout value in ms>" and "RESPONSE-LIMIT: <size limit in byte>"
            plus parameters if applicable to interpreted queries (see runInstalledQuery() above)
        """
        queryText = queryText.replace("$graphname", self.graphname)
        queryText = queryText.replace("@graphname@", self.graphname)
        if isinstance(params, dict):
            params = self._parseQueryParameters(params)
        return self._post(self.gsUrl + "/gsqlserver/interpreted_query", data=queryText,
            params=params, authMode="pwd")

    # TODO getRunningQueries()
    # GET /showprocesslist/{graph_name}
    # https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#list-running-queries

    # TODO GET /abortquery/{graph_name}

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
            to find out what it is actually. /
        This function "cleans" this output, separating and collecting vertices and edges in an easy
            to access way. It can also collect other output or ignore it. /
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
                            if eType["IsDirected"]:
                                config = eType["Config"]
                                rev = ""
                                if "REVERSE_EDGE" in config:
                                    rev = config["REVERSE_EDGE"]
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
            - `GET /statistics/{graph_name}`
                See https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_show_query_performance
        """
        if not seconds:
            seconds = 10
        else:
            seconds = max(min(seconds, 0), 60)
        if not segments:
            segments = 10
        else:
            segments = max(min(segments, 0), 100)
        return self._get(self.restppUrl + "/statistics/" + self.graphname + "?seconds=" +
                         str(seconds) + "&segment=" + str(segments), resKey="")
