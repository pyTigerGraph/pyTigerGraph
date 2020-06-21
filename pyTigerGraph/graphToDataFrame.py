import pandas as pd
import pyTigerGraph

class graphToDataFrame():
    def __init__(self, connection):
        assert isinstance(connection, pyTigerGraph.TigerGraphConnection), "Must pass in a TigerGraphConnection"
        self.connection = connection
    
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
        data = self.connection.getVertices(vertexType, select=select, where=where, limit=limit, sort=sort, timeout=timeout)
        df = pd.DataFrame(data)
        df = pd.concat([df.drop("attributes", axis=1), pd.DataFrame(df["attributes"].tolist())], axis=1)
        return df

    def getVertexDataframeByID(self, vertexType, vertexIds):
        """Retrieves vertices of the given vertex type, identified by their ID.

        Arguments
        - `vertexType`: Type of vertex desired
        - `vertexIds`: A single vertex ID or a list of vertex IDs.

        Endpoint:      GET /graph/{graph_name}/vertices
        Documentation: https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-graph-graph_name-vertices
        """
        data = self.connection.getVerticesById(vertexType, vertexIds)
        df = pd.DataFrame(data)
        df = pd.concat([df.drop("attributes", axis=1), pd.DataFrame(df["attributes"].tolist())], axis=1)
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
            data = self.connection.getEdges(sourceVertexType, vertex, edgeType, targetVertexType, targetVertexId, select, where, limit, sort, timeout)
            df = pd.DataFrame(data)
            try:
                frames.append(pd.concat([df.drop("attributes", axis=1), pd.DataFrame(df["attributes"].tolist())], axis=1))
            except:
                frames.append(df)
        return pd.concat(frames).reset_index().drop("index", axis=1)

    def getInstalledQueriesDataframe(self):
        """
        Returns dataframe of all installed queries. Does not take any arguments
        """
        data = self.connection.getEndpoints(dynamic=True)
        df = pd.DataFrame(data).T
        return df