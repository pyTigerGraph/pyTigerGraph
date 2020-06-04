import pandas as pd
import pyTigerGraph

class graphToDataFrame():
    def __init__(self, connection):
        self.connection = connection
    
    def getVerticieDataframe(self, vertexType, select="", where="", limit="", sort="", timeout=0):
        data = self.connection.getVertices(vertexType, select=select, where=where, limit=limit, sort=sort, timeout=timeout)
        df = pd.DataFrame(data)
        df = pd.concat([df.drop("attributes", axis=1), pd.DataFrame(df["attributes"].tolist())], axis=1)
        return df

    def getEdgesDataframe(self, sourceVertexType, sourceVerticies, edgeType=None, targetVertexType=None, targetVertexId=None, select="", where="", limit="", sort="", timeout=0):
        frames = []
        for vertex in sourceVerticies:
            data = self.connection.getEdges(sourceVertexType, vertex, edgeType, targetVertexType, targetVertexId, select, where, limit, sort, timeout)
            df = pd.DataFrame(data)
            frames.append(pd.concat([df.drop("attributes", axis=1), pd.DataFrame(df["attributes"].tolist())], axis=1))
        return pd.concat(frames).reset_index().drop("index", axis=1)

    