import pandas as pd
import pyTigerGraph

class Node():
    def __init__(self, nodeId, nodeNum, nodeType, features):
        self.id = nodeId
        self.num = nodeNum
        self.nodeType = nodeType
        self.features = features
        self.edges = []
    
    def getEdges(self):
        return self.edges
    
    def addEdge(self, edge):
        self.edges.append(edge)


class Edge():
    def __init__(self, edgeId, edgeType, features, fromNode, toNode):
        self.edgeId = edgeId
        self.edgeType = edgeType
        self.features = features
        self.fromNode = fromNode
        self.toNode = toNode

class mlGraph():
    def __init__(self, connection):
        assert isinstance(connection, pyTigerGraph.TigerGraphConnection), "Must pass in a TigerGraphConnection"
        self.conn = connection
        self.nodes = {}
        self.edges = []
        self.nodeCounter = 0

    def getFeatures(self, nodeId, nodeType):
        return [1 for i in range(0, 20)]

    def registerNode(self, nodeId, nodeType):
        if nodeId not in self.nodes.keys():
            self.nodes[nodeId] = Node(nodeId, self.nodeCounter, nodeType, self.getFeatures(nodeId, nodeType))
            self.nodeCounter += 1
        return self.nodes[nodeId]

    def addEdge(self, edge):
        fromNode = self.registerNode(edge["from_id"], edge["from_type"])
        toNode = self.registerNode(edge["to_id"], edge["to_type"])
        edgeId = edge["from_id"] + edge["to_id"]
        edge = Edge(edgeId, edge["e_type"], edge["attributes"], fromNode, toNode)
        self.edges.append(edge)

    def _pullSubGraph(self, sourceVertexType, sourceVertexId, limit = "", edgeTypes = None):
        if edgeTypes != None:
            for edgeType in edgeTypes:
                response = self.conn.getEdges(sourceVertexType, sourceVertexId, edgeType=edgeType, limit=limit)
        else:
            response = self.conn.getEdges(sourceVertexType, sourceVertexId, limit=limit)
        for edge in response:
            self.addEdge(edge)
        
    def createGraph(self, sourceVertexType, sourceVertexId, depth = 1, limit = "", edgeTypes = None):
        self._pullSubGraph(sourceVertexType, sourceVertexId, limit, edgeTypes)
        depth -= 1
        while depth > 0:
            for nodeId in self.nodes.keys():
                self._pullSubGraph(self.nodes[nodeId].nodeType, nodeId, limit, edgeTypes)
            depth -= 1
        
        return len(self.nodes.keys())
