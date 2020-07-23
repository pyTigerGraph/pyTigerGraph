import pyTigerGraph

class pyTGCyto: 
    """Cytoscape/ipyCytoscape support."""
    
    def _initData(self):
        return {"nodes": [], "edges": []}
    
    def __init__(self, tgconn):
        assert isinstance(tgconn, pyTigerGraph.TigerGraphConnection), "Connection must be a TigerGraphConnection"
        self.tgconn = tgconn
        self.data = self._initData()
        self.sep = "→"
        
    def clearData(self):
        self.data = self._initData()
        
    def getVertices(self, vertexType, labelAttr="", select="", where="", sort="", limit="", timeout=0):
        vertices = self.tgconn.getVertices(vertexType, select, where, sort, limit, timeout)
        name = ""
        for vertex in vertices:
            if labelAttr == 'v_id' or not labelAttr:
                name = vertex['v_id']
            else:
                if labelAttr in vertex['attributes']:
                    name = vertex['attributes'][labelAttr]
                else:
                    name = vertex['v_id']
            node = {'data': {'id': vertex['v_type'] + self.sep + vertex['v_id'], 'name': name, 'v_type': vertex['v_type']}}
            # TODO: add other attributes as well
            self.data["nodes"].append(node)

    def getEdgesByType(self, edgeType):
        eId = 0
        edges = self.tgconn.getEdgesByType(edgeType)
        for edge in edges:
            cedge = {'data': {'id': edge['e_type'] + self.sep + str(eId), 'source': edge['from_type'] + self.sep + edge['from_id'], 'target': edge['to_type'] + self.sep + edge['to_id']}}
            self.data["edges"].append(cedge)
            eId += 1
        
    def getData(self):
        return self.data