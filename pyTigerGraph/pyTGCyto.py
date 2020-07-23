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
        
    def getVertices(self, vertexType, labelAttr="", ids=None, select="", where="", limit="", sort="", timeout=0):
        conn = self.tgconn
        v_ids = []
        vertices = []
        if ids:
            vertices = conn.getVerticesById(vertexType, ids)
        else:
            vertices = conn.getVertices(vertexType, select, where, limit, sort, timeout)
        name = ""
        v_ids = []
        for vertex in vertices:
            v_ids.append(vertex['v_id'])
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
        return v_ids

    def getEdgesByType(self, edgeType, sourceVertices=None):
        conn = self.tgconn
        v_ids = set()
        edges = []
        if sourceVertices:
            vt = conn.getEdgeSourceVertexType(edgeType)
            for sc in sourceVertices:
                edges.append(conn.getEdges(vt, sc, edgeType)[0])
        else:
            edges = conn.getEdgesByType(edgeType)
        eId = 0
        for edge in edges:
            cedge = {'data': {'id': edge['e_type'] + self.sep + str(eId), 'source': edge['from_type'] + self.sep + edge['from_id'], 'target': edge['to_type'] + self.sep + edge['to_id']}}
            self.data["edges"].append(cedge)
            eId += 1
            v_ids.add(edge['to_id'])
        return list(v_ids)
        
    def getData(self):
        return self.data