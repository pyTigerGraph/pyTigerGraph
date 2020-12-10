import pyTigerGraph as tg

conn = tg.TigerGraphConnection(host="https://medzrouga.i.tgcloud.io", graphname="MyGraph", username="tigergraph",password="tigergraph", apiToken="3mva82884g93sofo5s8tksrnoo7il9v5")

class TestpyTigerGraph:

    def testgsql(self):
        assert True == True

    def testgetVer(self):
        assert "3.0.5" == conn.getVer()
