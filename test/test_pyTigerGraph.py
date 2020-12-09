import pyTigerGraph as tg

conn = tg.TigerGraphConnection(host="https://<BOX>", graphname="MyGraph", username="tigergraph",
                               password="tigergraph", apiToken="<api_token>")

class TestpyTigerGraph:

    def testgsql(self):
        assert True == conn.gsql("LS")

    def testgetVer(self):
        assert False == conn.getVer()
