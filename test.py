import pyTigerGraph as tg

#conn = tg.TigerGraphConnection(host="127.0.0.1", graphname="MyGraph", username="tigergraph", password="tigergraph",useCert=False)
#conn = tg.TigerGraphConnection(host="medzrouga.i.tgcloud.io",version="v3_0_5", graphname="MyGraph", username="tigergraph", password="<>",useCert=True,certPath='certificate.crt')
conn.gsql("SHOW USER")


