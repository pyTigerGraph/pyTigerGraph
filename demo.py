import pyTigerGraph as tg

graph = tg.TigerGraphConnection(ipAddress="YOUR_URL_HERE", apiToken="YOUR_API_TOKEN_HERE")

""" Creates an Auth Token
authToken = graph.getToken("YOUR SECRET GOES HERE", "1000000")

print(authToken)
"""


print(graph.getEndpoints()) #prints all possible endpoints on the server

preInstalledResult = graph.runInstalledQuery("getKeywords", {"vid":"Jazz", "vid.type":"Article"}) #example of running a pre-installed query

print(preInstalledResult["results"])

print(graph.getSchema())


""" Interpreted Queries - Work in Progress
query = "INTERPRET QUERY getArticle () FOR GRAPH MyGraph {start = {Article.*}; result = SELECT all FROM start-()->Article:all; PRINT result;}"

interpretedResult = graph.runInterpretedQuery(query)

print(interpretedResult)
"""


