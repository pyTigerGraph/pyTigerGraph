"""
pyTigerGraph (unit)tests

This script will walk through all of the features that pyTigerGraph offers. Before you get started
you will need a TigerGraph instance of your own. You can
 - spin one up yourself (https://dl.tigergraph.com/download.html)
 or
 - use one of our free cloud instances (https://tgcloud.io/).

We'll be using the Recommendation Engine (Movie Recommendation) example. If you're using TigerGraph
Cloud, you can select this sample when creating your cloud solution. Otherwise, you can download the
sample (https://www.tigergraph.com/starterkits/) for your installed version of TigerGraph.

Don't load data; this script will create the necessary data.
"""
import pyTigerGraph.pyTigerGraph as tg
import json
import pandas as pd
import argparse

sep = "\n" + ("-" * 80)


def print_sep(label: str):
    print(sep)
    print(label + "\n")


parser = argparse.ArgumentParser("pyTigerGraph (unit)tests")
parser.add_argument("-H", "--host", help="Host name or IP")
parser.add_argument("-g", "--graph", help="graph name", default="MyGraph")
parser.add_argument("-s", "--secret", help="Secret")
parser.add_argument("-u", "--user", help="Username", default="tigergraph")
parser.add_argument("-p", "--password", help="Password", default="tigergraph")
args = parser.parse_args()

hostName = args.host
graphName = args.graph
secret = args.secret
userName = args.user
password = args.password

conn = tg.TigerGraphConnection(host=hostName, graphname=graphName, username=userName,
    password=password)
authToken = conn.getToken(secret)[0]
# conn.debug = True

print_sep("getSchema()")
results = conn.getSchema()
print(json.dumps(results, indent=2))

print_sep("getEdgeTypes()")
results = conn.getVertexTypes()
print(json.dumps(results, indent=2))

print_sep("getEdgeTypes()")
results = conn.getEdgeTypes()
print(json.dumps(results, indent=2))

print_sep("getVertexType(\"person\")")
results = conn.getVertexType("person")
print(json.dumps(results, indent=2))

print_sep("getEdgeType(\"rate\")")
results = conn.getEdgeType("rate")
print(json.dumps(results, indent=2))

print_sep("getEdgeSourceVertexType(\"rate\")")
results = conn.getEdgeSourceVertexType("rate")
print(json.dumps(results, indent=2))

print_sep("getEdgeTargetVertexType(\"rate\")")
results = conn.getEdgeTargetVertexType("rate")
print(json.dumps(results, indent=2))

print_sep("isDirected(\"rate\")")
results = conn.isDirected("rate")
print(json.dumps(results, indent=2))

print_sep("getReverseEdge(\"rate\")")
reverseEdge = conn.getReverseEdge("rate")
print(json.dumps(results, indent=2))

print_sep("Fetching data")
numPeople = conn.getVertexCount("person")
numMovies = conn.getVertexCount("movie")
numEdges = conn.getEdgeCount("rate")

limit = 5
people = conn.getVertices("person", limit=limit)
movies = conn.getVertices("movie", limit=limit)
# edges = conn.getEdgesByType("rate", limit=limit)

print(
    f"There are currently {numPeople} people, {numMovies} movies, and {numEdges} edges connecting them")
print(f"Sample of people: {json.dumps(people, indent=2)}")
print(f"Sample of movies: {json.dumps(movies, indent=2)}")
# print(f"Our edges are: {json.dumps(edges, indent=2)}")

print_sep("Adding data")
results = conn.upsertVertex("person", "Dan", {})
print(results)

attributes = {"title": "Die Hard 4: Live Free or Die Hard", "genres": "action"}
results = conn.upsertVertex("movie", "1", attributes)
print(results)

attributes = {"rating": 8.6, "rated_at": "2016-05-07 23:43:11"}
results = conn.upsertEdge("person", "Dan", "rate", "movie", "1", attributes)
print(results)

# Vertex format [(PrimaryId, {attributes})]
people = [
    ("Ben", {}),
    ("Nick", {}),
    ("Leena", {})
]
movies = [
    (2, {"title": "Inception", "genres": "action|thriller"}),
    (3, {"title": "Her", "genres": "comedy|romance|drama"}),
    (4, {"title": "Ferris Bueller's Day Off", "genres": "comedy"})
]
# Edge format [(SourcePrimaryId, TargetPrimaryId, {attributes})]
ratings = [
    ("Ben", 2, {"rating": 7.3, "rated_at": "2018-11-02 14:22:45"}),
    ("Nick", 4, {"rating": 9.2, "rated_at": "2015-01-08 12:35:16"}),
    ("Nick", 3, {"rating": 8.7, "rated_at": "2016-09-02 10:48:12"}),
]

results = conn.upsertVertices("person", people)
print(results)

results = conn.upsertVertices("movie", movies)
print(results)

results = conn.upsertEdges("person", "rate", "movie", ratings)
print(results)

results = conn.getVerticesById("person", "Dan")
print(json.dumps(results, indent=2))

results = conn.getVerticesById("movie", ["2", "4"])
print(json.dumps(results, indent=2))

print_sep("Getting stats")
results = conn.getVertexStats("person")
print(json.dumps(results, indent=2))

results = conn.getVertexStats("movie")
print(json.dumps(results, indent=2))

results = conn.getEdgeStats("rate")
print(json.dumps(results, indent=2))

results = conn.getVertexStats("*", skipNA=True)
print(json.dumps(results, indent=2))

print_sep("Getting counts")
results = conn.getEdgeCountFrom("person", "Nick")
print(json.dumps(results, indent=2))

results = conn.getEdgeCountFrom("person", "Nick", edgeType="rate")
print(json.dumps(results, indent=2))

results = conn.getEdges("person", "Nick")
print(json.dumps(results, indent=2))

print_sep("Deleting data")
results = conn.delVerticesById("person", "Leena")
print(json.dumps(results, indent=2))

# Stats before deleting ratings below 9.0
results = conn.getEdgeStats("rate")
print(json.dumps(results, indent=2))
results = conn.getEdgeCount(edgeType="rate")
print(json.dumps(results, indent=2))

# Deleting ratings below 9.0
results = conn.delEdges("person", "Nick", edgeType="rate", where="rating < 9.0")
print(json.dumps(results, indent=2))

# Stats after deleting ratings below 9.0
results = conn.getEdgeStats("rate")
print(json.dumps(results, indent=2))
results = conn.getEdgeCount(edgeType="rate")
print(json.dumps(results, indent=2))

# Get all 'rate' edges
edges = conn.getEdgesByType("rate")

# Delete any edges with a rating less than 9.0
for edge in edges:
    if edge["attributes"]["rating"] < 9.0:
        rating = edge["attributes"]["rating"]
        fromPerson = edge["from_id"]
        deleted = conn.delEdges("person", edge["from_id"])
        print(f"Deleting a rating of {rating} from {fromPerson}")

print("-------------")
results = conn.getEdgeStats("rate")
edgeCnt = conn.getEdgeCount(edgeType="rate")
print("After deleting ratings less than '9.0'")
print(f"{edgeCnt} 'rate' edges")
print(json.dumps(results, indent=2))

# To be continued...

# EOF
