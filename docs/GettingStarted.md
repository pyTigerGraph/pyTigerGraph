# Getting Started
To download pyTigerGraph, simply run:
```pip install pyTigerGraph```
Once the package installs, you can import it and instantiate a connection to your database:
```py
import pyTigerGraph as tg

conn = tg.TigerGraphConnection(host="<hostname>", graphname="<graph_name>", username="<username>", password="<password>", apiToken="<api_token>")
```
If your database is not using the standard ports (or they are mapped), you can use the following arguments to specify those:
- restppPort (default 9000): [REST++ API port](https://docs.tigergraph.com/dev/restpp-api/restpp-requests)
- gsPort (default: 14240): [GraphStudio port](https://docs.tigergraph.com/ui/graphstudio/overview#TigerGraphGraphStudioUIGuide-GraphStudioOn-Premises)

For example, in case of using a local virtual machine with the ports mapped:
```py
conn = tg.TigerGraphConnection(host="localhost", restppPort=25900, gsPort=25240, graphname="MyGraph", username="tigergraph", password="tigergraph", apiToken="2aa016d747ede9gg6da3drslm98srfoj")
```