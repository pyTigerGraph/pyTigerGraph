# Getting Started
To download pyTigerGraph, simply run:
```pip install pyTigerGraph```
Once the package installs, you can import it and instantiate a connection to your database:
```py
import pyTigerGraph as tg

conn = tg.TigerGraphConnection(host="<hostname>", graphname="<graph_name>", username="<username>", password="<password>", apiToken="<api_token>", version="<tg_version>")
```
If your database is not using the standard ports (or they are mapped), you can use the following arguments to specify those:
- restppPort (default 9000): [REST++ API port](https://docs.tigergraph.com/dev/restpp-api/restpp-requests)
- gsPort (default: 14240): [GraphStudio port](https://docs.tigergraph.com/ui/graphstudio/overview#TigerGraphGraphStudioUIGuide-GraphStudioOn-Premises)

For example, in case of using a local virtual machine with the ports mapped:
```py
conn = tg.TigerGraphConnection(host="localhost", restppPort=25900, gsPort=25240, graphname="MyGraph", username="tigergraph", password="tigergraph", apiToken="2aa016d747ede9gg6da3drslm98srfoj")
```

# GSQL 101 With pyTigerGraph

Checkout [this](https://github.com/pyTigerGraph/pyTigerGraph/blob/master/examples/GSQL101%20-%20PyTigerGraph.ipynb) example for completing the GSQL 101 course in a Jupyter Notebook environment. Through this, you will also learn of various pyTigerGraph methods that you can use.

## TigerGraphConnection
```pyTigerGraph.TigerGraphConnection( host="http://localhost", graphname="MyGraph", username="tigergraph", password="tigergraph", restppPort="9000", gsPort="14240", apiToken="", useCert=True, clientVersion='3.0.0', secret=None)```
    Initiate a connection object.

        Arguments

        - `host`:              The ip address of the TigerGraph server.
        - `graphname`:         The default graph for running queries.
        - `username`:          The username on the TigerGraph server.
        - `password`:          The password for that user.
        - `restppPort`:        The post for REST++ queries.
        - `gsPort`:            The port of all other queries.
        - `apiToken`:          A token to use when making queries.
        - `useCert`:           True if we need to use a certificate because the server is secure (such as on TigerGraph 
                               Cloud). This needs to be False when connecting to an unsecure server such as TigerGraph Developer. 
                               When True the certificate would be downloaded when it is first needed. 
                               on the first GSQL command.
        - `version`:     Indicates which GSQL client version to download.

