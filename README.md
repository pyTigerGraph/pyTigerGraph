# Getting Started
To download pyTigerGraph, simply run:
```pip install pyTigerGraph```
Once the package installs, we can import it and instantiate a connection to your database:
```py
import pyTigerGraph as tg

conn = tg.TigerGraphConnection(ipAddress="YOUR_URL_HERE", graphname="YOUR_GRAPH_NAME", username="YOUR_USERNAME", password="YOUR_PASSWORD", apiToken="YOUR_API_TOKEN_HERE")
```
There are a few more options when instantiating the connection that aren't in the example above. They are:
* apiPort (default 9000): This should be changed if your server has been configured to use a different port for the REST++ endpoint service that TigerGraph provides
* interpreterPort (default 14240): This should be changed if your server has been configured to use a different port for GraphStudio

The username and password default to the TigerGraph default username and password, which is tigergraph. The API token can be obtained via the method described below.
# The Methods
Once you have the package installed and the connection instantiated, you are all set to use the provided methods. There are currently only three methods, but more are on the way.
## getToken("SECRET", "LIFETIME")
This method gets an API token given a secret key and the desired lifetime of the token, in seconds. When instantiating a connection to use this method, simply leave the apiToken field blank, so it looks like this:
```py

import pyTigerGraph as tg

conn = tg.TigerGraphConnection(ipAddress="YOUR_URL_HERE", graphname="YOUR_GRAPH_NAME", username="YOUR_USERNAME", password="YOUR_PASSWORD")

print(conn.getToken("YOUR_SECRET_HERE", "1000000")) #uses a lifetime of 1,000,000 seconds
```

## runInstalledQuery("queryName", {params}, timeout, sizeLimit)
This method runs a query installed on the database and returns the JSON response. The query name is a string and then a dictionary of parameters. Once you instantiate a connection, the code looks something like this:
```py
params = {"vid":"Jazz", "vid.type":"Article"} #query's arguments
queryName = "getKeywords"

preInstalledResult = conn.runInstalledQuery(queryName, params) 

print(preInstalledResult)
```
The timeout and sizeLimit parameters are optional, and should be passed in as integers. The default timeout is 16 seconds and the default sizeLimit is 320000 bytes.

## getEndpoints()
This method returns a JSON response of all possible endpoints on the server. To run, simply:
```py
endpoints = conn.getEndpoints()

print(endpoints)
```
