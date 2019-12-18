# TigerGraph Driver
Python wrapper for querying TigerGraph

## Usage
First, add TigerGraphDriver.py into your project's directory.
### Initialization
There are a few parameters in order to instantiate a connection to your TigerGraph instance.  These include:
* URL of the server (defaults to http://localhost)
* Name of the Graph (defaults to MyGraph)
* Username and Password (both default to tigergraph)
* The API call port (default 9000)
* The port for Interpreted Queries (default 14240)
* The API Token (Can be obtained with the getToken() method)

To instantiate the connection, simply use:
```py
graph = tg.TigerGraphConnection(ipAddress="https://YOUR_URL_HERE", apiToken="YOUR_TOKEN_HERE")
```

### Other Methods
#### runInstalledQuery(queryName, params)
Assumes there is a query installed on the server, takes its name as an argument as a string, and the needed parameters as a dictionary. Returns JSON.

#### getEndpoints()
Returns the JSON of all possible endpoints on the server

#### getToken(secret, lifetime)
Returns a API token given a secret key and lifetime of the token, which are both strings.

## TODO:
#### runInterpretedQuery(query) 
Currently does not work, due to what appears to be an internal server issue.
