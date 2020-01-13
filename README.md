# Getting Started
To download pyTigerGraph, simply run:
```pip install pyTigerGraph```
Once the package installs, we can import it and instantiate a connection to your database:
<script src="https://gist.github.com/parkererickson/99303ca26fdec11d2f6fede1a91b45bd.js"></script>

There are a few more options when instantiating the connection that aren't in the example above. They are:
* apiPort (default 9000): This should be changed if your server has been configured to use a different port for the REST++ endpoint service that TigerGraph provides
* interpreterPort (default 14240): This should be changed if your server has been configured to use a different port for GraphStudio

The username and password default to the TigerGraph default username and password, which is tigergraph. The API token can be obtained via the method described below.
# The Methods
Once you have the package installed and the connection instantiated, you are all set to use the provided methods. There are currently only three methods, but more are on the way.
## getToken("SECRET", "LIFETIME")
This method gets an API token given a secret key and the desired lifetime of the token, in seconds. When instantiating a connection to use this method, simply leave the apiToken field blank, so it looks like this:
<script src="https://gist.github.com/parkererickson/497da3e1660d9979e3e509fc05661a27.js"></script>

## runInstalledQuery("queryName", {params})
This method runs a query installed on the database and returns the JSON response. The query name is a string and then a dictionary of parameters. Once you instantiate a connection, the code looks something like this:
<script src="https://gist.github.com/parkererickson/9106b9a10b6ed6297d44e4a3f5255135.js"></script>

## getEndpoints()
This method returns a JSON response of all possible endpoints on the server. To run, simply:
<script src="https://gist.github.com/parkererickson/789d364f7278042a7570dd392954c83d.js"></script>