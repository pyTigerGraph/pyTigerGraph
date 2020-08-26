# GSQL Submodule
The GSQL submodule is integrated into TigerGraphConnection anf gets loaded the first time you execute one of its functions. It provides GSQL shell functionality, allowing you to create and execute queries, loading tasks, and authentication tasks. Java must be installed on the system. OpenSSL is also used, but you can provide your own certificate if OpenSSL is not installed on your system (see initGsql).

## Getting Started
First, you will need to create a TigerGraphConnection:
```python
import pyTigerGraph as tg 

conn = tg.TigerGraphConnection(
    host="your.server.ip.address", 
    graphname="social", 
    password="yourpassword", 
    version="3.0.0", 
    #useCert=False
)
```

When using a non-secure conncetion, for example to TigerGraph Developer, you must uncomment the `useCert=False` option.

You can now initilize and test your connction by issuing any GSQL command like:
```python
print(conn.gsql('ls', options=[]))
```

See the [GSQL101 notebook](https://github.com/pyTigerGraph/pyTigerGraph/blob/master/examples/GSQL101%20-%20PyTigerGraph.ipynb) for more examples.

## gsql
```conn.gsql(query, options=None)```
Runs a GSQL query and process the output.

Arguments:
    - `query`:      The text of the query to run as one string. 
    - `options`:    A list of strings that will be passed as options the the gsql_client. A `None` gets replaced with
      		    `['g', self.graphname]` causeing the queries to run on the default graph. Use `options=[]` to
		    overide the default graph and submit to the global enviroment instead.

Once you have a TigerGraphConnection set up, you can use the `gsql()` method to send any GSQL query. By default, all commands are run using the graph that was passed in with the TigerGraphConnection. This is done using the gsql_client command line options. You can customize these command line options via the `options` argument, which takes in a list of strings. If you wish to run a query globaly simply pass `options=[]`.

When the query resonse is a JSON formatted string the return value of `gsql()` would be a processed JSON object, otherwise the response text is returned as is.

## createSecret
```conn.createSecret(alias="")```
Returns a secret key. Takes in a string as an argument if you want to use an alias while generating the key.

## initGsql
```conn.initGsql(jarLocation="~/.gsql", certLocation="~/.gsql/my-cert.txt")
This commnad allows you to initiate the gsql submodule ahead of the first call to any other gsql submodule command. This allows you to customize the location at which the gsql_client.jar and the certificate will be stored.

## downloadJar and downloadCert
Use `conn.downloadJar=False` and `conn.downloadCert=Flase` to prevent the downloads. This must be set before runing `initGsql`. Mostly this is useful when working with a user supplied `gsql_client.jar` file or certificate. 