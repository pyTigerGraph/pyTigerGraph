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
    clientVersion="2.6.0",
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
    - `options`:    A list of strings that will be passed as options the the gsql_client. Use 
                    `options=[]` to overide the default graph.

Once you have a TigerGraphConnection set up, you can use the `gsql()` method to send any GSQL query. By default, all commands are run using the graph that is passed in with the TigerGraphConnection. You can customize the command line options via the options argument, which takes in a list. If you wish to run a query on the whole server, simply set the options argument to [].

When the query resonse is a JSON formatted the return value of `gsql()` would be the processed JSON object, otherwise the response text is returned as a string.

## createSecret
```conn.createSecret(alias="")```
Returns a secret key. Takes in a string as an argument if you want to use an alias while generating the key.

## initGsql
```conn.initGsql(jarLocation="~/.gsql", certLocation="~/.gsql/my-cert.txt")
This commnad allows you to initiate the gsql submodule ahead of the first call to any other gsql submodule command. This allows you to customize the location at which the gsql_client.jar and the certificate will be stored.

## downloadJar and downloadCert
Use `conn.downloadJar=False` and `conn.downloadCert=Flase` to prevent the automatic downloads. This is useful when working with a user supplied gsql_client.jar file or certificate. 