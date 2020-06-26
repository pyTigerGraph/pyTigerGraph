# GSQL Submodule (Beta)
The GSQL submodule provides GSQL shell functionality, allowing you to create and execute queries, loading tasks, and authentication tasks. Java must be installed on the system. OpenSSL is also used, but you can provide your own certificate if OpenSSL is not installed on your system.

## Getting Started
First, you will need to import the submodule and pass in a TigerGraphConnection object. This will look something like this:
```python
import pyTigerGraph as tg 
from pyTigerGraph import gsql

conn = tg.TigerGraphConnection(host='https://localhost', graphname="social")

shell = gsql.Gsql(conn, certNeeded=False)
```

## gsql
Once you have the connection setup, we can use the gsql() method to send a query. By default, all commands are run using the graph that is passed in with the TigerGraphConnection. You can customize the command line options via the options argument, which takes in a list. If you wish to run a query on the whole server, simply set the options argument to [].

## createSecret
Returns a secret key. Takes in a string as an argument if you want to use an alias while generating the key.