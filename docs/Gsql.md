# GSQL Submodule

pyTigerGraph uses the [GSQL command line client](https://docs.tigergraph.com/dev/using-a-remote-gsql-client) to enable sending arbitrary GSQL statements to the database. This enables you to execute operations that are currently not available through REST API endpoints.

- The client is implemented in Java and distributed as a `.jar` archive. The client is downloaded on demand to the machine running your pyTigerGraph application; so you need to ensure that your code can access the [download location](https://bintray.com/tigergraphecosys/tgjars/gsql_client) and can save the `.jar` file locally. Furthermore, Java 8+ JRE must be installed and accessible on the machine.
- The `.jar` file is downloaded only if it's not yet available locally, i.e. it is downloaded only once for each database version accessed. Thus, you will potentially have multiple versions of the `.jar` file downloaded.
- The name pattern of the `.jar` file is `gsql_client-x.y.z.jar`, e.g. `gsql_client-2.6.2.jar` or `gsql_client-3.0.0.jar`.
- The default location where the `.jar` file is saved locally is `~/.gsql` (which on Windows is `\Users\<username>\.gsql`). This can be changed using the `initGsql()` function's `jarLocation` parameter. The `initGsql()` needs to be called before any function that depends on the GSQL functionality (e.g. `gsql()` and `getSchema()`) is invoked. 
- Generally, the version of the database and the GSQL client must match. pyTigerGraph can query the version number of the database, and requests the appropriate client from the download location. In rare cases (when the changes/fixes do not impact the GSQL functionality) no new GSQL client version is released when a new version of the database is shipped. In these cases an appropriate GSQL client version needs to be manually specified via the `gsqlVersion` parameter (typically the latest available version that is lesser than the database version). You can check the list of available GSQL clients at the [download location](https://bintray.com/tigergraphecosys/tgjars/gsql_client).
- <span style="color:red">**NOTE:**</span> The intention is to replace the current functionality that uses the GSQL client with one that natively (i.e. using Python code only) communicates with the database. This is a short term plan, so look for changes in this are when you upgrade pyTgigerGraph (altough backward compatibility will be maintained).

First, you will need to [create a `TigerGraphConnection`](GettingStarted.md).

You can then initialize and test your connection by issuing any GSQL command like:
```python
print(conn.gsql('ls', options=[]))
```

See the [GSQL101 notebook](https://github.com/pyTigerGraph/pyTigerGraph/blob/master/examples/GSQL101%20-%20PyTigerGraph.ipynb) for more examples.

## gsql
`conn.gsql(query, options=None)`

Runs a GSQL query and processes the output.

Arguments:
- `query`: The text of the query to run as one string. 
- `options`: A list of strings that will be passed as options the the GSQL client. A `None` gets replaced with `['g', self.graphname]` causing the queries to run on the default graph. Use `options=[]` to overide the default graph and submit to the global graph instead.

When the query resonse is a JSON formatted string the return value of `gsql()` would be a processed JSON object, otherwise the response text is returned as is.

## initGsql - OBSOLETE
`conn.initGsql(jarLocation="", certLocation="")`

**OBSOLETE - GSQL initilization is handled automatically via `gsql` function.**

This function allows you to initiate the GSQL submodule ahead of the first call to any other GSQL submodule command. With it you to customize the location at which the GSQL client `.jar` and the SSL certificate will be stored.

If the defaults (as shown above) are appropriate, you do not need to use this function.
