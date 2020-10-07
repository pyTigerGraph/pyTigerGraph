# Getting Started

## Establishing the connection to a TigerGraph database

First, import pyTigerGraph:

```py
import pyTigerGraph as tg
```

The functionality of pyTigerGraph is implemented by the `TigerGraphConnection` class. To establish the connection, instantiate the class:

```
conn = tg.TigerGraphConnection(<parameters>)
```

The constuctor has following parameters:
- `host`:              The IP address or hostname of the TigerGraph server, including the scheme (`http` or `https`). Default: _http://localhost_.
- `graphname`:         The default graph for running queries. Default: _MyGraph_.
- `username`:          The username on the TigerGraph server. Default: _tigergraph_.
- `password`:          The password for that user. Default: _tigergraph_.
- `restppPort`:        The post for REST++ queries. Default: _9000_.
- `gsPort`:            The port of all other queries. Default: _14240_.
- `apiToken`:          A token to use when making queries. No default value. Ignored if [REST++ authentication is not enabled](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#enabling-and-using-user-authentication). See notes below. 
- `gsqlVersion`:       The version of GSQL client to be used. Default: same as database version. See [GSQL Submodule](Gsql.md) for more details.
- `useCert`:           True if SSL certificate is required for connection. No default value. See notes below.
- `certPath`:          The location/directory _and_ the name of the SSL certification file where the certification should be stored. No default value. See notes below.

**Notes**:
- As pyTigerGraph is communicating with the TigerGraph database through REST APIs, there is no real "connection". Most (but not all) function of pyTigerGraph sends (one or more) HTTP(s) request to the REST API and processes the data returned (typically a JSON response). Thus there is no "connection" that needs to be opened and then closed down. Instantiating pyTigerGraph simply means to provide the neccesary information to be able to send the requests and receive and response.
- See the [Token Management](TokenManagement.md) page for information on how authentication works and how to retrieve and manage API tokens.
- If the TigerGraph database uses [encrypted connections](https://docs.tigergraph.com/admin/admin-guide/data-encryption/encrypting-connections) (e.g. TigerGraph could instances), then you need to provide an SSL certificate for your connections. In this case you need to specify `userCert=True` and the location of the SSL certificate in `certPath`. pyTigerGraph will generate and download a self-signed SSL certificate for you. If `userCert=False` or `certPath` is not set, pyTigerGraph will try to connect without certificate. `userCert` should be `False` if you connect to an unsecure server such as a TigerGraph Developer instance.
  -  <span style="color:red">**NOTE:**</span> This functionality is not tested and most likely does not work on Windows. We intend to fix this; help is welcome (it seems all contributors are using Macs).

## GSQL 101 With pyTigerGraph

Checkout [this](https://github.com/pyTigerGraph/pyTigerGraph/blob/master/examples/GSQL101%20-%20PyTigerGraph.ipynb) example for completing the GSQL 101 course in a Jupyter Notebook environment. Through this, you will also learn of various pyTigerGraph methods that you can use.
