# Authentication

If [GSQL authentication](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#gsql-authentication) is enabled
in the TigerGraph database, then `username` and `password` need to be specified when the connection is established.

If [REST++ authentication](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication) is enabled, and you already have an API authorization token, specify that one as well. Alternatively, if you posess a
[secret](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#managing-credentials) (created previously in the database),
you can request a token via the [getToken](#getToken) function and use it during the session. See [Token Management](TokenManagement.md) for more details.

When [establising a connection](GettingStarted.md) the username and password default to the TigerGraph default username and password, which are _tigergraph_.
If user authentication is to e enabled, the _tigergraph_ user's password must be changed.
Furthermore, it is recommended not to use the _tigergraph_ user for anything other than system administration.
Instead, [create additional users](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#creating-and-managing-users) and
grant them the [appropriate privileges through roles](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#roles-and-privileges),
then use those users to access the database.

It is recommended to not push any form of authentication information to version control software. Because of this, we recommend to create a `cfg.py` file that looks like this:
```python
secret = "YOUR_SECRET_HERE"
token = ""
password = "YOUR_PASSWORD_HERE"
```
You can then import this config file into your Python applications, and add `cfg.py` to your `.gitignore`. To create a connection to your database, simply:
```python
import pyTigerGraph as tg 
import cfg

cfg.token = tg.TigerGraphConnection(host="<hostname>", graphname="<graph_name>").getToken(cfg.secret, "<token_lifetime>")[0]

conn = tg.TigerGraphConnection(host="<hostname>", graphname="<graph_name>", password=cfg.password, apiToken=cfg.token)
```
Substitute the hostname and graph name with the correct credentials, as well as the desired lifetime of the API token fetched.
