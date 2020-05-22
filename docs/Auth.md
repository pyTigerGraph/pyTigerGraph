If [user authentication](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#enabling-and-using-user-authentication) is enabled
in the TigerGraph database, then `username` and `password` need to be specified when the connection is established.
If you already have an API authorization token, specify that one as well. Alternatively, if you know a
[secret](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#create-show-drop-secret) (created previously in the database),
you can request a token via the [getToken](#getToken) function and use it during the session.

If user authentication is not enabled, then username, password and authorization token are not used (i.e. the database is insecure). This is only acceptable in case of development
or study environments.

The username and password default to the TigerGraph default username and password, which are _tigergraph_.
If user authentication is enabled, the tigergraph user's password can't be tigergraph, so a different one must be selected.
Furthermore, it is recommended not to use the _tigergraph_ user for anything other than system administration.
Instead, [create additional users](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#creating-and-managing-users) and
grant them the [appropriate privileges through roles](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#roles-and-privileges),
then use those users to access the database.