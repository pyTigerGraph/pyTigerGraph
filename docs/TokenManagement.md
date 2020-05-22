### getToken
`getToken(secret, setToken=True, lifetime=None)`

Requests an authorisation token.

This function returns a token only if [REST++ authentication is enabled](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication).
If not, an exception will be raised.

Arguments:
- `secret`: The secret (string) generated in GSQL using [`CREATE SECRET`](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#create-show-drop-secret).
- `setToken`: Set the connection's API token to the new value (default: `True`).
- `lifetime`: Duration of token validity (in secs, default 30 days = 2,592,000 secs).

Returns a tuple of `(<new_token>, <exporation_timestamp_unixtime>, <expiration_timestamp_ISO8601>)`. Return value can be ignored.

Note: expiration timestamp's time zone might be different from your computer's local time zone.

Documentation: [GET /requesttoken](https://docs.tigergraph.com/dev/restpp-api/restpp-requests#requesting-a-token-with-get-requesttoken)

### refreshToken
`refreshToken(secret, token=None, lifetime=2592000)`

Extends a token's lifetime.

This function works only if [REST++ authentication is enabled](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication).
If not, an exception will be raised.

Arguments:
- `secret`: The secret (string) generated in GSQL using [`CREATE SECRET`](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#create-show-drop-secret).
- `token`: The token requested earlier. If not specified, refreshes current connection's token.
- `lifetime`: Duration of token validity (in secs, default 30 days = 2,592,000 secs).

Returns a tuple of `(<token>, <exporation_timestamp_unixtime>, <expiration_timestamp_ISO8601>)`. Return value can be ignored.
Raises exception if specified token does not exists.

Note:
- New expiration timestamp will be _now + lifetime seconds_, **not** _current expiration timestamp + lifetime seconds_.
- Expiration timestamp's time zone might be different from your computer's local time zone.

Documentation: [PUT /requesttoken](https://docs.tigergraph.com/dev/restpp-api/restpp-requests#refreshing-tokens)

### deleteToken
`deleteToken(secret, token)`

Deletes a token.

This function works only if [REST++ authentication is enabled](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#rest-authentication).
If not, an exception will be raised.

Arguments:
- `secret`: The secret (string) generated in GSQL using [`CREATE SECRET`](https://docs.tigergraph.com/admin/admin-guide/user-access-management/user-privileges-and-authentication#create-show-drop-secret).
- `token`: The token requested earlier. If not specified, deletes current connection's token, so be careful.
- `skipNA`: Don't raise exception if specified token does not exist.

Returns `True` if deletion was successful or token did not exist but `skipNA` was `True`; raises exception otherwise.

Documentation: [DELETE /requesttoken](https://docs.tigergraph.com/dev/restpp-api/restpp-requests#deleting-tokens)