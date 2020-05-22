### echo
`echo()`

Pings the database.

Expected return value is "Hello GSQL"

Documentation: [GET /echo](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-echo-and-post-echo) and [POST /echo](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-echo-and-post-echo)

### getEndpoints
`getEndpoints(builtin=False, dynamic=False, static=False)`

Lists the REST++ endpoints and their parameters.

Arguments:
- `builtin`: TigerGraph provided REST++ endpoints.
- `dymamic`: Endpoints for user installed queries.
- `static`:  Static endpoints.

If none of the above arguments are specified, all endpoints are listed.
Documentation: [GET /endpoints](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-endpoints)

### getStatistics
`getStatistics(seconds=10, segment=10)`

Arguments:
- `seconds`: The duration of statistic collection period (the last _n_ seconds before the function call).
- `segments`: The number of segments of the latency distribution (shown in results as LatencyPercentile). By default, segments is 10, meaning the percentile range 0-100% will be divided into ten equal segments: 0%-10%, 11%-20%, etc. Segments must be [1, 100].

Retrieves real-time query performance statistics over the given time period.

Documentation: [GET /statistics](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-statistics)

### getVersion
`getVersion()`

Retrieves the git versions of all components of the system.

Documentation: [GET /version](https://docs.tigergraph.com/dev/restpp-api/built-in-endpoints#get-version)

### getVer
`getVer(component="product", full=False)`

Arguments:
- `component`: One of TigerGraph's components (e.g. product, gpe, gse).

Gets the version information of specific component.

Get the full list of components using [`getVersion`](#getVersion).

### getLicenseInfo
`getLicenseInfo()`

Returns the expiration date and remaining days of the license.

In case of evaluation/trial deployment, an information message and -1 remaining days are returned.