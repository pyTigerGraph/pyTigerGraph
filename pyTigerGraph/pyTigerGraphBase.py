"""pyTigerGraph basic functionality.

Low(est) level HTTP(S) communication used by all other modules (directly or indirectly).
"""

import base64
import json
import sys
from urllib.parse import urlparse

import requests

from pyTigerGraph.pyTigerGraphException import TigerGraphException


def excepthook(type, value, traceback):
    """This function prints out a given traceback and exception to sys.stderr.

    See: https://docs.python.org/3/library/sys.html#sys.excepthook
    """
    print(value)
    # TODO Proper logging


class pyTigerGraphBase(object):
    """pyTigerGraph basic functionality."""

    def __init__(self, host: str = "http://127.0.0.1", graphname: str = "MyGraph",
            username: str = "tigergraph", password: str = "tigergraph",
            restppPort: [int, str] = "9000", gsPort: [int, str] = "14240", gsqlVersion: str = "",
            version: str = "", apiToken: str = "", useCert: bool = True, certPath: str = None,
            debug: bool = False, sslPort: [int, str] = "443", gcp: bool = False):
        """Initiate a connection object.

        Args:
            host:
                The host name or IP address of the TigerGraph server.
            graphname:
                The default graph for running queries.
            username:
                The username on the TigerGraph server.
            password:
                The password for that user.
            restppPort:
                The port for REST++ queries.
            gsPort:
                The port of all other queries.
            gsqlVersion:
                The version of the GSQL client to be used. Effectively the version of the database
                being connected to.
            version:
                DEPRECATED; use `gsqlVersion()`.
            apiToken:
                DEPRECATED; use `getToken()` with a secret to get a session token.
            useCert:
                DEPRECATED; the need for CA certificate is now determined by URL scheme.
            certPath:
                The filesystem path to the CA certificate. Required in case of https connections.
            debug:
                Enable debug messages.
            sslPort:
                Port for fetching SSL certificate in case of firewall.
            gcp:
                Is firewall used?

        Raises:
            TigerGraphException: In case on invalid URL scheme.

        TODO Rename/generalise `gcp`
        """
        inputHost = urlparse(host)
        if inputHost.scheme not in ["http", "https"]:
            raise TigerGraphException("Invalid URL scheme. Supported schemes are http and https.",
                "E-0003")
        self.netloc = inputHost.netloc
        self.host = "{0}://{1}".format(inputHost.scheme, self.netloc)
        self.username = username
        self.password = password
        self.graphname = graphname

        # TODO Use more generic name (e.g. `onCloud` or `viaFirewall`; not `beta` or `cgp`
        self.beta = gcp
        restppPort = str(restppPort)
        if self.beta and (restppPort == "9000" or restppPort == "443"):
            # TODO Should not `sslPort` be used instead of hard coded value?
            self.restppPort = "443"
            self.restppUrl = self.host + ":443" + "/restpp"
        else:
            self.restppPort = restppPort
            self.restppUrl = self.host + ":" + self.restppPort
        self.gsPort = ""
        gsPort = str(gsPort)
        if self.beta and (gsPort == "14240" or gsPort == "443"):
            # TODO Should not `sslPort` be used instead of hard coded value?
            self.gsPort = "443"
            self.gsUrl = self.host + ":443"
        else:
            self.gsPort = gsPort
            self.gsUrl = self.host + ":" + self.gsPort
        self.url = ""

        self.apiToken = apiToken
        # TODO Eliminate version and use gsqlVersion only, meaning TigerGraph server version
        if gsqlVersion != "":
            self.version = gsqlVersion
        elif version != "":
            self.version = version
        else:
            self.version = ""
        self.base64_credential = base64.b64encode(
            "{0}:{1}".format(self.username, self.password).encode("utf-8")).decode("utf-8")
        if self.apiToken:
            self.authHeader = {'Authorization': "Bearer " + self.apiToken}
        else:
            self.authHeader = {'Authorization': 'Basic {0}'.format(self.base64_credential)}

        self.debug = debug
        if not self.debug:
            sys.excepthook = excepthook
            sys.tracebacklimit = None
        self.schema = None
        self.downloadCert = useCert
        if inputHost.scheme == "http":
            self.downloadCert = False
            self.useCert = False
            self.certPath = ''
        elif inputHost.scheme == "https":
            self.downloadCert = True
            self.useCert = True
            self.certPath = certPath
        self.downloadJar = False
        self.sslPort = sslPort

        self.gsqlInitiated = False

        self.Client = None

    def _errorCheck(self, res: dict):
        """Checks if the JSON document returned by an endpoint has contains ``error: true``. If so,
            it raises an exception.

        Args:
            res:
                The output from a request.

        Raises:
            TigerGraphException: if request returned with error, indicated in the returned JSON.
        """
        if "error" in res and res["error"] and res["error"] != "false":
            # Endpoint might return string "false" rather than Boolean false
            raise TigerGraphException(res["message"], (res["code"] if "code" in res else None))

    def _req(self, method: str, url: str, authMode: str = "token", headers: dict = None,
            data: [dict, list, str] = None, resKey: str = "results", skipCheck: bool = False,
            params: [dict, list, str] = None) -> [dict, list]:
        """Generic REST++ API request.

        Args:
            method:
                HTTP method, currently one of GET, POST or DELETE.
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, one of "token" (default) or "pwd".
            headers:
                Standard HTTP request headers.
            data:
                Request payload, typically a JSON document.
            resKey:
                The JSON subdocument to be returned, default is "result".
            skipCheck:
                Skip error checking? Some endpoints return error to indicate that the requested
                action is not applicable; a problem, but not really an error.
            params:
                Request URL parameters.

        Returns:
            The (relevant part of the) response from the request (as a dictionary).
        """
        if authMode == "token" and str(self.apiToken) != "":
            if isinstance(self.apiToken, tuple):
                self.apiToken = self.apiToken[0]
            self.authHeader = {'Authorization': "Bearer " + self.apiToken}
            _headers = self.authHeader
        else:
            self.authHeader = {'Authorization': 'Basic {0}'.format(self.base64_credential)}
            _headers = self.authHeader
            authMode = 'pwd'

        if authMode == "pwd":
            _auth = (self.username, self.password)
        else:
            _auth = None
        if headers:
            _headers.update(headers)
        if method == "POST":
            _data = data
        else:
            _data = None

        if self.useCert is True or self.certPath is not None:
            res = requests.request(method, url, headers=_headers, data=_data, params=params,
                verify=False)
        else:
            res = requests.request(method, url, headers=_headers, data=_data, params=params)

        if res.status_code != 200:
            res.raise_for_status()
        res = json.loads(res.text)
        if not skipCheck:
            self._errorCheck(res)
        if not resKey:
            if self.debug:
                print(res)
                # TODO Proper logging
            return res
        if self.debug:
            print(res[resKey])
            # TODO Proper logging
        return res[resKey]

    def _get(self, url: str, authMode: str = "token", headers: dict = None, resKey: str = "results",
            skipCheck: bool = False, params: [dict, list, str] = None) -> [dict, list]:
        """Generic GET method.

        Args:
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, one of "token" (default) or "pwd".
            headers:
                Standard HTTP request headers.
            resKey:
                The JSON subdocument to be returned, default is "result".
            skipCheck:
                Skip error checking? Some endpoints return error to indicate that the requested
                action is not applicable; a problem, but not really an error.
            params:
                Request URL parameters.

        Returns:
            The (relevant part of the) response from the request (as a dictionary).
       """
        res = self._req("GET", url, authMode, headers, None, resKey, skipCheck, params)
        return res

    def _post(self, url: str, authMode: str = "token", headers: dict = None,
            data: [dict, list, str] = None, resKey: str = "results", skipCheck: bool = False,
            params: [dict, list, str] = None) -> [dict, list]:
        """Generic POST method.

        Args:
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, one of "token" (default) or "pwd".
            headers:
                Standard HTTP request headers.
            data:
                Request payload, typically a JSON document.
            resKey:
                The JSON subdocument to be returned, default is "result".
            skipCheck:
                Skip error checking? Some endpoints return error to indicate that the requested
                action is not applicable; a problem, but not really an error.
            params:
                Request URL parameters.

        Returns:
            The (relevant part of the) response from the request (as a dictionary).
        """
        return self._req("POST", url, authMode, headers, data, resKey, skipCheck, params)

    def _delete(self, url: str, authMode: str = "token") -> [dict, list]:
        """Generic DELETE method.

        Args:
            url:
                Complete REST++ API URL including path and parameters.
            authMode:
                Authentication mode, one of "token" (default) or "pwd".

        Returns:
            The response from the request (as a dictionary).
       """
        return self._req("DELETE", url, authMode)
