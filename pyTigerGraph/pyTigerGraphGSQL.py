"""GSQL Interface."""

import os
import sys
from urllib.parse import urlparse

from pyTigerDriver import GSQL_Client

from pyTigerGraph.pyTigerGraphBase import pyTigerGraphBase
from pyTigerGraph.pyTigerGraphException import TigerGraphException


class pyTigerGraphGSQL(pyTigerGraphBase):
    """GSQL Interface."""

    def initGsql(self, certLocation: str = "~/.gsql/my-cert.txt") -> bool:
        """Initialises the GSQL support.

        Args:
            certLocation:
                The path and file of the CA certificate.

        Returns:
            `True` if initialisation was successful.

        Raises:
            Exception if initialisation was unsuccessful.
        """
        if not certLocation:
            if not os.path.isdir(os.path.expanduser("~/.gsql")):
                os.mkdir(os.path.expanduser("~/.gsql"))
            certLocation = "~/.gsql/my-cert.txt"

        self.certLocation = os.path.expanduser(certLocation)
        self.url = urlparse(self.gsUrl).netloc  # Getting URL with gsql port w/o https://
        sslhost = self.url.split(":")[0]

        if self.downloadCert:  # HTTP/HTTPS
            import ssl
            try:
                Res = ssl.get_server_certificate((sslhost, self.sslPort))
                # TODO Expected type 'Tuple[str, int]', got 'Tuple[bytes, Any]' instead
            except:  # TODO PEP 8: E722 do not use bare 'except'
                Res = ssl.get_server_certificate((sslhost, 14240))
                # TODO Expected type 'Tuple[str, int]', got 'Tuple[bytes, int]' instead

            try:
                certcontent = open(self.certLocation, 'w')
                certcontent.write(Res)
                certcontent.close()
            except Exception:  # TODO Too broad exception clause
                self.certLocation = "/tmp/my-cert.txt"

                certcontent = open(self.certLocation, 'w')
                certcontent.write(Res)
                certcontent.close()
            if os.stat(self.certLocation).st_size == 0:
                raise TigerGraphException(
                    "Certificate download failed. Please check that the server is online.", None)

        try:

            if self.downloadCert:
                if not self.certPath:
                    self.certPath = self.certLocation
                self.Client = GSQL_Client(urlparse(self.host).netloc, version=self.version,
                    username=self.username,
                    password=self.password,
                    cacert=self.certPath, gsPort=self.gsPort,
                    restpp=self.restppPort, debug=self.debug)
            else:
                self.Client = GSQL_Client(urlparse(self.host).netloc, version=self.version,
                    username=self.username,
                    password=self.password,
                    gsPort=self.gsPort, restpp=self.restppPort, debug=self.debug)
            self.Client.login()
            self.gsqlInitiated = True
            return True
        except Exception as e:
            print("Connection Failed check your Username/Password {}".format(e))
            self.gsqlInitiated = False

    def gsql(self, query: str, graphname: str = None, options=None) -> [str, dict]:
        """Runs a GSQL query and process the output.

        Args:
            query:
                The text of the query to run as one string. The query is one or more GSQL statement.
            graphname:
                The name of the graph to attach to. If not specified, the graph name provided at the
                time of establishing the connection will be used.
            options:
                DEPRECATED

        Returns:
            The output of the statement(s) executed.
        """
        if graphname is None:
            graphname = self.graphname
        if str(graphname).upper() == "GLOBAL" or str(graphname).upper() == "":
            graphname = ""
        if not self.gsqlInitiated:
            self.gsqlInitiated = self.initGsql()
        if self.gsqlInitiated:
            if "\n" not in query:
                res = self.Client.query(query, graph=graphname)
                if isinstance(res, list):
                    return "\n".join(res)
                else:
                    return res
            else:
                res = self.Client.run_multiple(query.split("\n"))
                if isinstance(res, list):
                    return "\n".join(res)
                else:
                    return res
        else:
            print("Couldn't Initialize the client see above error.")
            sys.exit(1)
