import warnings

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from pyTigerGraph.pyTigerGraphAuth import pyTigerGraphAuth
from pyTigerGraph.pyTigerGraphEdge import pyTigerGraphEdge
from pyTigerGraph.pyTigerGraphLoading import pyTigerGraphLoading
from pyTigerGraph.pyTigerGraphPath import pyTigerGraphPath
from pyTigerGraph.pyTigerGraphUDT import pyTigerGraphUDT
from pyTigerGraph.pyTigerGraphVertex import pyTigerGraphVertex
from .gds import gds

# Added pyTigerDriver Client

warnings.filterwarnings("default", category=DeprecationWarning)


# TODO Proper deprecation handling; import deprecation?

class TigerGraphConnection(pyTigerGraphVertex, pyTigerGraphEdge, pyTigerGraphUDT, pyTigerGraphAuth,
    pyTigerGraphLoading, pyTigerGraphPath):
    """Python wrapper for TigerGraph's REST++ and GSQL APIs"""

    def __init__(self, host: str = "http://127.0.0.1", graphname: str = "MyGraph",
            username: str = "tigergraph", password: str = "tigergraph",
            restppPort: [int, str] = "9000", gsPort: [int, str] = "14240", gsqlVersion: str = "",
            version: str = "", apiToken: str = "", useCert: bool = True, certPath: str = None,
            debug: bool = False, sslPort: [int, str] = "443", gcp: bool = False):
        super().__init__(host, graphname, username, password, restppPort
            , gsPort, gsqlVersion, version, apiToken, useCert, certPath, debug, sslPort, gcp)
        self.gds = gds.GDS(self)

# EOF
