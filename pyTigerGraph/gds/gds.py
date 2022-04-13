from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection
from .featurizer import Featurizer

class GDS:
    def __init__(self, conn:"TigerGraphConnection"):
        self.conn = conn
    
    def featurizer(self):
        return Featurizer(self.conn)