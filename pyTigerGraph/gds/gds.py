from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection
from .featurizer import Featurizer

class GDS:
    def __init__(self, conn:"TigerGraphConnection"):
        self.conn = conn
    
    def featurizer(self,name_of_query:str=None,result_attr: str = None,local_gsql_path: str = None):
        return Featurizer(self.conn,name_of_query,result_attr,local_gsql_path)