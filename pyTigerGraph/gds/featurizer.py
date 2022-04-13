from asyncio import tasks
from importlib_metadata import metadata
from parso import split_lines
from typing import TYPE_CHECKING, Union
if TYPE_CHECKING:
    from ..pyTigerGraph import TigerGraphConnection
from ..pyTigerGraphVertex import pyTigerGraphVertex
from ..pyTigerGraphEdge import pyTigerGraphEdge
from ..pyTigerGraphSchema import pyTigerGraphSchema
from ..pyTigerGraphQuery import pyTigerGraphQuery
from ..pyTigerGraphUtils import pyTigerGraphUtils
from .utilities import random_string
import os
from os.path import join as pjoin
import re
import random
import string

class Featurizer:
    def __init__(
    self, 
    conn: "TigerGraphConnection"):
    # name_of_query: str = None,
    # result_attr: str = None, 
    # local_gsql_path: str = None):

        """Class for Feature Extraction.
        The job of a feature extracter is to install and run the current algorithms in graph data science libarary.
        Currently, a set of graph algorithms are moved to the gsql folder and have been saved into a dictionary along with their output type.
        To add a specific algorithm, it should be added both to the gsql folder and class variable dictionary. 
        Args:
            conn (TigerGraphConnection): Connection to the TigerGraph database.
        """

        self.conn = conn
        self.queryResult_type_dict = {"tg_pagerank":"Float","tg_fastRP":"List<Double>","tg_label_prop":"INT","tg_louvain":"INT"}#List of graph algorithms
        self.params_dict = {}#input parameter for the desired algorithm to be run

    def _is_query_installed(self, query_name: str) -> bool:
        #If the query id already installed return true
        resp = "GET /query/{}/{}".format(self.conn.graphname, query_name)
        queries = self.conn.getInstalledQueries()
        return resp in queries

    def _install_query_file(self, file_path: str, replace: dict = None):
        #Read the first line of the query file to get the query name, e.g, CREATE QUERY query_name ...
        try:
            with open(file_path) as infile:
                firstLine = infile.readline()
        except FileNotFoundError:
            print('File ',file_path,' does not exist')
            raise
        try:
            name_of_query = firstLine.split("QUERY")[1].strip().split("(")[0]
        except:
            raise ValueError("Cannot parse the query file. The query file should start with CREATE QUERY query_name ...")
        # If a suffix is to be added to query name
        if replace and ("{QUERYSUFFIX}" in replace):
            name_of_query = name_of_query.replace("{QUERYSUFFIX}",replace["{QUERYSUFFIX}"])
        # If query is already installed, skip.
        if self._is_query_installed(name_of_query.strip()):
            return name_of_query
        # Otherwise, install query from file.
        with open(file_path) as infile:
            query = infile.read()
        # Replace placeholders with actual content if given
        if replace:
            for placeholder in replace:
                query = query.replace(placeholder, replace[placeholder])
        # TODO: Check if Distributed query is needed.
        query = ("USE GRAPH {}\n".format(self.conn.graphname) + query + "\ninstall Query {}\n".format(name_of_query))
        print("Installing and optimizing the queries, it might take a minute")
        resp = self.conn.gsql(query)
        status = resp.splitlines()[-1]
        if "Failed" in status:
            raise ConnectionError(status)
        return name_of_query 

    def installAlgorithm(self,query_name:str,schema_type:str="VERTEX",attr_name:str=None,schema_name:str=None):
        '''
        Checks if the query is already installed, if not it will install the query and change the schema if an attribute needs to be added.
        It can change 
        
        Args:
            query_name (str): the name of query to be installed
            schema_type (str): vertex or edge 
            attr_name (str): An attribute name that needs to be added to the vertex/edge
            schema_name (str): the name of specified vertex/edge
        '''
        query_path = pjoin(os.path.dirname(os.path.abspath(__file__)), "gsql", "featurizer", query_name+'.gsql')
        self.local_gsql_path = query_path
        resp = self._install_query_file(query_path)
        if attr_name:
            _ = self._add_attribute(schema_type,self.queryResult_type_dict[query_name],attr_name)
        return resp.strip() 

    
    def _add_attribute(self, schema_type: str, attr_type: str,attr_name: str=None):#, schema_name: str=None):
        #If the current attribute is not already added to the schema, it will create the schema job to do that.
        #If there are multile schema types, the name of schemas can be chosen to add the attr_name to them. 

        # Check whether to add the attribute to vertex(vertices) or edge(s)
        v_type = False
        if schema_type.upper() == "VERTEX":
            target = self.conn.getVertexTypes()
            v_type = True
        elif schema_type.upper() == "EDGE":
            target = self.conn.getEdgeTypes()
        else:
            raise Exception('schema_type has to be VERTEX or EDGE')
        # If attribute should be added to a specific vertex/edge name
        # if schema_name != None:
        #     target.clear()
        #     target.append(schema_name)
        # For every vertex or edge type
        tasks = []
        for t in target:
            attributes = []
            if v_type:
                meta_data =  self.conn.getVertexType(t)
            else:
                meta_data = self.conn.getEdgeType(t)
            for i in range(len(meta_data['Attributes'])):
                attributes.append(meta_data['Attributes'][i]['AttributeName'])
            # If attribute is not in list of vertex attributes, do the schema change to add it
            if attr_name != None and attr_name  not in attributes:
                tasks.append("ALTER {} {} ADD ATTRIBUTE ({} {});\n".format(
                        schema_type, t, attr_name, attr_type))
        # If attribute already exists for schema type t, nothing to do
        if not tasks:
            return "Attribute already exists"
        # Create schema change job 
        job_name = "add_{}_attr_{}".format(schema_type,random_string(6)) 
        job = "USE GRAPH {}\n".format(self.conn.graphname) + "CREATE GLOBAL SCHEMA_CHANGE JOB {} {{\n".format(
            job_name) + ''.join(tasks) + "}}\nRUN GLOBAL SCHEMA_CHANGE JOB {}".format(job_name)
        # Submit the job
        resp = self.conn.gsql(job)
        status = resp.splitlines()[-1]
        if "Failed" in status:
            raise ConnectionError(status)
        else:
            print(status)
        return 'Global schema change succeeded.'

    
    def _get_Params(self,name_of_query:str):
        #return default query parameters by parsing the query header.
        local_gsql_path = pjoin(os.path.dirname(
            os.path.abspath(__file__)), "gsql", "featurizer", name_of_query + ".gsql")
        with open(local_gsql_path) as infile:
            _dict = {}
            query = infile.read()
        try:
            input_params = query[query.find('(')+1:query.find(')')]
            list_params =input_params.split(',')
            for i in range(len(list_params)):
                if "=" in list_params[i]:
                    params_type = list_params[i].split('=')[0].split()[0]
                    if params_type.lower() == 'float' or params_type.lower() == 'double':
                        _dict[list_params[i].split('=')[0].split()[1]] = float(list_params[i].split('=')[1])
                    if params_type.lower() == 'bool':
                        _dict[list_params[i].split('=')[0].split()[1]] = bool(list_params[i].split('=')[1])
                    if params_type.lower() == 'int':
                        _dict[list_params[i].split('=')[0].split()[1]] = int(list_params[i].split('=')[1])
                else:
                    _dict[list_params[i].split()[1]] =  None
        except:
            print("The algorithm does not have any input parameter.")
        self.params_dict[name_of_query] = _dict
        return _dict  
               
    def runAlgorithm(self,name_of_query:str,params:dict = None, timeout:int=2147480,sizeLimit:int=None):
        '''
        Runs an installed query.
        The query must be already created and installed in the graph.
        If the query accepts input parameters and the parammeters have not been provided, they will be initialized by parsing the query.
        Args:
            name_of_query:
                The name of the query to be executed.
            params:
                Query parameters. a dictionary.
            timeout:
                Maximum duration for successful query execution (in milliseconds).
                See https://docs.tigergraph.com/tigergraph-server/current/api/#_gsql_query_timeout
            sizeLimit:
                Maximum size of response (in bytes).
                See https://docs.tigergraph.com/tigergraph-server/current/api/#_response_size

        Returns:
            The output of the query, a list of output elements (vertex sets, edge sets, variables,
            accumulators, etc.
        '''
        if params == None:
            params = self._get_Params(name_of_query)
            print(params)
            if params:
                print("Default parameters are:",params)
                if None in params.values():
                    raise ValueError("Query parameters which are None need to be initialized.")
            else:
                print("No parameters")
                result = self.conn.runInstalledQuery(name_of_query)
                if result != None:
                    return result
        else:     
            result = self.conn.runInstalledQuery(name_of_query, params,timeout=timeout,sizeLimit = sizeLimit)
            if result != None:
                return result
        


    