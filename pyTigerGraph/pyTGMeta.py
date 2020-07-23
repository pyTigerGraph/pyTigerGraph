from pytg.gsql import Gsql
import re

class pyTGMeta(object):
    '''
    classdocs
    '''


    def __init__(self, conn):
        '''
        Constructor
        '''
        self.conn = conn
        self.shell = Gsql(conn, client_version=self.conn.getVer(), certNeeded=False)
        self.meta = self.collectMeta()

    def _gsql(self, query, options=None):
        None

    def _getQueries(self):
        """ Get query metadata from REST++ endpoint.
    
        It will not return data for queries that are not (yet) installed.
        """
        qs = []
        eps = self.conn.getEndpoints(dynamic=True)
        for ep in eps:
            q = eps[ep]
            q["Name"] = q["parameters"]["query"]["default"]
            q["endpoint"] = ep.split(" ")[1]
            q["method"] = ep.split(" ")[0]
            qs.append(q)
        return qs

    def _processGsqlLs(self):

        res = self.shell.gsql('ls')
        # print(res)

        qpatt = re.compile("[\s\S\n]+CREATE", re.MULTILINE)

        res = res.split("\n")
        i = 0
        while i < len(res):
            l = res[i]
            # Processing vertices
            if l.startswith("  - VERTEX"):
                vtName = l[11:l.find("(")]
                vs = self.meta["VertexTypes"]
                for v in vs:
                    if v["Name"] == vtName:
                        v["Statement"] = "CREATE " + l[4:]
                        break

            # Processing edges
            elif l.startswith("  - DIRECTED") or l.startswith("  - UNDIRECTED"):
                etName = l[l.find("EDGE") + 5:l.find("(")]
                es = self.meta["EdgeTypes"]
                for e in es:
                    if e["Name"] == etName:
                        e["Statement"] = "CREATE " + l[4:]
                        break

            # Processing loading jobs
            elif res[i].startswith("  - CREATE LOADING JOB"):
                txt = ""
                tmp = l[4:]
                txt += tmp + "\n"
                jobName = tmp.split(" ")[3]
                i += 1
                l = res[i]
                while not (l.startswith("  - CREATE") or l.startswith("Queries")):
                    txt += l[4:] + "\n"
                    i += 1
                    l = res[i]
                txt = txt.rstrip(" \n")
                i -= 1
                self.meta["LoadingJobs"].append({"Name": jobName, "Statement": txt})

            # Processing queries
            elif l.startswith("Queries:"):
                i += 1
                l = res[i]
                while l != "":
                    qName = l[4:l.find("(")]
                    txt = self.shell.gsql("SHOW QUERY " + qName).rstrip(" \n")
                    txt = re.sub(qpatt, "CREATE", txt)
                    qs = self.meta["Queries"]
                    found = False
                    for q in qs:
                        if q["Name"] == qName:
                            q["Statement"] = txt
                            found = True
                            break
                    if not found:  # Most likely the query is created but not installed
                        qs.append({"Name": qName, "Statement": txt})
                    i = i + 1
                    l = res[i]

            # Processing UDTs
            elif l.startswith("User defined tuples:"):
                i += 1
                l = res[i]
                while l != "":
                    udtName = l[4:l.find("(")].rstrip()
                    us = self.meta["UDTs"]
                    for u in us:
                        if u["name"] == udtName:
                            u["Statement"] = "TYPEDEF TUPLE <" + l[l.find("(")+1:].rstrip(")") + "> " + udtName
                            u["Name"] = udtName
                            break
                    i = i + 1
                    l = res[i]

            # Processing data sources
            elif l.startswith("Data Sources:"):
                i += 1
                l = res[i]
                while l != "":
                    dsDetails = l[4:].split()
                    ds = {"Name": dsDetails[1], "Type": dsDetails[0], "Details": dsDetails[2]}
                    ds["Statement"] = "CREATE DATA_SOURCE " + dsDetails[0].upper() + " " + dsDetails[1] + ' = "' + dsDetails[2].lstrip("(").rstrip(")").replace('"', "'") + '"'
                    ds["Statement2"] = "GRANT DATA_SOURCE " + dsDetails[1] + " TO GRAPH " + self.conn.graphname
                    self.meta["DataSources"].append(ds)
                    i = i + 1
                    l = res[i]

            # Processing graphs (actually, only one graph should be listed)
            elif l.startswith("  - Graph"):
                gName = l[10:l.find("(")]
                self.meta["Graphs"].append({"Name": gName, "Statement": "CREATE GRAPH " + l[10:].replace(":v","").replace(":e",""),"Text": l[10:]})

            # Ignoring the rest (schema change jobs, labels, comments, empty lines, etc.)
            else:
                pass
            i += 1

    def _processUsers(self):
        us = self.meta["Users"]
        res = self.shell.gsql("SHOW USER")
        res = res.split("\n")
        i = 0
        while i < len(res):
            l = res[i]
            if "- Name:" in l:
                if "tigergraph" in l:
                    i += 1
                    l = res[i]
                    while l != "":
                        i += 1
                        l = res[i]
                else:
                    uName = l[10:]
                    roles = []
                    i += 1
                    l = res[i]
                    while l != "":
                        if "- GraphName: " + self.conn.graphname in l:
                            i += 1
                            l = res[i]
                            roles = l[l.find(":") + 2:].split(", ")
                        i += 1
                        l = res[i]
                    us.append({"Name": uName, "Roles": roles})
            i += 1

    def _processGroups(self):
        gs = self.meta["Groups"]
        res = self.shell.gsql("SHOW GROUP")
        res = res.split("\n")
        i = 0
        while i < len(res):
            l = res[i]
            if "- Name:" in l:
                gName = l[10:]
                roles = []
                rule = ""
                i += 1
                l = res[i]
                while l != "":
                    if  "- GraphName: " + self.conn.graphname in l:
                        i += 1
                        l = res[i]
                        roles = l[l.find(":") + 2:].split(", ")
                    elif "- Rule: " in l:
                        rule = l[l.find(":") + 2:]  
                    i += 1
                    l = res[i]
                gs.append({"Name": gName, "Roles": roles, "Rule": rule})
            i += 1

    def collectMeta(self):
        self.meta = {}

        # Get vertex, edge and user defined types from REST++ API end point
        self.meta.update(self.conn.getSchema(True, True))

        # Get query details from RESP++ end point
        qs = self._getQueries()
        self.meta["Queries"] = self._getQueries()

        # Process metadata from GSQL `ls` output
        self.meta["LoadingJobs"] = []
        self.meta["Graphs"] = []
        self.meta["DataSources"] = []
        
        self._processGsqlLs()

        # Process data from SHOW USER and SHOW GROUP
        self.meta["Users"] = []
        self.meta["Groups"] = []

        self._processUsers()
        self._processGroups()
        
        # Intentionally omitting secrets and tokens

        return self.meta
    
    def generateUDTDDL(self, udtTypes="*"):
        udts = []
        if udtTypes == "*":
            udts = self.conn.getUDTs()
        elif isinstance(udtTypes, str):
            udts = [udtTypes]
        elif isinstance(udtTypes, list):
            udts = udtTypes
        else:
            return None
        ret = []
        for ux in self.meta["UDTs"]:
            if ux["Name"] in udts:
                ret.append(ux["Statement"])
        return ret
    
    def generateVertexDDL(self, vertexTypes="*"):
        vts = []
        if vertexTypes == "*":
            vts = self.conn.getVertexTypes()
        elif isinstance(vertexTypes, str):
            vts = [vertexTypes]
        elif isinstance(vertexTypes, list):
            vts = vertexTypes
        else:
            return None
        ret = []
        for vx in self.meta["VertexTypes"]:
            if vx["Name"] in vts:
                ret.append(vx["Statement"])
        return ret
    
    def generateEdgeDDL(self, edgeTypes="*"):
        ets = []
        if edgeTypes == "*":
            ets = self.conn.getEdgeTypes()
        elif isinstance(edgeTypes, str):
            ets = [edgeTypes]
        elif isinstance(edgeTypes, list):
            ets = edgeTypes
        else:
            return None
        ret = []
        for ex in self.meta["EdgeTypes"]:
            if ex["Name"] in ets:
                ret.append(ex["Statement"])
        return ret
    
    def generateGraphDDL(self):
        for gx in self.meta["Graphs"]:
            if gx["Name"] == self.conn.graphname:
                return [gx["Statement"]]
    
    def generateDataSourceDDL(self, dsNames="*"):
        dss = []
        if dsNames == "*":
            for lj in self.meta["DataSources"]:
                dss.append(lj["Name"])
        elif isinstance(dsNames, str):
            dss = [dsNames]
        elif isinstance(dsNames, list):
            dss = dsNames
        else:
            return None
        ret = []
        for dx in self.meta["DataSources"]:
            if dx["Name"] in dss:
                ret.append([dx["Statement"],dx["Statement2"]])
        return ret
    
    def generateLoadingJobDDL(self, jobNames="*"):
        ljs = []
        if jobNames == "*":
            for lj in self.meta["LoadingJobs"]:
                ljs.append(lj["Name"])
        elif isinstance(jobNames, str):
            ljs = [jobNames]
        elif isinstance(jobNames, list):
            ljs = jobNames
        else:
            return None
        ret = []
        for lx in self.meta["LoadingJobs"]:
            if lx["Name"] in ljs:
                ret.append("BEGIN\n"+ lx["Statement"] + "\nEND")
        return ret
        
    def generateQueryDDL(self, queryNames="*"):
        """Generates DDL for queries.
        
        If query's REST endpoint is specified, also adds `INSTALL QUERY`.
        `INSTALL QUERY ALL` is not used as some of the queries might be executed as installed query only.
        """
        qus = []
        if queryNames == "*":
            for q in self.meta["Queries"]:
                qus.append(q["Name"])
        elif isinstance(queryNames, str):
            qus = [queryNames]
        elif isinstance(queryNames, list):
            qus = queryNames
        else:
            return None
        ret = []
        for qx in self.meta["Queries"]:
            if qx["Name"] in qus:
                if "endpoint" in qx:
                    stmts = ["BEGIN\n"+ qx["Statement"] + "\nEND"]
                    stmts.append("INSTALL QUERY " + qx["Name"])
                    ret.append(stmts)
                else:
                    ret.append("BEGIN\n"+ qx["Statement"] + "\nEND")
        return ret
    
    def generateUserDDL(self, userName="*"):
        uss = []
        if userName == "*":
            for u in self.meta["Users"]:
                uss.append(u["Name"])
        elif isinstance(userName, str):
            uss = [userName]
        elif isinstance(userName, list):
            uss = userName
        else:
            return None
        ret = []
        for ux in self.meta["Users"]:
            if ux["Name"] in uss:
                stmts = ["// CREATE USER " + ux["Name"]]
                for r in ux["Roles"]:
                    stmts.append("GRANT ROLE " + r + " TO " + ux["Name"])
                ret.append(stmts)
        return ret
    
    def generateGroupDDL(self, groupName="*"):
        grs = []
        if groupName == "*":
            for gr in self.meta["Groups"]:
                grs.append(gr["Name"])
        elif isinstance(groupName, str):
            grs = [groupName]
        elif isinstance(groupName, list):
            grs = groupName
        else:
            return None
        ret = []
        for gx in self.meta["Groups"]:
            if gx["Name"] in grs:
                stmts = ["CREATE GROUP " + gx["Name"] + " PROXY " + gx["Rule"]]
                for r in gx["Roles"]:
                    stmts.append("GRANT ROLE " + r + " ON GRAPH " + self.conn.graphname + " TO " + gx["Name"])
                ret.append(stmts)
        return ret
    
    def generateDDL(self):
        ret = {}

        rx = self.generateUDTDDL()
        if rx:
            ret["UDTs"] = rx

        rx = self.generateVertexDDL()
        if rx:
            ret["VertexTypes"] = rx
            
        rx = self.generateEdgeDDL()
        if rx:
            ret["EdgeTypes"] = rx

        ret["Graph"] = self.generateGraphDDL()
        
        rx = self.generateDataSourceDDL()
        if rx:
            ret["DataSources"] = rx
        
        rx = self.generateLoadingJobDDL()
        if rx:
            ret["LoadingJobs"] = rx
            
        rx = self.generateQueryDDL()
        if rx:
            ret["Queries"] = rx
            
        rx = self.generateUserDDL()
        if rx:
            ret["Users"] = rx
            
        rx = self.generateGroupDDL()
        if rx:
            ret["Groups"] = rx
            
        return ret
