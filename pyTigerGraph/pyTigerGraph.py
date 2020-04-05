import requests
import json

class TigerGraphConnection:
    def __init__(self, ipAddress="http://localhost", graphname="MyGraph", username="tigergraph", password="tigergraph", apiPort = "9000", interpreterPort = "14240", apiToken="", serverAccessPort = "8123"):
        self.url = ipAddress
        self.username = username
        self.password = password
        self.graphname = graphname
        self.apiPort = apiPort
        self.interpreterPort = interpreterPort
        self.apiToken = "Bearer "+apiToken
        self.serverAccessPort = serverAccessPort
    
    def runInstalledQuery(self, queryName, params, timeout=None, sizeLimit=None):
        if timeout == None:
            timeout = 16000
        if sizeLimit == None:
            sizeLimit = 32000000
        queryUrl = self.url+":"+self.apiPort+"/query/"+self.graphname+"/"+queryName
        response = requests.request("GET", queryUrl,  params=params, headers={'Authorization':self.apiToken, "RESPONSE-LIMIT":str(sizeLimit), "GSQL-TIMEOUT":str(timeout)})
        return json.loads(response.text)

    def getVertices(self, vertexType, limit=None):
        if limit != None:
            queryUrl = self.url+":"+self.apiPort+"/graph/"+self.graphname+"/vertices/"+vertexType+"?limit="+limit
        else:
            queryUrl = self.url+":"+self.apiPort+"/graph/"+self.graphname+"/vertices/"+vertexType
        response = requests.request("GET", queryUrl, headers={'Authorization':self.apiToken})
        return json.loads(response.text)

    def runInterpretedQuery(self, query):
        queryUrl = self.url+":"+self.interpreterPort+"/gsqlserver/interpreted_query"
        print(queryUrl)
        response = requests.request("POST", queryUrl, data=query, auth=(self.username, self.password), headers={'Authorization':self.apiToken})
        return json.loads(response.text)

    def runEcho(self):
        queryUrl = self.url+":"+self.apiPort+"/echo"
        response = requests.request("GET", queryUrl, headers={'Authorization':self.apiToken})
        return json.loads(response.text)

    def getEndpoints(self):
        queryUrl = self.url+":"+self.apiPort+"/endpoints"
        response = requests.request("GET", queryUrl, headers={'Authorization':self.apiToken})
        return json.loads(response.text)

    def getToken(self, secret, lifetime):
        queryUrl = self.url+":"+self.apiPort+"/requesttoken?secret="+secret+"&lifetime="+lifetime      
        response = requests.request("GET", queryUrl, auth=(self.username, self.password))
        return json.loads(response.text)
    
    def deleteToken(self, secret):
        queryUrl = self.url+":"+self.apiPort+"/requesttoken?secret="+secret      
        response = requests.request("DELETE", queryUrl, auth=(self.username, self.password))
        return json.loads(response.text)

    def refreshToken(self, secret, token, lifetime):
        queryUrl = self.url+":"+self.apiPort+"/requesttoken?secret="+secret+"&token="+token+"&lifetime="+lifetime
        response = requests.request("PUT", queryUrl, auth=(self.username, self.password))
        return json.loads(response.text)
'''
    def getSchema(self):
         queryUrl = self.url+":"+self.serverAccessPort+"/gsql/schema?graph="+self.graphname
         response = requests.request("GET", queryUrl, auth=(self.username, self.password))
         return json.loads(response.text)   
'''
