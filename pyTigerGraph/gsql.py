import urllib.request
import os
import subprocess, yaml, re
import pyTigerGraph

class Gsql():
    def __init__(self, connection, client_version="3.0.0", jarLocation="~/.gsql", certNeeded=True, certLocation="~/.gsql/my-cert.txt", jarDownload=True, certDownload=True):
        assert isinstance(connection, pyTigerGraph.TigerGraphConnection), "Must pass in a TigerGraphConnection"
        self.connection = connection
        self.jarLocation = os.path.expanduser(jarLocation)
        self.certLocation = os.path.expanduser(certLocation)
        self.certNeeded = certNeeded
        self.client_version = client_version
        self.url = connection.gsUrl.replace("https://", "").replace("http://", "") # Getting URL with gsql port w/o https://
        self.stdout=''
        self.stderr=''
        
        '''
        if (noJava):
            Exception("Install Java")
        '''

        if(not os.path.exists(self.jarLocation)):
            os.mkdir(self.jarLocation)
    
        if jarDownload:
            print("Downloading gsql client Jar")
            jar_url = ('https://bintray.com/api/ui/download/tigergraphecosys/tgjars/' 
                    + 'com/tigergraph/client/gsql_client/' + client_version 
                    + '/gsql_client-' + client_version + '.jar')
                    
            urllib.request.urlretrieve(jar_url, self.jarLocation + '/gsql_client.jar') # TODO: Store this with the package?
        
        if(certNeeded and certDownload): #HTTP/HTTPS
            '''
            if (noOpenSSL):
                Exception("No OpenSSL, provide own certificication")
            '''
            print("Downloading SSL Certificate")
            os.system("openssl s_client -connect "+self.url+" < /dev/null 2> /dev/null | openssl x509 -text > "+self.certLocation) # TODO: Python-native SSL?
            if os.stat(self.certLocation).st_size == 0:
                print('Certificate download failed. Please check that the server is online.')


    def gsql(self, query, options=None):
        
        if (options == None):
            options = ["-g", self.connection.graphname]
        
        cmd = ['java', '-DGSQL_CLIENT_VERSION=v' + self.client_version.replace('.','_'),
               '-jar', self.jarLocation + '/gsql_client.jar' ]

        if self.certNeeded:
            cmd += ['-cacert', self.certLocation]

        cmd += [
        '-u', self.connection.username, '-p', self.connection.password, 
        '-ip', self.url]
        
        comp = subprocess.run(cmd + options + [query], 
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

        self.stdout = comp.stdout.decode()
        self.stderr = comp.stderr.decode()
        
        try:
            json_string = re.search('(\{|\[).*$',
                                    self.stdout.replace('\n',''))[0]
            json_object = yaml.safe_load(json_string)
        except:
            return self.stdout
        else:
            return json_object
        
    def createSecret(self, alias=""):
        response = self.gsql("CREATE SECRET"+" "+alias)
        try:
            secret = re.search('The secret\: (\w*)',response.replace('\n',''))[1]
            return secret
        except:
            return None

    # TODO: showSecret()
