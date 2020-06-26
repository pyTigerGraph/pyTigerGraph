import urllib.request
import os
import subprocess, yaml, re

class gsql():
    def __init__(self, connection, client_version, jarLocation=".gsql/", certNeeded=True, certLocation=".gsql/my-cert.txt"):
        self.connection = connection
        self.jarLocation = jarLocation
        self.certLocation = certLocation
        self.url = connection.gsUrl[8:] # Getting URL with gsql port w/o https://
        '''
        if (noJava):
            Exception("Install Java")
        '''
        if(not os.path.exists(jarLocation)):
            os.mkdir(jarLocation)
    
        if(not os.path.exists(jarLocation+"gsql_client.jar")):
            print("Downloading gsql client Jar")
            jar_url = ('https://bintray.com/api/ui/download/tigergraphecosys/tgjars/' 
                    + 'com/tigergraph/client/gsql_client/' + client_version 
                    + '/gsql_client-' + client_version + '.jar')
                    
            urllib.request.urlretrieve(jar_url, jarLocation + 'gsql_client.jar') # TODO: Store this with the package?
        
        if(certNeeded): #HTTP/HTTPS
            '''
            if (noOpenSSL):
                Exception("No OpenSSL, provide own certificication")
            '''
            if(not os.path.exists(certLocation)):
                print("Creating new SSL Certificate")
                os.system("openssl s_client -connect "+self.url+" < /dev/null 2> /dev/null | openssl x509 -text > "+self.certLocation) # TODO: Python-native SSL?


    def gsql(self, query, options=None):
        cmd = ['java', '-DGSQL_CLIENT_VERSION=v2_6_0', '-jar', 'gsql_client.jar',
        '-cacert', self.certLocation, '-u', self.connection.username, '-p', self.connection.password, 
        '-ip', self.url]
        if (not options):
            comp = subprocess.run(cmd + ["-g", self.connection.graphname] + [query], 
                                stdout=subprocess.PIPE).stdout.decode()
        else:
           comp = subprocess.run(cmd + options + [query], 
                                stdout=subprocess.PIPE).stdout.decode() 
        
        try:
            json_string = re.search('(\{|\[).*$',comp.replace('\n',''))[0]
            json_object = yaml.safe_load(json_string)
        except:
            return comp
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