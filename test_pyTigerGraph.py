import pyTigerGraphBeta as tg
import pytest
import os

conn = tg.TigerGraphConnection(host=os.environ['HOST_TG'], graphname="MyGraph", username="tigergraph",password=os.environ['HOST_PASS'],useCert=False,version="3.1.0")


class TestpyTigerGraph:

    @pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
    def testgsql(self):
        assert conn.gsql("LS")[0] == "---- Global vertices, edges, and all graphs"

    @pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
    def test_get_statistics(self):
        conn.gsql("drop all")
        conn.gsql('''
            CREATE VERTEX Test (PRIMARY_ID id STRING) WITH primary_id_as_attribute="true"
        ''', options=[])
        conn.gsql('''CREATE GRAPH TestGraph(Test)''', options=[])
        conn.graphname = "TestGraph"
        conn.apiToken = conn.getToken(conn.createSecret())[0]
        val = conn.getStatistics()

        # Assert
        assert val == {}

    def testgetVer(self):
        assert "3.1.0" == conn.getVer()

    @pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
    def test_run_interpreted_query(self):

        conn.graphname = "TestGraph"
        conn.apiToken = conn.getToken(conn.createSecret())[0]
        val = conn.runInterpretedQuery('''
            INTERPRET QUERY () FOR GRAPH TestGraph {
                Seed = {Test.*};
                PRINT Seed;
            }
        ''')

        # Assert
        assert val == [{'Seed': []}]

    @pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
    def test_get_installed_queries(self):

        conn.graphname = "TestGraph"
        conn.apiToken = conn.getToken(conn.createSecret())[0]
        val = conn.getInstalledQueries()

        # Assert
        assert val == {}


    @pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
    def testcreateSecret(self):
        conn.gsql("USE GRAPH MyGraph")
        secret = conn.createSecret()
        assert secret != None

    @pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
    def testgetVersion(self):
        conn.gsql("USE GRAPH MyGraph")
        val = conn.getVersion()
        assert val[0]["name"] == 'product'

    @pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
    def testgetToken(self):
        token = None
        try:
            conn.gsql("USE GRAPH TestGraph")
            token = conn.getToken(conn.createSecret())
        except:
            pass

        assert token != None

    @pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
    def testrefreshToken(self):
        newToken = None
        try:
            conn.gsql("USE GRAPH TestGraph")
            secret = conn.createSecret()
            token = conn.getToken(secret)
            newToken = conn.refreshToken(secret)
        except:
            pass

        assert newToken != None

    @pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
    def testdeleteToken(self):
        didDelete = False
        try:
            conn.gsql("USE GRAPH TestGraph")
            didDelete = conn.deleteToken(conn.createSecret())
        except:
            pass

        assert didDelete != False

    @pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
    def testgetEndpoints(self):
        conn.gsql("USE GRAPH TestGraph")
        conn.getToken(conn.createSecret())
        endpoints = conn.getEndpoints()
        assert endpoints != {}


    @pytest.mark.filterwarnings('ignore::urllib3.exceptions.InsecureRequestWarning')
    def test_run_installed_query(self):

        conn.graphname = "TestGraph"
        conn.gsql("USE GRAPH TestGraph")
        conn.apiToken = conn.getToken(conn.createSecret())[0]
        conn.gsql('USE GRAPH TestGraph')
        conn.gsql('''
            CREATE QUERY TestQuery() FOR GRAPH TestGraph {
                Seed = {Test.*};
                PRINT Seed;
            }
        ''', options=[])
        conn.gsql("INSTALL QUERY TestQuery")
        val = conn.runInstalledQuery("TestQuery")
        conn.gsql("drop all")
        # Assert
        assert val == [{'Seed': []}]
