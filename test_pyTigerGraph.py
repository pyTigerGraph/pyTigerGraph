import pyTigerGraph as tg
import pytest

conn = tg.TigerGraphConnection(host="https://medzrouga.i.tgcloud.io", graphname="MyGraph", username="tigergraph",password="tigergraph", apiToken="3mva82884g93sofo5s8tksrnoo7il9v5")

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
        assert "3.0.5" == conn.getVer()

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



