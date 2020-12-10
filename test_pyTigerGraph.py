import pyTigerGraph as tg

conn = tg.TigerGraphConnection(host="https://medzrouga.i.tgcloud.io", graphname="MyGraph", username="tigergraph",password="tigergraph", apiToken="3mva82884g93sofo5s8tksrnoo7il9v5")

class TestpyTigerGraph:

    def testgsql(self):
        assert conn.gsql("LS")[0] == "---- Global vertices, edges, and all graphs"

    def testgetVer(self):
        assert "3.0.5" == conn.getVer()

    def test_get_statistics(self):
        conn.gsql('''
            CREATE VERTEX Test (PRIMARY_ID id STRING) WITH primary_id_as_attribute="true"
        ''', options=[])
        conn.gsql('''CREATE GRAPH TestGraph(Test)''', options=[])
        conn.graphname = "TestGraph"
        conn.apiToken = conn.getToken(conn.createSecret())
        val = conn.getStatistics()
        # Drops the graph created
        conn.gsql('''
            USE GRAPH TestGraph
            DROP VERTEX Test
            DROP GRAPH TestGraph
        ''')
        # Assert
        assert val == {}

    def test_run_interpreted_query(self):
        conn.gsql('''
            CREATE VERTEX Test (PRIMARY_ID id STRING) WITH primary_id_as_attribute="true"
        ''', options=[])
        conn.gsql('''CREATE GRAPH TestGraph(Test)''', options=[])
        conn.graphname = "TestGraph"
        conn.apiToken = conn.getToken(conn.createSecret())
        val = conn.runInterpretedQuery('''
            INTERPRET QUERY () FOR GRAPH TestGraph {
                Seed = {Test.*};
                PRINT Seed;
            }
        ''')
        # Drops the graph created
        conn.gsql('''
            USE GRAPH TestGraph
            DROP VERTEX Test
            DROP GRAPH TestGraph
        ''')
        # Assert
        assert val == [{'Seed': []}]

    def test_get_installed_queries(self):
        conn.gsql('''
            CREATE VERTEX Test (PRIMARY_ID id STRING) WITH primary_id_as_attribute="true"
        ''', options=[])
        conn.gsql('''CREATE GRAPH TestGraph(Test)''', options=[])
        conn.graphname = "TestGraph"
        conn.apiToken = conn.getToken(conn.createSecret())
        val = conn.getInstalledQueries()
        # Drops the graph created
        conn.gsql('''
            USE GRAPH TestGraph
            DROP VERTEX Test
            DROP GRAPH TestGraph
        ''')
        # Assert
        assert val == {}

    def test_run_installed_query(self):
        conn.gsql('''
            CREATE VERTEX Test (PRIMARY_ID id STRING) WITH primary_id_as_attribute="true"
        ''', options=[])
        conn.gsql('''CREATE GRAPH TestGraph(Test)''', options=[])
        conn.graphname = "TestGraph"
        conn.apiToken = conn.getToken(conn.createSecret())
        conn.gsql('''
            USE GRAPH TestGraph
            CREATE QUERY TestQuery() FOR GRAPH TestGraph {
                Seed = {Test.*};
                PRINT Seed;
            }
            INSTALL QUERY TestQuery
        ''', options=[])
        val = conn.runInstalledQuery("TestQuery")
        # Drops the graph created
        conn.gsql('''
            USE GRAPH TestGraph
            DROP QUERY TestQuery
            DROP VERTEX Test
            DROP GRAPH TestGraph
        ''', options=[])
        # Assert
        assert val == [{'Seed': []}]



