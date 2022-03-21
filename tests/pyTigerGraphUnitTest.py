import unittest
from os.path import exists

import pyTigerGraph as pyTG


class pyTigerGraphUnitTest(unittest.TestCase):
    conn = None

    def setUp(self):
        params = {
            "host": "http://127.0.0.1",
            "graphname": "tests",
            "username": "tigergraph",
            "password": "tigergraph",
            "restppPort": "9000",
            "gsPort": "14240",
            "gsqlVersion": "",
            "userCert": True,
            "certPath": None,
            "sslPort": "443",
            "gcp": False
        }

        fname = "testserver.cfg"
        if exists(fname):
            try:
                cfg = open(fname, "r")

                lines = cfg.readlines()

                for l in lines:
                    ll = l.rstrip("\n").split("=")
                    params[ll[0]] = ll[1]

            except OSError as e:
                print(e.strerror)

        self.conn = pyTG.TigerGraphConnection(host=params["host"], graphname=params["graphname"],
            username=params["username"], password=params["password"],
            restppPort=params["restppPort"], gsPort=params["gsPort"],
            gsqlVersion=params["gsqlVersion"], useCert=params["userCert"],
            certPath=params["certPath"], sslPort=params["sslPort"], gcp=params["gcp"])
