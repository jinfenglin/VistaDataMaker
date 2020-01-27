import os

from neo4j import GraphDatabase
import pandas as pd

vista_dir = "data/Vista"
uri = "bolt://localhost:7687"


class Neo4jVista:
    def __init__(self):
        self._driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

    def close(self):
        self._driver.close()

    def add_Req(self):
        sys_req = dict()
        sub_req = dict()
        req = dict()
        req_sheet = pd.read_excel(os.path.join(vista_dir, "VistA RequirementsHierarchy.xlsx"))
        ids = req_sheet["Regulation ID"]

        print(ids)

    def add_HIPAA(self):
        pass

    def add_CCHIT(self):
        pass

    def link_Reqs(self):
        pass

    def link_req_hippa(self):
        pass

    def link_req_CCHIT(self):
        pass

    def link_code_commit(self):
        pass

    def add_features(self):
        pass

    def run(self):
        self.add_Req()
        self.add_CCHIT()
        self.add_features()
        self.add_HIPAA()


if __name__ == "__main__":
    nv = Neo4jVista()
    nv.run()
