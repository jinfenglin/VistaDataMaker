import os
from collections import defaultdict

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
        sys_req = defaultdict(list)
        sub_req = defaultdict(list)
        req = dict()
        req_sheet = pd.read_excel(os.path.join(vista_dir, "VistA RequirementsHierarchy.xlsx"))
        req_sheet = req_sheet[req_sheet["Regulation ID"].notna() & req_sheet["Requirement"].notna()]

        for index, row in req_sheet.iterrows():
            reg_id = row["Regulation ID"]
            sys_id = row["Sections"]
            sub_id = row["Sub-section"].replace("\'","\\\'")
            print(row["Requirement"])
            content = row["Requirement"].replace("\'","\\\'")
            sys_req[sys_id].append(sub_id)
            sub_req[sub_id].append(reg_id)
            req[reg_id] = content

        # Create node
        for sys_id in sys_req:
            self.create_node("SystemRequirement", {"id": sys_id})
        for sub_id in sub_req:
            self.create_node("SubSystemRequirement", {"id": sub_id})
        for req_id in req:
            self.create_node("Requirement", {"id": req_id, "content": req[req_id]})

        # Create links
        for sys_id in sys_req:
            for sub_id in sys_req[sys_id]:
                self.create_link("SystemRequirement", sys_id, "SubSystemRequirement", sub_id)
        for sub_id in sub_req:
            for req_id in req:
                self.create_link("SubSystemRequirement", sub_id, "Requirement", req_id)

    @staticmethod
    def __create_node(tx, node_label, attribs):
        attrib_str = ",".join(["{}:'{}'".format(x, attribs[x]) for x in attribs])
        query = "CREATE(a: {} {{{}}})".format(node_label, attrib_str)
        print(query)
        result = tx.run(query)

    @staticmethod
    def __create_link(tx, source_type, source_id, target_type, target_id):
        query = "MATCH(a:{}), (b:{}) WHERE a.id = '{}' AND b.id = '{}' " \
                "CREATE(a) -[r]->(b)".format(source_type, target_type, source_id, target_id)
        tx.run(query)

    def create_node(self, node_label, attribs: dict):
        with self._driver.session() as session:
            session.write_transaction(self.__create_node, node_label, attribs)

    def create_link(self, source_type, target_type, source_id, target_id):
        with self._driver.session() as session:
            session.write_transaction(self.__create_link, source_type, source_id, target_type, target_id)

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
