import os
import re
from collections import defaultdict

from neo4j import GraphDatabase
import pandas as pd

vista_dir = "data/Vista"
uri = "bolt://localhost:7687"


class Neo4jVista:
    def __init__(self):
        self._driver = GraphDatabase.driver(uri, auth=("neo4j", "1992"))

    def close(self):
        self._driver.close()

    def add_Req(self, create_node=False, create_link=False):
        sys_req = defaultdict(set)
        sub_req = defaultdict(set)
        req = dict()
        req_sheet = pd.read_excel(os.path.join(vista_dir, "VistA RequirementsHierarchy.xlsx"))
        req_sheet = req_sheet[req_sheet["Regulation ID"].notna() & req_sheet["Requirement"].notna()]

        for index, row in req_sheet.iterrows():
            reg_id = row["Regulation ID"]
            sys_id = row["Sections"]
            sub_id = row["Sub-section"].replace("\'", "\\\'")
            content = row["Requirement"].replace("\'", "\\\'")
            sys_req[sys_id].add(sub_id)
            sub_req[sub_id].add(reg_id)
            req[reg_id] = content

        # Create node
        if create_node:
            for sys_id in sys_req:
                self.create_node("SystemRequirement", {"id": sys_id})
            for sub_id in sub_req:
                self.create_node("SubSystemRequirement", {"id": sub_id})
            for req_id in req:
                self.create_node("Requirement", {"id": req_id, "content": req[req_id]})

        # Create links
        if create_link:
            for sys_id in sys_req:
                for sub_id in sys_req[sys_id]:
                    self.create_link("SystemRequirement", sys_id, "SubSystemRequirement", sub_id, "sys_sub")
            for sub_id in sub_req:
                for req_id in sub_req[sub_id]:
                    self.create_link("SubSystemRequirement", sub_id, "Requirement", req_id, "sub_req")

    @staticmethod
    def __create_node(tx, node_label, attribs):
        attrib_str = ",".join(["{}:'{}'".format(x, attribs[x]) for x in attribs])
        query = "CREATE(a: {} {{{}}})".format(node_label, attrib_str)
        print(query)
        result = tx.run(query)

    @staticmethod
    def __create_link(tx, source_type, source_id, target_type, target_id, relation_label, score):
        score_attrib = "{{score:{}}}".format(score)
        query = "MATCH(a:{}), (b:{}) WHERE a.id = '{}' AND b.id = '{}' " \
                "CREATE(a)-[r:{} {}]->(b)".format(source_type, target_type, source_id, target_id,
                                                  relation_label, score_attrib)
        print(query)
        tx.run(query)

    def create_node(self, node_label, attribs: dict):
        for a in attribs:
            if isinstance(attribs[a], str):
                attribs[a] = str(attribs[a]).replace("\'\""," ")
        with self._driver.session() as session:
            session.write_transaction(self.__create_node, node_label, attribs)

    def create_link(self, source_type, source_id, target_type, target_id, relation_label, score=0):
        with self._driver.session() as session:
            session.write_transaction(self.__create_link, source_type, source_id, target_type, target_id,
                                      relation_label, score)

    def read_vista_xml(self, file_path):
        doc_dict = dict()
        id_pattern = re.compile("<art_id>([^</]*)</art_id>")
        art_pattern = re.compile("<art_title>([^</]*)</art_title>")
        with open(file_path) as fin:
            for line in fin:
                id = id_pattern.search(line)
                art_title = art_pattern.search(line)
                if id and art_title:
                    id = id.group(1).strip("\n\r\t")
                    art_title = art_title.group(1).strip("\n\r\t")
                    doc_dict[id] = art_title
        return doc_dict

    def add_HIPAA(self):
        hippa = "data/Vista/Processed/11HIPAA_Goal_Model.xml"
        doc_dict = self.read_vista_xml(hippa)
        for id in doc_dict:
            content = doc_dict[id]
            self.create_node("HIPPA", {"id": id, "content": content})

    def add_CCHIT(self):
        cchit = "data/Vista/Processed/Processed-CCHIT-NEW-For-Poirot.xml"
        doc_dict = self.read_vista_xml(cchit)
        for id in doc_dict:
            content = doc_dict[id]
            self.create_node("CCHIT", {"id": id, "content": content})

    def add_features(self):
        path = "output/features.csv"
        df = pd.read_csv(path)
        for index, row in df.iterrows():
            id = row["name"]
            feature = row["detail"]
            self.create_node("Feature", {"id": id, "feature": feature})

    def add_commit(self, write_node=True):
        path = "output/commit.csv"
        df = pd.read_csv(path)
        commit_code = defaultdict(set)
        for index, row in df.iterrows():
            # commit_id, commit_summary, commit_files, commit_time
            id = row["commit_id"]
            summary = row["commit_summary"].replace("\'"," ")
            commit_files = row[" commit_files"]
            if str(commit_files) == "nan":
                commit_files = ""
            commit_time = row["commit_time"]
            commit_files_names = [x[0] for x in re.findall("(.+?(.zwr|.csv|.m))",commit_files)]
            commit_code[id].update(commit_files_names)
            if write_node:
                self.create_node("Commit",
                                 {"id": id, "summary": summary, "files": commit_files, "time": commit_time})
        return commit_code

    def add_code(self, code_dir, limit=-1):
        index = {}
        packages = os.listdir(code_dir)
        for pk in packages:
            pk_path = os.path.join(code_dir, pk)
            for dirName, subdirList, fileList in os.walk(pk_path):
                for fname in fileList:
                    index[fname] = pk

        for i, fname in enumerate(index):
            if limit > 0 and i > limit:
                break
            package = index[fname]
            content = "place holder..."
            self.create_node("Code", {"id": fname, "package": package, "content": content})

    def link_req_hippa(self):
        pass

    def link_req_CCHIT(self):
        df = pd.read_csv("data/Vista/Traces/AcceptanceTable.csv")
        for index, row in df.iterrows():
            cchit_id = row["Query TagID"]
            req_id = row["Artifact TagID"]
            accept = row["Accept"]
            if not accept:
                continue
            self.create_link("CCHIT", cchit_id, "Requirement", req_id, "cchit_req")

    def link_commit_req(self):
        c_id = "5f5cd52c8bb1f1c8fb8386f28ee39b596a49a7ca"
        score = 0.8
        req_id = "WV-COS-CSI-015"
        self.create_link("Commit", c_id, "Requirement", req_id, "commit_req", score=score)

    def link_code_commit(self):
        commit_code = self.add_commit(write_node=False)
        for commit_id in commit_code:
            for code_id in commit_code[commit_id]:
                self.create_link("Commit", commit_id, "Code", code_id, "commit_code")

    def link_feature_req(self):
        f_id = "Documentation "
        score = 0.8
        req_id = "WV-COS-CSI-015"
        self.create_link("Feature", f_id, "Requirement", req_id, "feature_req", score=score)

    def run(self):
        # self.add_Req()
        # self.add_HIPAA()
        # self.add_CCHIT()
        # self.add_features()
        # self.add_commit()
        # self.add_code("G:\Download\VistA-M-master\packages", limit=2000)
        # self.link_req_CCHIT()
        # self.link_code_commit()
        # self.link_feature_req()
        self.link_commit_req()


if __name__ == "__main__":
    nv = Neo4jVista()
    nv.run()
