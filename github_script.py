import math
import os

# pip install PyGithub. Lib operates on remote github to get issues
from github import Github
import re
import argparse
# pip install GitPython. Lib operates on local repo to get commits
import git as local_git


class MyCommit:
    def __init__(self, commit_id, summary, diffs, commit_time):
        self.commit_id = commit_id
        self.summary = summary
        self.diffs = diffs
        self.commit_time = commit_time

    def __str__(self):
        summary = re.sub("[,\r\n]+", " ", self.summary)
        diffs = " ".join(self.diffs)
        diffs = re.sub("[,\r\n]+", " ", diffs)
        return "{},{},{},{}\n".format(self.commit_id, summary, diffs, self.commit_time)


class RepoCollector:
    def __init__(self, user_name, passwd, download_path, repo_path, do_translation):
        self.user_name = user_name
        self.passwd = passwd
        self.download_path = download_path
        self.repo_path = repo_path
        self.do_translate = do_translation

    def run(self):
        repo_url = "git@github.com:{}.git".format(self.repo_path)
        repo_name = repo_url.split("/")[1]
        clone_path = os.path.join(self.download_path, repo_name)
        if not os.path.exists(clone_path):
            local_git.Repo.clone_from(repo_url, clone_path, branch='master')
        local_repo = local_git.Repo(clone_path)

        commit_file_path = os.path.join(output_dir, "commit.csv")
        if not os.path.isfile(commit_file_path):
            print("creating commit.csv...")
            with open(commit_file_path, 'w', encoding="utf8") as fout:
                fout.write("commit_id,commit_summary, commit_files,commit_time\n")
                for i, commit in enumerate(local_repo.iter_commits()):
                    print("commit #{}".format(i))
                    id = commit.hexsha
                    summary = commit.summary
                    create_time = commit.committed_datetime

                    commit = MyCommit(id, summary, differs, create_time)
                    fout.write(str(commit))


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Github script")
    parser.add_argument("-u", help="user name")
    parser.add_argument("-p", help="password")
    parser.add_argument("-d", help="download path")
    parser.add_argument("-r", nargs="+", help="repo path in github, a list of repo path can be passed")
    parser.add_argument("-o", help="output directory", default="output")
    args = parser.parse_args()
    for repo_path in args.r:
        print("Processing repo: {}".format(repo_path))
        rpc = RepoCollector(args.u, args.p, args.d, repo_path, args.t)
        rpc.run()
