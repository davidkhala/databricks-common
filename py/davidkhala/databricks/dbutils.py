import re
from typing import Iterable

from databricks.sdk.dbutils import RemoteDbUtils, FileInfo


class FS:
    def __init__(self, dbutils: RemoteDbUtils):
        self._ = dbutils

    def __getattr__(self, name):
        return getattr(self._, name)

    def tree(self, path, *, indent=""):

        files = self._.fs.ls(path)

        for f in files:
            name = f.name

            print(indent + "├── " + name.rstrip("/"))

            if self.isDir(f):
                self.tree(f.path, indent=indent + "│   ")

    @staticmethod
    def isDir(f: FileInfo):
        return f.size == 0 and f.modificationTime == 0

    def cat(self, path:str) -> str:
        content = self._.fs.head(path)
        print(content)
        return content
    
    ignore = [".DS_Store", ".md", ".sh", ".git", ".github", ".gitignore", "LICENSE", '.zip', '.pdf']

    def ls(self, path, *, ignore_patterns: list[str] = None) -> Iterable[FileInfo]:
        if not ignore_patterns:
            ignore_patterns = self.ignore
        combined = r"({})".format("|".join(ignore_patterns))
        for file_info in self._.fs.ls(path):
            if not re.search(combined, file_info.name):
                yield file_info
