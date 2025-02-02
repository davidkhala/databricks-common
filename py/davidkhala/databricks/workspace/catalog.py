from databricks.sdk.errors import platform

from davidkhala.databricks.workspace import Workspace


class Catalog:
    def __init__(self, w: Workspace):
        self.w: Workspace = w

    @property
    def catalogs(self):
        return self.w.client.catalogs

    def create(self, name, *, withMetastoreLevelStorage=False, storage_root=None):

        if self.get(name):
            return

        if withMetastoreLevelStorage:
            return self.catalogs.create(name)
        else:
            if storage_root is None:
                storage_root = self.get().storage_root
            return self.catalogs.create(name, storage_root=storage_root)

    def get(self, name=None):
        if not name:
            name = self.w.catalog
        try:
            return self.catalogs.get(name)
        except platform.NotFound as e:
            if str(e) == f"Catalog '{name}' does not exist.":
                return None
            else:
                raise e

    def delete(self, name):
        return self.catalogs.delete(name, force=True)


class Schema:
    default = 'default'

    def __init__(self, w: Workspace, name=default, catalog: str = None):
        self.w: Workspace = w
        if not catalog:
            catalog = self.w.catalog
        self.catalog = catalog
        self.name = name

    @property
    def schemas(self):
        return self.w.client.schemas

    def get(self):
        try:
            return self.schemas.get(f"{self.catalog}.{self.name}")
        except platform.NotFound as e:
            if str(e) == f"Schema '{self.catalog}.{self.name}' does not exist.":
                return None

    def create(self):
        try:
            return self.schemas.create(self.name, self.catalog)
        except platform.BadRequest as e:
            if str(e) == f"Schema '{self.name}' already exists":
                return
            raise e

    def delete(self):
        try:
            return self.schemas.delete(f"{self.catalog}.{self.name}", force=True)
        except platform.NotFound as e:
            if str(e) == f"Schema '{self.catalog}.{self.name}' does not exist.":
                return None
            raise e
