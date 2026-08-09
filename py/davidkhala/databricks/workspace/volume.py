import io
import os
from os import PathLike
from typing import BinaryIO

from databricks.sdk import WorkspaceClient, errors
from databricks.sdk.errors import NotFound
from databricks.sdk.mixins.files import DownloadFileResult
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.service.files import DownloadResponse

from davidkhala.databricks.workspace import Workspace
from davidkhala.databricks.workspace.catalog import Schema
from davidkhala.databricks.workspace.types import ClientWare


class Volume(ClientWare):
    def __init__(self, w: Workspace, volume=None, schema=None, catalog=None):
        super().__init__(w.client)
        self.catalog = catalog or w.catalog
        self.schema = schema or Schema.default
        self.volume = volume

    def list(self):
        return self.client.volumes.list(self.catalog, self.schema)

    @property
    def path(self):
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def full_name(self):
        return f"{self.catalog}.{self.schema}.{self.volume}"

    def get(self):
        try:
            return self.client.volumes.read(self.full_name)
        except errors.platform.ResourceDoesNotExist as e:
            if str(e) == f"Volume '{self.full_name}' does not exist.":
                return None
            raise e

    def create(self, volume_type=VolumeType.MANAGED):
        try:
            return self.client.volumes.create(self.catalog, self.schema, self.volume, volume_type)
        except errors.platform.ResourceAlreadyExists as e:
            if str(e) == f"Volume '{self.volume}' already exists":
                return
            else:
                raise e

    def delete(self):
        try:
            self.client.volumes.delete(self.full_name)
        except errors.platform.ResourceDoesNotExist as e:
            if str(e) == f"Volume '{self.full_name}' does not exist.":
                return
            else:
                raise e

    @property
    def fs(self):
        return VolumeFS(self.client, self.path)


class VolumeFS(ClientWare):
    """
    https://docs.databricks.com/en/dev-tools/sdk-python.html#manage-files-in-unity-catalog-volumes
    """

    def __init__(self, client: WorkspaceClient, volume_root_path):
        super().__init__(client)
        self.path = volume_root_path

    def upload(self, local_path, target_path=None, *, overwrite=False):
        if target_path is None:
            target_path = f"{self.path}/{os.path.basename(local_path)}"

        with open(local_path, 'rb') as file:
            file_bytes = file.read()
            binary_data = io.BytesIO(file_bytes)
            self.client.files.upload(target_path, binary_data, overwrite=overwrite)
        return target_path

    def ls(self):
        return self.client.files.list_directory_contents(self.path)

    def exists(self, *, relative_path=None, absolute_path=None) -> bool:
        if not absolute_path:
            absolute_path = f"{self.path}/{relative_path}"
        try:
            self.client.files.get_metadata(absolute_path)
            return True
        except NotFound:
            return False


    def read(self, *, relative_path=None, absolute_path = None) -> BinaryIO | None:
        if not absolute_path:
            absolute_path = f"{self.path}/{relative_path}"
        resp: DownloadResponse = self.client.files.download(absolute_path)
        return resp.contents

    def download(self, relative_path: str, sink: PathLike):
        resp: DownloadFileResult = self.client.files.download_to(f"{self.path}/{relative_path}", str(sink))
        return resp

    def rm(self, relative_volume_path, recursive=True):
        target_path = f"{self.path}/{relative_volume_path}"
        if recursive:
            self.client.files.delete_directory(target_path)
        else:
            self.client.files.delete(target_path)
