from typing import Iterator

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails

from davidkhala.databricks.workspace.types import ClientWare


class Cluster(ClientWare):

    def __init__(self, client: WorkspaceClient):
        super().__init__(client)
        self.cluster_id = client.config.cluster_id

    def clusters(self) -> Iterator[ClusterDetails]:
        return self.client.clusters.list()

    def cluster_ids(self) -> Iterator[str]:
        return (cluster.cluster_id for cluster in self.clusters())

    def get_one(self):
        if self.cluster_id is None:
            self.cluster_id = next(self.cluster_ids())
        return self

    def start(self):
        self.client.clusters.ensure_cluster_is_running(self.cluster_id)

    def stop(self):
        self.client.clusters.delete_and_wait(self.cluster_id)
