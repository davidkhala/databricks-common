import warnings
from typing import Iterator

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails


class Cluster:
    cluster_id: str
    client: WorkspaceClient
    def __init__(self, client: WorkspaceClient):
        self.client = client

    def clusters(self) -> Iterator[ClusterDetails]:
        return self.client.clusters.list()
    def cluster_ids(self) -> Iterator[str]:
        return (cluster.cluster_id for cluster in self.clusters())

    def get_one(self):
        for cluster_id in self.cluster_ids():
            self.cluster_id = cluster_id
            return self
        return None

    def start(self):
        self.client.clusters.ensure_cluster_is_running(self.cluster_id)

    def stop(self):
        self.client.clusters.delete_and_wait(self.cluster_id)

    def pollute(self):
        warnings.warn(f"workspace.config.cluster_id changes {self.client.config.cluster_id}->{self.cluster_id}")
        self.client.config.cluster_id = self.cluster_id
